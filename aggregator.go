package aggregator

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"

	"github.com/spf13/afero"
)

type workerResult struct {
	*cache
	confirm chan struct{}
	err     error
}

type AggContainer struct {
	PathToFiles string	//Required path to source files.
	FileExt     string	//Optional extension of source files. By default will read all files.
	HeaderRow   string	//Optional header row in file. If sets it will be skipped by worker and will be written in result file
	ResultFile  string	//Path to file where results of aggregation will be placed. By defaults will be created in source dir.

	Mapper   func(row string) (string, []interface{}, error)	//Convert row from a file to slice.
	UnMapper func(key string, data []interface{}) string		//Inverse transformation from slice to row
	Reducer  func(a, b []interface{}) []interface{}				//Reduce function. Convert two slice to one

	NumWorkers       int
	MainBufferSize   int
	WorkerBufferSize int

	Fs afero.Fs		//Filesystem switcher. afero.NewOsFs() - for work OR afero.NewMemMapFs() - virtual memory for testing
}

//Convert row from a file to slice.
func (container *AggContainer) SetMapper(m func(row string) (string, []interface{}, error)) {
	container.Mapper = m
}

//Inverse transformation from slice to row
func (container *AggContainer) SetUnMapper(um func(key string, data []interface{}) string) {
	container.UnMapper = um
}

//Reduce function. Convert two slice to one
func (container *AggContainer) SetReducer(r func(a, b []interface{}) []interface{}) {
	container.Reducer = r
}

//Checking for required parameters. Set defaults.
func (container *AggContainer) Init() error {
	if container.Fs == nil {
		container.Fs = afero.NewOsFs()
	}

	if container.PathToFiles == "" {
		return errors.New("PathToFiles is required")
	}
	container.PathToFiles = strings.TrimRight(container.PathToFiles, "/")

	if container.ResultFile == "" {
		container.ResultFile = container.PathToFiles + "/_aggregate.result"
	}

	if container.MainBufferSize == 0 {
		container.MainBufferSize = 6000000
	}

	if container.WorkerBufferSize == 0 {
		container.WorkerBufferSize = 500000
	}

	if container.NumWorkers == 0 {
		container.NumWorkers = 10
	}

	err := createTempDir(container.PathToFiles, container.Fs)
	if err != nil {
		return errors.New("Cannot create temp dir: " + err.Error())
	}

	err = makeClean(container.PathToFiles+"/temp", container.ResultFile, container.Fs)
	if err != nil {
		return errors.New("Cannot make clean start: " + err.Error())
	}

	return nil
}

//Aggregation launch. Starting (NumWorkers) workers which read files from source folder and aggregate into their cache.
//When cache riches WorkerBufferSize, the data is dumped to the manager. The manager aggregates it and write to files.
func (container *AggContainer) Start() error {
	m := newCache(true)
	done := make(chan struct{})
	defer close(done)
	paths, errc := walkFiles(done, container.PathToFiles, container.FileExt, container.Fs)

	c := make(chan workerResult)
	var wg sync.WaitGroup

	wg.Add(container.NumWorkers)
	for i := 0; i < container.NumWorkers; i++ {
		go func() {
			container.workerUnit(done, paths, c)
			wg.Done()
		}()
	}
	go func() {
		wg.Wait()
		close(c)
	}()

	for r := range c {
		if r.err != nil {
			return r.err
		}

		r.data.Range(func(key interface{}, value interface{}) bool {
			if v, ok := m.load(fmt.Sprintf("%v", key)); ok {
				m.save(fmt.Sprintf("%v", key), container.Reducer(v, value.([]interface{})))
			} else {
				m.save(fmt.Sprintf("%v", key), value.([]interface{}))
			}
			return true
		})

		r.confirm <- struct{}{}

		if mapInBytes(m.dataLen()) > container.MainBufferSize {
			m.dump(container)
			m.clear()
		}
	}

	m.dump(container)

	if err := <-errc; err != nil {
		return err
	}

	err := container.makeResult()
	if err != nil {
		return err
	}
	return nil
}

func walkFiles(done <-chan struct{}, root string, ext string, fs afero.Fs) (<-chan string, <-chan error) {
	paths := make(chan string)
	errc := make(chan error, 1)
	go func() {
		defer close(paths)
		errc <- afero.Walk(fs, root, func(path string, info os.FileInfo, err error) error {
			if err != nil {
				return err
			}
			if !info.Mode().IsRegular() {
				return nil
			}

			if ext != "" && filepath.Ext(path) != "."+ext {
				return nil
			}

			select {
			case paths <- path:
			case <-done:
				return errors.New("walk canceled")
			}
			return nil
		})
	}()
	return paths, errc
}

func (container *AggContainer) workerUnit(done <-chan struct{}, paths <-chan string, c chan<- workerResult) {
	wc := newCache(false)
	confirm := make(chan struct{})

	for path := range paths {
		func() {
			f, err := container.Fs.Open(path)
			if err != nil {
				c <- workerResult{wc, confirm, err}
				return
			}
			defer f.Close()

			scanner := bufio.NewScanner(f)
			scanner.Split(bufio.ScanLines)

			rowCounter := 0

			if container.HeaderRow != "" {
				scanner.Scan()
				rowCounter++
			}

			for scanner.Scan() {
				key, entry, err := container.Mapper(scanner.Text())
				if err != nil {
					c <- workerResult{wc, confirm, errors.New("Cannot parse csv file " + path + " at row # " + strconv.Itoa(rowCounter) + " : invalid syntax")}
					return
				}

				if v, ok := wc.load(key); ok {
					wc.save(key, container.Reducer(v, entry))
				} else {
					wc.save(key, entry)
				}
				rowCounter++

				if rowCounter%100 == 0 && mapInBytes(wc.dataLen()) > container.WorkerBufferSize {
					select {
					case c <- workerResult{wc, confirm, err}:
						<-confirm
						wc.clear()
					case <-done:
						return
					}
				}
			}
		}()
	}
	select {
	case c <- workerResult{wc, confirm, nil}:
		<-confirm
		wc.clear()
	case <-done:
		return
	}
}

func (container *AggContainer) addEntryToFile(key string, data []interface{}) error {
	fname := container.PathToFiles + "/temp/" + key + ".tmp"
	_, eerr := container.Fs.Stat(fname)

	f, err := container.Fs.OpenFile(fname, os.O_RDWR|os.O_CREATE, 0755)
	if err != nil {
		return err
	}
	defer f.Close()

	var entry []interface{}
	if eerr == nil || !os.IsNotExist(eerr) {
		scanner := bufio.NewScanner(f)
		scanner.Split(bufio.ScanLines)

		scanner.Scan()
		_, entry, _ = container.Mapper(scanner.Text())
		entry = container.Reducer(entry, data)
	} else {
		entry = data
	}

	f.Truncate(0)
	f.Seek(0, 0)

	w := bufio.NewWriter(f)
	_, err = w.WriteString(container.UnMapper(key, entry) + "\n")

	w.Flush()

	if err != nil {
		return err
	}

	return nil
}

func (container *AggContainer) makeResult() error {
	out, err := container.Fs.OpenFile(container.ResultFile, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	defer out.Close()

	out.Truncate(0)
	out.Seek(0, 0)

	if container.HeaderRow != "" {
		w := bufio.NewWriter(out)
		_, err = w.WriteString(container.HeaderRow + "\n")
		w.Flush()
	}

	afero.Walk(container.Fs, container.PathToFiles+"/temp", func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.Mode().IsRegular() {
			return nil
		}

		if filepath.Ext(path) != ".tmp" {
			return nil
		}

		in, err := container.Fs.Open(path)
		if err != nil {
			return err
		}
		defer in.Close()

		_, err = io.Copy(out, in)
		if err != nil {
			return err
		}
		in.Close()

		if err := container.Fs.Remove(path); err != nil {
			return err
		}

		return nil
	})
	return nil
}
