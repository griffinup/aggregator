package main

import (
	"bufio"
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	//"runtime"
	"strconv"
	"strings"
	"sync"
	"unsafe"
	"github.com/spf13/afero"
)

const numWorkers = 20
//Размер буффера воркера. Воркер читает файлы и аггрегирует данные в буфер, при достижении размера скидывает наверх менеджеру.
const workerBuffer int = 500000
//Размер буффера менеджера. Аггрегируются данные от воркеров и при достижении размера свопит данные на диск.
const mainBuffer int = 6000000

var AppFs = afero.NewOsFs()

//func PrintMemUsage() {
//	var m runtime.MemStats
//	runtime.ReadMemStats(&m)
//	fmt.Printf("Alloc = %v MiB", bToMb(m.Alloc))
//	fmt.Printf("\tTotalAlloc = %v MiB", bToMb(m.TotalAlloc))
//	fmt.Printf("\tSys = %v MiB", bToMb(m.Sys))
//	fmt.Printf("\tNumGC = %v\n", m.NumGC)
//}

//func bToMb(b uint64) uint64 {
//	return b / 1024 / 1024
//}

//Формат строки в csv
type Vector struct {
	A, B, C float64
}

func (a Vector) Add(b Vector) Vector {
	a.A += b.A
	a.B += b.B
	a.C += b.C
	return a
}

//Добавление данных по дате в файл
func addVectorToFile(root string, name string, data Vector, fs afero.Fs) error {
	fname := strings.TrimRight(root, "/") + "/temp/" + name + ".tmp"
	_, eerr := fs.Stat(fname);

	f, err := fs.OpenFile(fname, os.O_RDWR|os.O_CREATE, 0755)
	if err != nil {
		return err
	}
	defer f.Close()

	v := Vector{}

	if eerr == nil || !os.IsNotExist(eerr) {
		csvr := csv.NewReader(bufio.NewReader(f))
		csvr.Comma = ';'

		row, err := csvr.Read()
		if err != nil && err != io.EOF{
			return err
		}

		if v.A, err = strconv.ParseFloat(strings.TrimSpace(row[1]), 64); err != nil {
			return err
		}
		if v.B, err = strconv.ParseFloat(strings.TrimSpace(row[2]), 64); err != nil {
			return err
		}
		if v.C, err = strconv.ParseFloat(strings.TrimSpace(row[3]), 64); err != nil {
			return err
		}
	}

	v = v.Add(data)

	f.Truncate(0)
	f.Seek(0,0)

	w := csv.NewWriter(bufio.NewWriter(f))
	w.Comma = ';'
	w.Write([]string{name, fmt.Sprintf("%v", v.A), fmt.Sprintf("%v", v.B), fmt.Sprintf("%v", v.C)})
	w.Flush()

	if err := w.Error(); err != nil {
		return err
	}

	return nil
}

//Структура ответа воркера
type workerResult struct {
	*Cache
	confirm chan struct{}
	err  error
}

//Структура кеша
type Cache struct {
	isGlobal bool
	data map[string]Vector
	mx sync.Mutex
}

func NewCache(isglobal bool) *Cache {
	return &Cache{
		isGlobal: isglobal,
		data: make(map[string]Vector),
	}
}

func (c *Cache) Add(key string, value Vector) {
	if c.isGlobal {
		c.mx.Lock()
		defer c.mx.Unlock()
	}

	if _, ok := c.data[key]; ok {
		c.data[key] = c.data[key].Add(value)
	} else {
		c.data[key] = value
	}
}

func (c *Cache) Store(root string) {
	if !c.isGlobal {
		return
	}

	for key, value := range c.data {
		err := addVectorToFile(root, key, value, AppFs)
		if err != nil {
			fmt.Println(err)
		}
	}
}

func (c *Cache) Clear() {
	c.mx.Lock()
	defer c.mx.Unlock()
	c.data = map[string]Vector{}
}

func mapInBytes(maplen int) int {
	var memstring string
	var memvector Vector

	return maplen * 8 + (maplen * 8 * (int)(unsafe.Sizeof(memstring))) + (maplen * 8 * (int)(unsafe.Sizeof(memvector)))
}

func RemoveContents(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	names, err := d.Readdirnames(-1)
	if err != nil {
		return err
	}
	for _, name := range names {
		err = os.RemoveAll(filepath.Join(dir, name))
		if err != nil {
			return err
		}
	}
	return nil
}
func walkFiles(done <-chan struct{}, root string) (<-chan string, <-chan error) {
	paths := make(chan string)
	errc := make(chan error, 1)
	go func() {
		defer close(paths)
		errc <- filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
			if err != nil {
				return err
			}
			if !info.Mode().IsRegular() {
				return nil
			}

			if filepath.Ext(path) != ".cvs" {
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

func makeResult(root string) error {
	out, err := os.OpenFile(root + "/__aggregate.result", os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	defer out.Close()

	out.Truncate(0)
	out.Seek(0,0)

	filepath.Walk(root + "/temp", func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.Mode().IsRegular() {
			return nil
		}

		if filepath.Ext(path) != ".tmp" {
			return nil
		}

		in, err := os.Open(path)
		if err != nil {
			return err
		}
		defer in.Close()

		_, err = io.Copy(out, in)
		if err != nil {
			return err
		}
		in.Close()

		if err := os.Remove(path); err != nil {
			return err
		}

		return nil
	})
	return nil
}

func AggregateUnit(done <-chan struct{}, paths <-chan string, c chan<- workerResult) {
	wc := NewCache(false);
	confirm := make(chan struct{})

	for path := range paths {
		f, err := os.Open(path)
		if err != nil {
			c <- workerResult{wc, confirm,err}
			return
		}
		defer f.Close()

		csvr := csv.NewReader(bufio.NewReader(f))
		csvr.Comma = ';'

		rowcounter := 0

		for {

			row, err := csvr.Read()
			if err != nil {
				if err == io.EOF {
					err = nil
				}
				break
			}

			v := Vector{}
			var erra, errb, errc error
			v.A, erra = strconv.ParseFloat(strings.TrimSpace(row[1]), 64)
			v.B, errb = strconv.ParseFloat(strings.TrimSpace(row[2]), 64)
			v.C, errc = strconv.ParseFloat(strings.TrimSpace(row[3]), 64)

			if erra != nil || errb != nil || errc != nil {
				c <- workerResult{wc, confirm, errors.New("Cannot parse csv file " + path + " at row # " + strconv.Itoa(rowcounter) + " : invalid syntax")}
				return
			}

			wc.Add(row[0], v)
			rowcounter++

			if mapInBytes(len(wc.data)) > workerBuffer {
				select {
					case c <- workerResult{wc, confirm, err}:
						<- confirm
						wc.Clear()
					case <-done:
						return
				}
			}
		}

		select {
			case c <- workerResult{wc, confirm, err}:
				<- confirm
				wc.Clear()
			case <-done:
				return
		}
	}
}

func AggregateAll(root string) (error) {
	m := NewCache(true)
	done := make(chan struct{})
	defer close(done)
	paths, errc := walkFiles(done, root)

	c := make(chan workerResult)
	var wg sync.WaitGroup

	wg.Add(numWorkers)
	for i := 0; i < numWorkers; i++ {
		go func() {
			AggregateUnit(done, paths, c)
			wg.Done()
		}()
	}
	go func() {
		wg.Wait()
		close(c)
	}()

	for r := range c {
		if r.err != nil {
			done <- struct{}{}
			return r.err
		}

		for key, value := range r.data {
			m.Add(key, value)
		}
		r.confirm <- struct{}{}

		if mapInBytes(len(m.data)) > mainBuffer {
			m.Store(root)
			m.Clear()
		}
		//PrintMemUsage()
	}
	m.Store(root)

	if err := <-errc; err != nil {
		return err
	}

	return nil
}

func main() {
	path := strings.TrimRight(os.Args[1], "/")
	err := os.MkdirAll(path + "/temp", os.ModePerm)
	if err != nil {
		fmt.Println(err)
		return
	}
	RemoveContents(path + "/temp")
	os.Remove(path + "/__aggregate.result")

	err = AggregateAll(path)
	if err != nil {
		fmt.Println(err)
		return
	}

	err = makeResult(path)
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Println("Result:", path + "/__aggregate.result")

	RemoveContents(path + "/temp")
}