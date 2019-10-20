package aggregator

import (
	"os"
	"path/filepath"

	"github.com/spf13/afero"
)

func createTempDir(path string, fs afero.Fs) error {
	err := fs.MkdirAll(path+"/temp", os.ModePerm)
	if err != nil {
		return err
	}

	return nil
}

func makeClean(temppath string, resultpath string, fs afero.Fs) error {
	err := removeContents(temppath, fs)
	if err != nil {
		return err
	}
	_ = os.Remove(resultpath)

	return nil
}

func removeContents(dir string, fs afero.Fs) error {
	d, err := fs.Open(dir)
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