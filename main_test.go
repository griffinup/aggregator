package main

import (
	"github.com/spf13/afero"
	"github.com/stretchr/testify/require"
	"os"
	"testing"
)

var globalcache *Cache = &Cache{
	isGlobal: true,
	data: make(map[string]Vector),
}

var nonglobalcache *Cache = &Cache{
	isGlobal: false,
	data: make(map[string]Vector),
}

func TestAdd(t *testing.T) {
	result := Vector{1., 1., 1.}.Add(Vector{2., 2., 2.})
	require.Equal(t, Vector{3., 3., 3.}, result, "should add correctly")
}

func TestNewCache(t *testing.T) {
	require.Equal(t, globalcache, NewCache(true), "should create correctly")
	require.Equal(t, nonglobalcache, NewCache(false), "should create correctly")
}

func TestClear(t *testing.T) {
	c := NewCache(false)
	c.Add("test", Vector{1., 1., 1.})
	require.NotEqual(t, nonglobalcache, c, "should add correctly")
	c.Clear()
	require.Equal(t, nonglobalcache, c, "should clear correctly")
}

func TestAddVectorToFile(t *testing.T) {
	var appFs = afero.NewMemMapFs()

	appFs.MkdirAll("test/temp/", 0755)

	addVectorToFile("test/", "2019-10-10", Vector{1., 1., 1.}, appFs)

	_, err := appFs.Stat("test/temp/2019-10-10.tmp")
	if os.IsNotExist(err) {
		t.Errorf("file \"%s\" does not exist.\n", "test/temp/2019-10-10.tmp")
	}
	addVectorToFile("test/", "2019-10-10", Vector{2., 2., 2.}, appFs)

	if b, _ := afero.FileContainsBytes(appFs, "test/temp/2019-10-10.tmp", []byte("2019-10-10;3;3;3\n")); b != true {
		t.Errorf("file's \"%s\" content didn't match.\n", "test/2019-10-10.tmp")
	}
}
