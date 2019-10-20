package aggregator

import (
	"testing"
	"github.com/stretchr/testify/require"
)

var globalcache *Cache = &Cache{
	isGlobal: true,
}

var nonglobalcache *Cache = &Cache{
	isGlobal: false,
}

func TestNewCache(t *testing.T) {
	require.Equal(t, globalcache, newCache(true), "should create correctly")
	require.Equal(t, nonglobalcache, newCache(false), "should create correctly")
}

func TestClear(t *testing.T) {
	c := newCache(false)
	var interfaceSlice = make([]interface{}, 1)

	interfaceSlice = append(interfaceSlice, "abc")

	c.save("test", interfaceSlice)
	require.NotEqual(t, nonglobalcache, c, "should add correctly")
	c.clear()
	require.Equal(t, nonglobalcache, c, "should clear correctly")
}

//func TestAddVectorToFile(t *testing.T) {
//	var appFs = afero.NewMemMapFs()
//
//	appFs.MkdirAll("test/temp/", 0755)
//
//	addVectorToFile("test/", "2019-10-10", Vector{1., 1., 1.}, appFs)
//
//	_, err := appFs.Stat("test/temp/2019-10-10.tmp")
//	if os.IsNotExist(err) {
//		t.Errorf("file \"%s\" does not exist.\n", "test/temp/2019-10-10.tmp")
//	}
//	addVectorToFile("test/", "2019-10-10", Vector{2., 2., 2.}, appFs)
//
//	if b, _ := afero.FileContainsBytes(appFs, "test/temp/2019-10-10.tmp", []byte("2019-10-10;3;3;3\n")); b != true {
//		t.Errorf("file's \"%s\" content didn't match.\n", "test/2019-10-10.tmp")
//	}
//}
