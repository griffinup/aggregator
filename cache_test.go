package aggregator

import (
	"github.com/stretchr/testify/require"
	"testing"
)

var globalcache *cache = &cache{
	isGlobal: true,
}

var nonglobalcache *cache = &cache{
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
