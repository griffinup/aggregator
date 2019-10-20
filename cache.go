package aggregator

import (
	"fmt"
	"sync"
	"unsafe"
)

type cache struct {
	isGlobal bool
	data     sync.Map
}

func newCache(isglobal bool) *cache {
	return &cache{
		isGlobal: isglobal,
		data:     sync.Map{},
	}
}

func (c *cache) save(key string, value []interface{}) {
	c.data.Store(key, value)
}

func (c *cache) load(key string) ([]interface{}, bool) {
	if v, ok := c.data.Load(key); ok {
		return v.([]interface{}), ok
	}
	return nil, false
}

func (c *cache) dataLen() int {
	counter := 0
	c.data.Range(func(key interface{}, value interface{}) bool {
		counter++
		return true
	})
	return counter
}

func (c *cache) dump(ac *AggContainer) {
	if !c.isGlobal {
		return
	}

	c.data.Range(func(key interface{}, value interface{}) bool {
		err := ac.addEntryToFile(fmt.Sprintf("%v", key), value.([]interface{}))
		if err != nil {
			return false
		}
		return true
	})
}

func (c *cache) clear() {
	c.data = sync.Map{}
}

func mapInBytes(maplen int) int {
	var memKey string
	var memValue []interface{}

	return maplen*8 + (maplen * 8 * (int)(unsafe.Sizeof(memKey))) + (maplen * 8 * (int)(unsafe.Sizeof(memValue)))
}
