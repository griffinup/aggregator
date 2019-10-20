package aggregator

import (
	"fmt"
	"sync"
	"unsafe"
)

type Cache struct {
	isGlobal bool
	data     sync.Map
}

func newCache(isglobal bool) *Cache {
	return &Cache{
		isGlobal: isglobal,
		data:     sync.Map{},
	}
}

func (c *Cache) save(key string, value []interface{}) {
		c.data.Store(key, value)
}

func (c *Cache) load(key string) ([]interface{}, bool)  {
	if v, ok := c.data.Load(key); ok {
		return v.([]interface{}), ok
	}
	return nil, false
}

func (c *Cache) dataLen() int {
	counter := 0
	c.data.Range(func(key interface{}, value interface{}) bool {
		counter++
		return true
	})
	return counter
}

func (c *Cache) dump(ac *AggContainer) {
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

func (c *Cache) clear() {
	c.data = sync.Map{}
}

func mapInBytes(maplen int) int {
	var memstring string
	var memvector interface{}

	return maplen*8 + (maplen * 8 * (int)(unsafe.Sizeof(memstring))) + (maplen * 8 * (int)(unsafe.Sizeof(memvector)))
}