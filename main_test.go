package main

import (
	"testing"
	"github.com/stretchr/testify/require"
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

