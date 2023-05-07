// Package register has a registration method that a goroutine.Pool implementation can use
// to register the pool. This is useful to allow gathering of metrics data for OTEL enabled
// applications.
package register

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"sync"

	"github.com/gostdlib/concurrency/goroutines"

	_ "unsafe"
)

var registry = map[string]goroutines.Pool{}
var mu = sync.RWMutex{}

// Register registers a name for a pool in the registry.
func Register(pool goroutines.Pool) error {
	mu.Lock()
	defer mu.Unlock()

	name := pool.GetName()
	if name == "" {
		return nil
	}

	if _, ok := registry[name]; ok {
		return fmt.Errorf("name already taken")
	}

	registry[name] = pool
	return nil
}

// Unregister unregisters the pool from the registry.
func Unregister(pool goroutines.Pool) {
	mu.Lock()
	delete(registry, pool.GetName())
	mu.Unlock()
}

var numOrHyphen = regexp.MustCompile(`[0-9-\s]`)

// ValidateBaseName returns an error if the name contains numbers or hyphens.
func ValidateBaseName(name string) error {
	if numOrHyphen.MatchString(name) {
		return fmt.Errorf("name cannot contain numbers or hyphens")
	}
	return nil
}

// NewName takes the base name of the pool and returns a unique name for the pool by trying
// the next number until it finds a unique name.
func NewName(name string) string {
	if !numOrHyphen.MatchString(name) {
		return name + "-1"
	}

	sp := strings.SplitAfter(name, "-")
	n, err := strconv.Atoi(sp[1])
	if err != nil {
		panic(fmt.Sprintf("register is broken, name %s is invalid", name))
	}

	n++
	return fmt.Sprintf("%s-%d", sp[0], n)
}

// Pools returns all pools registered by this package. Order is non-deterministic.
func Pools() chan goroutines.Pool {
	ch := make(chan goroutines.Pool, 1)
	go func() {
		mu.RLock()
		defer mu.RUnlock()
		for _, p := range registry {
			ch <- p
		}
	}()
	return ch
}
