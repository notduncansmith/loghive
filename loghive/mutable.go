package loghive

import (
	"sync"
)

// Mutable wraps a `sync.RWMutex` with functions for reading/writing it. It is intended to be embedded within structs that require cross-thread synchronization
type Mutable struct {
	mut  *sync.RWMutex
	Name string
}

// NewMutable returns a new Mutable
func NewMutable(name string) *Mutable {
	mut := sync.RWMutex{}
	return &Mutable{&mut, name}
}

// WithRLock calls `f` while holding a Read Lock on `mut`
func (m *Mutable) WithRLock(f func() interface{}) interface{} {
	m.mut.RLock()
	defer m.mut.RUnlock()
	result := f()
	return result
}

// WithLock calls `f` while holding a Read/Write Lock on `mut`
func (m *Mutable) WithLock(f func() interface{}) interface{} {
	m.mut.Lock()
	defer m.mut.Unlock()
	result := f()
	return result
}

// DoWithRLock is like WithRLock but does not return a value
func (m *Mutable) DoWithRLock(f func()) {
	m.mut.RLock()
	defer m.mut.RUnlock()
	f()
}

// DoWithLock is like WithLock but does not return a value
func (m *Mutable) DoWithLock(f func()) {
	m.mut.Lock()
	defer m.mut.Unlock()
	f()
}
