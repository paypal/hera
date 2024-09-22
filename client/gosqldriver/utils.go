package gosqldriver

import (
	"errors"
	"sync"
	"sync/atomic"
)

var ErrInvalidConn = errors.New("invalid connection")

// atomicError provides thread-safe error handling
type atomicError struct {
	value atomic.Value
	mu    sync.Mutex
}

// Set sets the error value atomically. The value must not be nil.
func (ae *atomicError) Set(err error) {
	if err == nil {
		panic("atomicError: nil error value")
	}
	ae.mu.Lock()
	defer ae.mu.Unlock()
	ae.value.Store(err)
}

// Value returns the current error value, or nil if none is set.
func (ae *atomicError) Value() error {
	v := ae.value.Load()
	if v == nil {
		return nil
	}
	return v.(error)
}

type atomicBool struct {
	value uint32
	mu    sync.Mutex
}

// Store sets the value of the bool regardless of the previous value
func (ab *atomicBool) Store(value bool) {
	ab.mu.Lock()
	defer ab.mu.Unlock()
	if value {
		atomic.StoreUint32(&ab.value, 1)
	} else {
		atomic.StoreUint32(&ab.value, 0)
	}
}

// Load returns whether the current boolean value is true
func (ab *atomicBool) Load() bool {
	ab.mu.Lock()
	defer ab.mu.Unlock()
	return atomic.LoadUint32(&ab.value) > 0
}

// Swap sets the value of the bool and returns the old value.
func (ab *atomicBool) Swap(value bool) bool {
	ab.mu.Lock()
	defer ab.mu.Unlock()
	if value {
		return atomic.SwapUint32(&ab.value, 1) > 0
	}
	return atomic.SwapUint32(&ab.value, 0) > 0
}
