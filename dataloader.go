package godataloader

import (
	"sync"
	"time"
)

type DataLoaderConfig[Key comparable, T any] struct {
	Fetch    func(keys []Key) ([]T, []error)
	Wait     time.Duration
	MaxBatch int
}

type DataLoader[Key comparable, T any] struct {
	fetch    func(keys []Key) ([]T, []error)
	wait     time.Duration
	maxBatch int
	cache    map[Key]T
	batch    *loaderBatch[Key, T]
	mu       sync.Mutex
}

func NewDataLoader[Key comparable, T any](config DataLoaderConfig[Key, T]) *DataLoader[Key, T] {
	return &DataLoader[Key, T]{
		fetch:    config.Fetch,
		wait:     config.Wait,
		maxBatch: config.MaxBatch,
	}
}

type loaderBatch[Key comparable, T any] struct {
	keys    []Key
	data    []T
	error   []error
	closing bool
	done    chan struct{}
}

func (l *DataLoader[Key, T]) Load(key Key) (T, error) {
	return l.LoadThunk(key)()
}

// LoadThunk returns a function that when called will block waiting for a Asset.
// This method should be used if you want one goroutine to make requests to many
// different data loaders without blocking until the thunk is called.
func (l *DataLoader[Key, T]) LoadThunk(key Key) func() (T, error) {
	l.mu.Lock()
	if it, ok := l.cache[key]; ok {
		l.mu.Unlock()
		return func() (T, error) {
			return it, nil
		}
	}
	if l.batch == nil {
		l.batch = &loaderBatch[Key, T]{
			done: make(chan struct{}),
		}
	}
	batch := l.batch
	pos := batch.keyIndex(l, key)
	l.mu.Unlock()

	return func() (T, error) {
		<-batch.done

		var data T
		if pos < len(batch.data) {
			data = batch.data[pos]
		}

		var err error
		// its convenient to be able to return a single error for everything
		if len(batch.error) == 1 {
			err = batch.error[0]
		} else if batch.error != nil {
			err = batch.error[pos]
		}

		if err == nil {
			l.mu.Lock()
			l.unsafeSet(key, data)
			l.mu.Unlock()
		}

		return data, err
	}
}

func (l *DataLoader[Key, T]) LoadAll(keys []Key) ([]T, []error) {
	results := make([]func() (T, error), len(keys))

	for i, key := range keys {
		results[i] = l.LoadThunk(key)
	}

	assets := make([]T, len(keys))
	errors := make([]error, len(keys))
	for i, thunk := range results {
		assets[i], errors[i] = thunk()
	}
	return assets, errors
}

func (l *DataLoader[Key, T]) LoadAllThunk(keys []Key) func() ([]T, []error) {
	results := make([]func() (T, error), len(keys))
	for i, key := range keys {
		results[i] = l.LoadThunk(key)
	}
	return func() ([]T, []error) {
		assets := make([]T, len(keys))
		errors := make([]error, len(keys))
		for i, thunk := range results {
			assets[i], errors[i] = thunk()
		}
		return assets, errors
	}
}

func (l *DataLoader[Key, T]) Prime(key Key, value T) bool {
	l.mu.Lock()
	var found bool
	if _, found = l.cache[key]; !found {
		// make a copy when writing to the cache, its easy to pass a pointer in from a loop var
		// and end up with the whole cache pointing to the same value.
		l.unsafeSet(key, value)
	}
	l.mu.Unlock()
	return !found
}

func (l *DataLoader[Key, T]) Clear(key Key) {
	l.mu.Lock()
	delete(l.cache, key)
	l.mu.Unlock()
}

func (l *DataLoader[Key, T]) unsafeSet(key Key, value T) {
	if l.cache == nil {
		l.cache = map[Key]T{}
	}
	l.cache[key] = value
}

func (b *loaderBatch[Key, T]) keyIndex(l *DataLoader[Key, T], key Key) int {
	for i, existingKey := range b.keys {
		if key == existingKey {
			return i
		}
	}

	pos := len(b.keys)
	b.keys = append(b.keys, key)
	if pos == 0 {
		go b.startTimer(l)
	}

	if l.maxBatch != 0 && pos >= l.maxBatch-1 {
		if !b.closing {
			b.closing = true
			l.batch = nil
			go b.end(l)
		}
	}

	return pos
}

func (b *loaderBatch[Key, T]) startTimer(l *DataLoader[Key, T]) {
	time.Sleep(l.wait)
	l.mu.Lock()

	if b.closing {
		l.mu.Unlock()
		return
	}

	l.batch = nil
	l.mu.Unlock()

	b.end(l)
}

func (b *loaderBatch[Key, T]) end(l *DataLoader[Key, T]) {
	b.data, b.error = l.fetch(b.keys)
	close(b.done)
}
