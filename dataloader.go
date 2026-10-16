package godataloader

import (
	"fmt"
	"sync"
	"time"
)

// DataLoaderConfig configures a DataLoader.
type DataLoaderConfig[Key comparable, T any] struct {
	// Fetch loads the values for a batch of distinct keys. It returns one value
	// per key, in the same order as keys. Errors can be returned one per key in
	// the same order, as a single error that applies to every key, or as nil
	// when every key succeeded.
	Fetch func(keys []Key) ([]T, []error)
	// Wait is how long a batch collects keys before it is sent to Fetch.
	Wait time.Duration
	// MaxBatch is the most keys sent to Fetch at once. Zero means no limit.
	MaxBatch int
}

// DataLoader batches loads of values by key into calls to Fetch and caches
// the values it loads. The cache is never evicted, so a DataLoader is usually
// created per request.
type DataLoader[Key comparable, T any] struct {
	fetch    func(keys []Key) ([]T, []error)
	wait     time.Duration
	maxBatch int

	mu sync.Mutex
	// cache holds values from successful fetches and Prime.
	cache map[Key]T
	// pending holds the keys whose batch has not finished fetching.
	pending map[Key]pendingLoad[Key, T]
	// batch is the batch still collecting keys, if any.
	batch *loaderBatch[Key, T]
}

// NewDataLoader returns a DataLoader that loads values with config.Fetch.
func NewDataLoader[Key comparable, T any](config DataLoaderConfig[Key, T]) *DataLoader[Key, T] {
	return &DataLoader[Key, T]{
		fetch:    config.Fetch,
		wait:     config.Wait,
		maxBatch: config.MaxBatch,
		cache:    map[Key]T{},
		pending:  map[Key]pendingLoad[Key, T]{},
	}
}

type pendingLoad[Key comparable, T any] struct {
	batch *loaderBatch[Key, T]
	pos   int
}

type loaderBatch[Key comparable, T any] struct {
	keys    []Key
	results []result[T]
	timer   *time.Timer
	// closing is set once the batch stops collecting keys.
	closing bool
	done    chan struct{}
}

type result[T any] struct {
	value T
	err   error
}

// Load returns the value for key, waiting for its batch to be fetched if it
// is not cached.
func (l *DataLoader[Key, T]) Load(key Key) (T, error) {
	return l.LoadThunk(key)()
}

// LoadThunk adds key to a batch and returns a function that blocks until the
// value for key is loaded. Use it to start loads from several DataLoaders in
// one goroutine before waiting on any of them.
func (l *DataLoader[Key, T]) LoadThunk(key Key) func() (T, error) {
	l.mu.Lock()
	if value, ok := l.cache[key]; ok {
		l.mu.Unlock()
		return func() (T, error) {
			return value, nil
		}
	}
	p, ok := l.pending[key]
	if !ok {
		p = l.enqueue(key)
	}
	l.mu.Unlock()

	return func() (T, error) {
		<-p.batch.done
		r := p.batch.results[p.pos]
		return r.value, r.err
	}
}

// LoadAll returns the values and errors for keys, in the same order as keys.
func (l *DataLoader[Key, T]) LoadAll(keys []Key) ([]T, []error) {
	return l.LoadAllThunk(keys)()
}

// LoadAllThunk is LoadThunk for several keys.
func (l *DataLoader[Key, T]) LoadAllThunk(keys []Key) func() ([]T, []error) {
	thunks := make([]func() (T, error), len(keys))
	for i, key := range keys {
		thunks[i] = l.LoadThunk(key)
	}
	return func() ([]T, []error) {
		values := make([]T, len(keys))
		errs := make([]error, len(keys))
		for i, thunk := range thunks {
			values[i], errs[i] = thunk()
		}
		return values, errs
	}
}

// Prime caches value for key unless key is already cached, and reports
// whether it did. A primed value is not replaced by a fetch already in
// progress.
func (l *DataLoader[Key, T]) Prime(key Key, value T) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	if _, found := l.cache[key]; found {
		return false
	}
	l.cache[key] = value
	return true
}

// Clear removes key from the cache. A fetch of key that has already started
// does not cache its value, so the next Load fetches key again.
func (l *DataLoader[Key, T]) Clear(key Key) {
	l.mu.Lock()
	defer l.mu.Unlock()
	delete(l.cache, key)
	if p, ok := l.pending[key]; ok && p.batch.closing {
		delete(l.pending, key)
	}
}

// enqueue adds key to the batch collecting keys, starting one if needed.
// l.mu must be held.
func (l *DataLoader[Key, T]) enqueue(key Key) pendingLoad[Key, T] {
	b := l.batch
	if b == nil {
		b = &loaderBatch[Key, T]{done: make(chan struct{})}
		b.timer = time.AfterFunc(l.wait, func() { l.closeBatch(b) })
		l.batch = b
	}

	p := pendingLoad[Key, T]{batch: b, pos: len(b.keys)}
	b.keys = append(b.keys, key)
	l.pending[key] = p

	if l.maxBatch > 0 && len(b.keys) >= l.maxBatch {
		b.timer.Stop()
		b.closing = true
		l.batch = nil
		go l.finish(b)
	}
	return p
}

// closeBatch fetches b when its wait is over, unless it filled up first.
func (l *DataLoader[Key, T]) closeBatch(b *loaderBatch[Key, T]) {
	l.mu.Lock()
	if b.closing {
		l.mu.Unlock()
		return
	}
	b.closing = true
	l.batch = nil
	l.mu.Unlock()

	l.finish(b)
}

// finish fetches b, caches its values and wakes up the loads waiting on it.
func (l *DataLoader[Key, T]) finish(b *loaderBatch[Key, T]) {
	b.results = l.fetchResults(b.keys)

	l.mu.Lock()
	for i, key := range b.keys {
		// Clear stops a key from being pending on b, because the value
		// fetched for it may be out of date.
		if l.pending[key].batch != b {
			continue
		}
		delete(l.pending, key)
		if _, primed := l.cache[key]; !primed && b.results[i].err == nil {
			l.cache[key] = b.results[i].value
		}
	}
	l.mu.Unlock()

	close(b.done)
}

// fetchResults calls Fetch and returns one result per key. If Fetch panics or
// returns too few values or the wrong number of errors, the affected keys get
// an error instead.
func (l *DataLoader[Key, T]) fetchResults(keys []Key) (results []result[T]) {
	results = make([]result[T], len(keys))
	defer func() {
		if r := recover(); r != nil {
			err := fmt.Errorf("dataloader: fetch panicked: %v", r)
			for i := range results {
				results[i] = result[T]{err: err}
			}
		}
	}()

	values, errs := l.fetch(keys)
	if len(errs) > 1 && len(errs) != len(keys) {
		err := fmt.Errorf("dataloader: fetch returned %d errors for %d keys", len(errs), len(keys))
		for i := range results {
			results[i].err = err
		}
		return results
	}

	for i := range keys {
		switch len(errs) {
		case 0:
		case 1:
			results[i].err = errs[0]
		default:
			results[i].err = errs[i]
		}
		if i < len(values) {
			results[i].value = values[i]
		} else if results[i].err == nil {
			results[i].err = fmt.Errorf("dataloader: fetch returned %d values for %d keys", len(values), len(keys))
		}
	}
	return results
}
