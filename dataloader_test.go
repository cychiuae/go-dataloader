package godataloader

import (
	"errors"
	"slices"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestDataLoader(t *testing.T) {
	integerDataLoader := NewDataLoader(DataLoaderConfig[string, int]{
		Fetch: func(keys []string) ([]int, []error) {
			result := make([]int, 0, len(keys))
			errs := make([]error, 0, len(keys))
			for _, k := range keys {
				value, err := strconv.Atoi(k)
				result = append(result, value)
				errs = append(errs, err)
			}
			return result, errs
		},
		MaxBatch: 0,
		Wait:     10 * time.Millisecond,
	})
	if integerDataLoader == nil {
		t.Fatal("Fail to create a dataloader")
	}
	numberZero, err := integerDataLoader.Load("0")
	if err != nil {
		t.Fatal("Loader returns err")
	}
	if numberZero != 0 {
		t.Fatalf("Loader returns wrong number %d", numberZero)
	}

	numberOne, err := integerDataLoader.Load("1")
	if err != nil {
		t.Fatal("Loader returns err")
	}
	if numberOne != 1 {
		t.Fatalf("Loader returns wrong number %d", numberOne)
	}
	if cacheSize := len(integerDataLoader.cache); cacheSize != 2 {
		t.Fatalf("Loader has wrong cache size %d", cacheSize)
	}
}

func TestModelDataLoader(t *testing.T) {
	type User struct {
		ID string
	}

	var batches [][]string
	var mu sync.Mutex
	userDataLoader := NewDataLoader(DataLoaderConfig[string, *User]{
		Fetch: func(userIDs []string) ([]*User, []error) {
			mu.Lock()
			batches = append(batches, userIDs)
			mu.Unlock()
			result := make([]*User, 0, len(userIDs))
			for _, userID := range userIDs {
				if userID != "missing" {
					result = append(result, &User{ID: userID})
				} else {
					result = append(result, nil)
				}
			}
			return result, nil
		},
		MaxBatch: 0,
		Wait:     100 * time.Millisecond,
	})

	var wg sync.WaitGroup
	for i := range 10 {
		userID := "missing"
		if i%3 != 0 {
			userID = strconv.Itoa(i)
		}
		wg.Go(func() {
			user, err := userDataLoader.Load(userID)
			if err != nil {
				t.Error("Loader err")
				return
			}
			if userID == "missing" {
				if user != nil {
					t.Error("Expect nil User")
				}
			} else if userID != user.ID {
				t.Error("Wrong user ID")
			}
		})
	}
	wg.Wait()

	if len(batches) != 1 {
		t.Fatalf("Expect 1 fetch, got %d", len(batches))
	}
	if len(batches[0]) != 7 {
		t.Fatalf("Expect 7 distinct keys in the batch, got %v", batches[0])
	}
}

// newCountingLoader returns a loader of int keys to their string form that
// records every batch it fetches.
func newCountingLoader(maxBatch int) (*DataLoader[int, string], func() [][]int) {
	var mu sync.Mutex
	var batches [][]int
	l := NewDataLoader(DataLoaderConfig[int, string]{
		Fetch: func(keys []int) ([]string, []error) {
			mu.Lock()
			batches = append(batches, slices.Clone(keys))
			mu.Unlock()
			values := make([]string, len(keys))
			for i, k := range keys {
				values[i] = strconv.Itoa(k)
			}
			return values, nil
		},
		Wait:     time.Millisecond,
		MaxBatch: maxBatch,
	})
	return l, func() [][]int {
		mu.Lock()
		defer mu.Unlock()
		return slices.Clone(batches)
	}
}

func TestLoadAll(t *testing.T) {
	l, batches := newCountingLoader(0)

	values, errs := l.LoadAll([]int{1, 2, 1, 3})
	if want := []string{"1", "2", "1", "3"}; !slices.Equal(values, want) {
		t.Fatalf("Expect %v, got %v", want, values)
	}
	for i, err := range errs {
		if err != nil {
			t.Fatalf("Unexpected error for key %d: %v", i, err)
		}
	}
	if got := batches(); len(got) != 1 || !slices.Equal(got[0], []int{1, 2, 3}) {
		t.Fatalf("Expect one batch of [1 2 3], got %v", got)
	}

	values, _ = l.LoadAllThunk([]int{3, 4})()
	if want := []string{"3", "4"}; !slices.Equal(values, want) {
		t.Fatalf("Expect %v, got %v", want, values)
	}
	if got := batches(); len(got) != 2 || !slices.Equal(got[1], []int{4}) {
		t.Fatalf("Expect cached key 3 not to be fetched again, got %v", got)
	}
}

func TestMaxBatch(t *testing.T) {
	l, batches := newCountingLoader(2)

	l.LoadAll([]int{1, 2, 3, 4, 5})

	got := batches()
	if len(got) != 3 {
		t.Fatalf("Expect 3 batches, got %v", got)
	}
	for _, b := range got {
		if len(b) > 2 {
			t.Fatalf("Batch %v is bigger than MaxBatch", b)
		}
	}
}

func TestFetchErrors(t *testing.T) {
	errBoom := errors.New("boom")
	tests := []struct {
		name  string
		fetch func(keys []int) ([]int, []error)
		// wantErrs reports, per key, whether that key should get an error.
		wantErrs []bool
	}{
		{
			name: "per key",
			fetch: func(keys []int) ([]int, []error) {
				return make([]int, len(keys)), []error{nil, errBoom, nil}
			},
			wantErrs: []bool{false, true, false},
		},
		{
			name: "single error for every key",
			fetch: func(keys []int) ([]int, []error) {
				return nil, []error{errBoom}
			},
			wantErrs: []bool{true, true, true},
		},
		{
			name: "wrong number of errors",
			fetch: func(keys []int) ([]int, []error) {
				return make([]int, len(keys)), []error{nil, nil}
			},
			wantErrs: []bool{true, true, true},
		},
		{
			name: "empty errors",
			fetch: func(keys []int) ([]int, []error) {
				return make([]int, len(keys)), []error{}
			},
			wantErrs: []bool{false, false, false},
		},
		{
			name: "too few values",
			fetch: func(keys []int) ([]int, []error) {
				return make([]int, 2), nil
			},
			wantErrs: []bool{false, false, true},
		},
		{
			name: "panic",
			fetch: func(keys []int) ([]int, []error) {
				panic("boom")
			},
			wantErrs: []bool{true, true, true},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			l := NewDataLoader(DataLoaderConfig[int, int]{Fetch: tt.fetch, Wait: time.Millisecond})
			_, errs := l.LoadAll([]int{1, 2, 3})
			for i, err := range errs {
				if (err != nil) != tt.wantErrs[i] {
					t.Errorf("Key %d: got error %v, want error: %v", i, err, tt.wantErrs[i])
				}
			}
		})
	}
}

func TestErrorIsNotCached(t *testing.T) {
	var calls atomic.Int32
	l := NewDataLoader(DataLoaderConfig[int, int]{
		Fetch: func(keys []int) ([]int, []error) {
			if calls.Add(1) == 1 {
				return nil, []error{errors.New("boom")}
			}
			return make([]int, len(keys)), nil
		},
		Wait: time.Millisecond,
	})

	if _, err := l.Load(1); err == nil {
		t.Fatal("Expect the first load to fail")
	}
	if _, err := l.Load(1); err != nil {
		t.Fatalf("Expect the second load to fetch again and succeed, got %v", err)
	}
}

func TestPrime(t *testing.T) {
	l, batches := newCountingLoader(0)

	if !l.Prime(1, "primed") {
		t.Fatal("Expect Prime to cache a new key")
	}
	if l.Prime(1, "again") {
		t.Fatal("Expect Prime not to replace a cached key")
	}
	if value, _ := l.Load(1); value != "primed" {
		t.Fatalf("Expect primed value, got %q", value)
	}
	if got := batches(); len(got) != 0 {
		t.Fatalf("Expect no fetch for a primed key, got %v", got)
	}
}

func TestClear(t *testing.T) {
	l, batches := newCountingLoader(0)

	l.Load(1)
	l.Clear(1)
	l.Load(1)

	if got := batches(); len(got) != 2 {
		t.Fatalf("Expect a cleared key to be fetched again, got %v", got)
	}
}

// blockingLoader returns a loader whose Fetch signals started and then waits
// for release. Each fetch returns version for every key.
func blockingLoader(version *atomic.Int32, started chan<- struct{}, release <-chan struct{}) (*DataLoader[int, int32], *atomic.Int32) {
	var calls atomic.Int32
	l := NewDataLoader(DataLoaderConfig[int, int32]{
		Fetch: func(keys []int) ([]int32, []error) {
			calls.Add(1)
			v := version.Load()
			started <- struct{}{}
			<-release
			values := make([]int32, len(keys))
			for i := range values {
				values[i] = v
			}
			return values, nil
		},
		Wait: time.Millisecond,
	})
	return l, &calls
}

func TestLoadDuringFetchSharesIt(t *testing.T) {
	var version atomic.Int32
	started, release := make(chan struct{}, 1), make(chan struct{})
	l, calls := blockingLoader(&version, started, release)

	first := l.LoadThunk(1)
	<-started
	second := l.LoadThunk(1)
	close(release)
	first()
	second()

	if got := calls.Load(); got != 1 {
		t.Fatalf("Expect 1 fetch for a key already being fetched, got %d", got)
	}
}

func TestClearDuringFetch(t *testing.T) {
	var version atomic.Int32
	version.Store(1)
	started, release := make(chan struct{}, 2), make(chan struct{})
	l, _ := blockingLoader(&version, started, release)

	thunk := l.LoadThunk(1)
	<-started
	l.Clear(1)
	version.Store(2)
	close(release)

	if value, _ := thunk(); value != 1 {
		t.Fatalf("Expect the load started before Clear to return 1, got %d", value)
	}
	if value, _ := l.Load(1); value != 2 {
		t.Fatalf("Expect a load after Clear to fetch the new value 2, got %d", value)
	}
}
