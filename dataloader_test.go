package godataloader

import (
	"strconv"
	"sync"
	"testing"
	"time"
)

func TestDataLoader(t *testing.T) {
	integerDataLoader := NewDataLoader(DataLoaderConfig[string, int]{
		Fetch: func(keys []string) ([]int, []error) {
			result := make([]int, 0, len(keys))
			errors := make([]error, 0, len(keys))
			for _, k := range keys {
				value, err := strconv.Atoi(k)
				result = append(result, value)
				errors = append(errors, err)
			}
			return result, errors
		},
		MaxBatch: 0,
		Wait:     10 * time.Millisecond,
	})
	if integerDataLoader == nil {
		t.Fatal("Fail to create a dataloader")
	}
	numberZero, err := integerDataLoader.Load("0")
	if numberZero != 0 {
		t.Fatalf("Loader returns wrong number %d", numberZero)
	}
	if err != nil {
		t.Fatal("Loader returns err")
	}

	numberOne, err := integerDataLoader.Load("1")
	if numberOne != 1 {
		t.Fatalf("Loader returns wrong number %d", numberOne)
	}
	if err != nil {
		t.Fatal("Loader returns err")
	}
	if cacheSize := len(integerDataLoader.cache); cacheSize != 2 {
		t.Fatal("Loader has wrong cache size")
	}
}

func TestModelDataLoader(t *testing.T) {
	type User struct {
		ID string
	}

	userDataLoader := NewDataLoader(DataLoaderConfig[string, *User]{
		Fetch: func(userIDs []string) ([]*User, []error) {
			result := make([]*User, 0, len(userIDs))
			errors := make([]error, 0, len(userIDs))
			for _, userID := range userIDs {
				if userID != "missing" {
					result = append(result, &User{ID: userID})
				} else {
					result = append(result, nil)
				}
				errors = append(errors, nil)
			}
			return result, errors
		},
		MaxBatch: 0,
		Wait:     100 * time.Millisecond,
	})

	var wg sync.WaitGroup

	for i := 0; i < 10; i++ {
		wg.Add(1)

		userID := func() string {
			if i%3 == 0 {
				return "missing"
			}
			return strconv.Itoa(i)
		}()
		go func() {
			defer wg.Done()
			user, err := userDataLoader.Load(userID)
			if err != nil {
				t.Fatal("Loader err")
			}
			if userID == "missing" {
				if user != nil {
					t.Fatal("Expect nil User")
				}
			} else {
				if userID != user.ID {
					t.Fatal("Wrong user ID")
				}
			}
		}()
	}

	wg.Wait()
}
