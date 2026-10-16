# Go Dataloader

This is a dataloader implementation copying from https://github.com/vektah/dataloaden
but using golang generic instead of code generation

## Requirements

- Golang 1.26

## Installation

```sh
$ go get github.com/cychiuae/go-dataloader
```

## Example

```golang
userDataLoader := NewDataLoader(DataLoaderConfig[string, *User]{
  Fetch: func(userIDs []string) ([]*User, []error) {
    return query.QueryUsersByIDs(userIDs)
  },
  MaxBatch: 0,
  Wait:     100 * time.Millisecond,
})

user, err := userDataLoader.Load("user-id")
```

## Behaviour

- Loads made within `Wait` of the first one are sent to `Fetch` together, at most
  `MaxBatch` keys at a time (`0` means no limit). A key that is already being
  fetched is not fetched again.
- `Fetch` returns one value per key, in the same order as the keys. It can return
  one error per key, a single error that applies to every key, or `nil`.
- If `Fetch` panics, or returns too few values or the wrong number of errors, the
  affected keys get an error instead.
- Successful values are cached, errors are not. Use `Prime` to add a value to the
  cache and `Clear` to remove one.
- The cache is never evicted, so create a new dataloader per request rather than
  sharing one for the lifetime of the program.
