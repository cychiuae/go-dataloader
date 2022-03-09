# Go Dataloader

This is a dataloader implementation copying from https://github.com/vektah/dataloaden
but using golang generic instead of code generation

## Requirements

- Golang 1.18

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
