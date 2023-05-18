# `wait`: A Safer WaitGroup for Production Applications

[![Go Reference](https://pkg.go.dev/badge/github.com/gostdlib/concurrency/concurrency.svg)](https://pkg.go.dev/github.com/gostdlib/concurrency/slices)
[![Go Report Card](https://goreportcard.com/badge/github.com/gostdlib/concurrency)](https://goreportcard.com/report/github.com/gostdlib/concurrency)

## Introduction

The `wait` package provides a safer alternative to `sync.WaitGroup` for managing parallel operations in production applications. It addresses common issues such as counter decrementation errors and offers built-in capabilities for parallel operations using various pool primitives for goroutine reuse. Additionally, it supports OpenTelemetry tracing for application diagnostics. It's important to note that the operations performed by this package are parallel rather than concurrent, as defined in the famous Go talk by Rob Pike: [link to Go talk](https://go.dev/blog/waza-talk). We also offer parallel and concurrent pipelines in our [github.com/gostdlib/concurrency/pipelines](https://github.com/gostdlib/concurrency/pipelines) package set.

## Installation

To install the `wait` package, use the following command:

```sh
go get github.com/gostdlib/concurrency/
```

## Key Features

- Use [`wait.Group`](https://pkg.go.dev/github.com/gostdlib/prim#WaitGroup) if you need:
  - A safer version of `sync.WaitGroup` for parallel jobs
  - A parallel job runner that collects errors after all jobs complete
  - A parallel job runner that can stop processing on the first error
  - A parallel job runner that can be cancelled with a `Context`
  - Reuse and limitation of goroutines by supplying a [`goroutines.Pool`](https://pkg.go.dev/github.com/gostdlib/goroutiones#Pool)
  - Integration with your preferred logging solution, including support for OpenTelemetry spans

## Package Purpose

The `wait` package aims to achieve the following goals:

### Reduce Programmatic Errors

#### Deadlocks

Consider the following example that demonstrates a common mistake when using `sync.WaitGroup`:

```go
func main() {
    wg := sync.WaitGroup{}

    for i := 0; i < 100; i++ {
        i := i
        wg.Add(1)
        go func() {
            // <- There should be a defer wg.Done() here
            fmt.Println(i)
        }()
    }

    wg.Wait()
}
```

In this code, there is a missing `defer wg.Done()` statement, leading to synchronization errors and potential deadlocks. With our `WaitGroup`, you can avoid this problem:

```go
func main() {
    ctx := context.Background()
    wg := prim.WaitGroup{}
   
    for i := 0; i < 100; i++ {
        i := i
        wg.Go(
            ctx,
            func(ctx context.Context) error {
               return fmt.Println(i)
            },
        )
    }
    wg.Wait()
}
```

#### Error Capturing

Old way of capturing only the first error:

```go
func main() {
    wg := sync.WaitGroup{}
    ch := make(chan error, 1)

    for i := 0; i < 100; i++ {
        i := i
        wg.Add(1)
        go func() {
            defer wg.Done()
            if i % 2 == 0 {
                select {
                case ch <- errors.New("error"):
                default:
                }
            }
            fmt.Println(i)
        }()
    }

    wg.Wait()
}
```

Using our error capturing

 approach:

```go
func main() {
    wg := prim.WaitGroup{}

    for i := 0; i < 100; i++ {
        i := i
        wg.Go(
            context.Background(),
            func(ctx context.Context) error {
                if i == 3 {
                    return errors.New("error")
                }
                return fmt.Println(i)
            },
        )
    }

    if err := wg.Wait(); err != nil {
        panic(err) // or handle the error as per your requirements
    }

    // Individual errors can be unpacked using errors.Unwrap()
}
```

To stop processing on the first error, modify the `WaitGroup` as follows:

```go
ctx, cancel := context.WithCancel(context.Background())
wg := wait.Group{CancelOnErr: cancel}
...
```

We capture all errors using error wrapping and retrieve them using the `errors.Unwrap` method. Only the `Context.Deadline` error is captured if it is the first error encountered.

### Reuse Resources and Limit Goroutines

The standard way to limit the number of goroutines is by using a channel. However, this approach creates a new goroutine each time and can lead to deadlocks if you forget to defer removal of a `struct{}` from the channel. Consider the following example:

```go
func main() {
    wg := sync.WaitGroup{}
    limit := make(chan struct{}, runtime.NumCPU())

    for i := 0; i < 100; i++ {
        i := i
        wg.Add(1)
        limit <- struct{}{}
        go func() {
            defer wg.Done()
            defer func() { <-limit }()

            fmt.Println(i)
        }()
    }

    wg.Wait()
}
```

With our `wait` package, you can limit the number of goroutines to the number of available CPUs and reuse the goroutines for subsequent calls:

```go
func main() {
    p, _ := pooled.New(runtime.NumCPU()) // Reuse goroutines and limit their number.
    wg := prim.WaitGroup{Pool: p}

    for i := 0; i < 100; i++ {
        i := i
        wg.Go(
            ctx,
            func(ctx context.Context) error {
                return fmt.Println(i)
            },
        )
    }

    if err := wg.Wait(); err != nil {
        panic(err) // or handle the error as per your requirements
    }
}
```

## Comparison with `golang.org/x/errgroup` and `github.com/sourcegraph/conc`

### Differences from `golang.org/x/errgroup`

- Our package does not provide direct support for streaming. However, you can set up pipelines similar to `errgroup` using our `concurrency/pipelines` packages, which offer a more formal approach.

### Differences from `github.com/sourcegraph/conc`

- We offer an advanced `WaitGroup` that combines the functionalities of `conc#WaitGroup` and `conc/pool#Pool`. It provides more comprehensive capabilities than both.
- Our package supports errors for goroutines instead of panics. Catching panics in goroutines is considered a bad practice. If you encounter panics in your own libraries, it's recommended to fix them if they are non-critical. For third-party panics, catch them in the goroutine and return them as errors.

### Additional Features

- Our package provides support for `goroutine` reuse, rate limiting, and more through the `concurrency/goroutines` package (optional).
- We offer integration with OpenTelemetry logging for advanced debugging.

## Acknowledgments

This package, along with `github.com/sourcegraph/conc`, builds upon the great work done in `golang.org/x/errgroup` by Bryan Mills.