# `Prim`: Parallel primatives for production applications

[![Go Reference](https://pkg.go.dev/badge/github.com/gostdlib/concurrency/concurrency.svg)](https://pkg.go.dev/github.com/gostdlib/concurrency/)
[![Go Report Card](https://goreportcard.com/badge/github.com/gostdlib/concurrency)](https://goreportcard.com/report/github.com/gostdlib/concurrency)

The `prim` package is your one stop shop for advanced parallel primatives to create robust and easy to diagnose applications. These are parallel and not concurrent by definitions layed out in the famous Go talk by Rob Pike: https://go.dev/blog/waza-talk . We provide parallel and concurrent pipelines in our https://github.com/gostdlib/concurrency/pipelines set of packages.

```sh
go get github.com/gostdlib/concurrency/
```

# A quick look

- Use [`prim.WaitGroup`](https://pkg.go.dev/github.com/gostdlib/prim#WaitGroup) if you want:
    - A safer version of `sync.WaitGroup` for parallel jobs
    - A parallel job runner that collects errors after all jobs complete
    - A parallel job runner that CAN stops processing on the first error
    - A parallel job runner that CAN be `Context` cancelled
    - Reuse and limiting of goroutines by supplying a [`goroutines.Pool`](https://pkg.go.dev/github.com/gostdlib/goroutiones#Pool)
    - Integration with logging of your choice, including support for OpenTelemetry spans
- Use [`prim.Slice`](https://pkg.go.dev/github.com/gostdlib/prim#Slice) if you want:
    - In place parallel processing of a slice of values
    - Support for processing errors
    - Support for OpenTelemetry spans
- Use [`prim.ResultSlice`](https://pkg.go.dev/github.com/gostdlib/prim#Slice) if you want:
    - To parallel process a slice of values and return an identical slice of values (not necessarily of the same type)
    - Support for processing errors
    - Support for OpenTelemetry spans

# Package purpose

- Reduce programatic errors (goroutine leaks, deadlocks, ...)
- Reuse resources (goroutines)
- Reduce allocations (support for standard values and pointer values) 
- Integration with OpenTelemetry `span`s by default

## Reduce programatic errors

### Deadlocks

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

This problem leads to syncronization errors where the goroutines might be editing `slice` or `map` values and we didn't wait for completion. On top of that we experience a deadlock. 

With our `WaitGroup`

```go
func main() {
    wg := prim.WaitGroup{}

    for i := 0; i < 100; i++ {
        i := i
        wg.Go(
            context.Background(),
            func(ctx context.Context) error {
               return fmt.Println(i)
            },
        )
    }
    wg.Wait()
}
```

### Error capturing

Old way that only gets the first error: 
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

Capture errors our way:

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
        panic(err) // or whatever you want to do with the error
    }
}
```

If you want it to stop on the first error, just modify the `WaitGroup` like so:

```go
ctx, cancel := context.WithCancel(context.Background())
wg := prim.WaitGroup{CancelOnErr: cancel}
...
```

We capture all errors using error wrapping. You can retrieve all the errors using the `errors.Unwrap` method. We only capture a `Context.Deadline` error if it is the
first error.

### Reuse resources and limit number of goroutines

The standard way in which we limit the goroutines to the number of CPUs we have, but it use a new goroutine each time:

```go
func main() {
    wg := sync.WaitGroup{}
    limit := make(chan struct{}, runtime.NumCPU())

    for i := 0; i < 100; i++ {
        i := i
        wg.Add(1)
        limit <-struct{}{}
        go func() { // <-New goroutine spawned each time, wasteful
            defer wg.Done()
            defer func(){<-limit}()

            fmt.Println(i)
        }()
    }

    wg.Wait()
}
```

Our way where we limit the number of goroutines to the number of CPUs we have and only spawn the goroutines once:

```go
func main() {
    p, _ := pooled.New(runtime.NumCPU()) // <-Reuse goroutines and limit number of goroutines.
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
        panic(err) // or whatever you want to do with the error
    }
}
```

# In constrast to `golang.org/x/errgroup` and `github.com/sourcegraph/conc`  

`golang.org/x/errgroup` differs in:

- It is the progenitor of these types of packages
- Doesn't contain any of these `slice` operations, though it can certainly mimic them
- Can do everything `conc/stream`, `conc#WaitGroup` and `conc/pool` can do, mostly it is a style difference

`sourcegraph/conc` provides similar generic concurrency operators.

- A single Pool type for many parallel operations
- A stream processor for concurrent operations
- A `WaitGroup` that simply handles `wg.Add(1)` and `defer wg.Done()` to reduce errors
- An `iter` package that handles operations on slices
- Support for catching panics

`prim` differs in:

- No direct support for streaming, this is handled by our `concurrency/pipelines` packages, though you could setup pipelines similar to how `errgroup` does it. I'd just recommend the more formal way in `concurrency/pipelines`
- A more advanced `WaitGroup` that encompases the uses of `conc#WaitGroup` and `conc/pool#Pool` types. Its kinda like the baby of `conc#WaitGroup` and `errgroup#Group`
- Support for `goroutine` reuse, rate limiting, etc.. using the `concurrency/goroutines` package (this is optional)
- Support for OpenTelemetry logging for advanced debugging
- Support for OpenTelemetry metrics for advanced monitoring
- Supports errors for goroutines, not panics. Supporting panic catching in goroutines is a bad practice. If you are catching panics in your own libaries, you should fix the panic if its not critical. If its in a third-party, you should catch it in the goroutine and return it as an error.