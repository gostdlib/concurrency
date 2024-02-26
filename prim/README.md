# `Prim`: Parallel primatives for production applications

The packages in `prim` is your one stop shop for advanced parallel primatives to create robust and easy to diagnose applications. These are parallel and not concurrent by definitions layed out in the famous Go talk by Rob Pike: https://go.dev/blog/waza-talk . We provide parallel and concurrent pipelines in our https://github.com/gostdlib/concurrency/pipelines set of packages.

```sh
go get github.com/gostdlib/concurrency/
```

# A quick look

- Use [`prim/wait`](https://pkg.go.dev/github.com/gostdlib/concurrency/prim/wait) if you want:
    - A safer version of `sync.WaitGroup` for parallel jobs
    - A parallel job runner that collects errors after all jobs complete
    - A parallel job runner that CAN stops processing on the first error
    - A parallel job runner that CAN be `Context` cancelled
    - Reuse and limiting of goroutines by supplying a [`goroutines.Pool`](https://pkg.go.dev/github.com/gostdlib/concurrency/goroutines#Pool)
    - Integration with logging of your choice, including support for OpenTelemetry spans
- Use [`prim/slices`](https://pkg.go.dev/github.com/gostdlib/concurrency/prim/slices) if you want:
    - To access elements in a slice in parallel to perform some operation
    - Support for processing errors
    - Support for OpenTelemetry spans
- Use [`prim/chans`](https://pkg.go.dev/github.com/gostdlib/concurrency/prim/chans) if you want:
    - To access elements from a receive channel in parallel to perform some operation
    - Support for processing erors
    - Support for OpenTelemetry spans
