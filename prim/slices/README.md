# `Slices`: Parallel operations on slices

[![Go Reference](https://pkg.go.dev/badge/github.com/gostdlib/concurrency/concurrency.svg)](https://pkg.go.dev/github.com/gostdlib/concurrency/prim/slices)

The `slices` package provides concurrent operations on slice types. This is for large slices that have complex transformations. For smaller use cases, a non-concurrent `for loop` will probably suffice.

```sh
go get github.com/gostdlib/concurrency/
```

# A quick look

- Access elements in a slice in parallel to perform some operation
- Support for processing errors
- Support for OpenTelemetry spans

# Package purpose

- Reduce programatic errors (goroutine leaks, deadlocks, ...)
- Reuse resources (goroutines)
- Integration with OpenTelemetry `span`s by default
