<img src="./gears.jpg"  width="424" height="280">

# Concurrency
Packages for handling Go concurrency.

[![Go Reference](https://pkg.go.dev/badge/github.com/gostdlib/concurrency/concurrency.svg)](https://pkg.go.dev/github.com/gostdlib/concurrency/)
[![Go Report Card](https://goreportcard.com/badge/github.com/gostdlib/concurrency)](https://goreportcard.com/report/github.com/gostdlib/concurrency)

# Introduction

The packages contained within this repository provide safer patterns for doing concurrent and parallel operations. They are built on top of [`github.com/gostdlib/base`](https://pkg.go.dev/github.com/gostdlib/base), using the worker pool attached to the `Context` (`context.Pool()`) and `base/concurrency/sync` primitives such as `sync.Group` instead of raw goroutines.

In addition, these packages integrate with OpenTelemetry (OTEL) and provide execution traces to give insight into how your software is operating. Along with exported metrics within these packages, the insights can then be used to provide deeper profiling of your software.

> **Note:** The `goroutines/` and `prim/` packages that previously lived here have been removed. Goroutine pooling, limiting and reuse now live in [`base/concurrency`](https://pkg.go.dev/github.com/gostdlib/base/concurrency) (`context.Pool()`, `sync.Group`, `worker.Limited`), and the parallel slice/chan helpers are replaced by the `patterns/` packages below.

# A quick look
- `patterns/` : A set of packages providing safer versions of common concurrency patterns
    - Use [`patterns/feeder`](https://pkg.go.dev/github.com/gostdlib/concurrency/patterns/feeder) if you want:
        - To feed a pipeline of key/value operations (add, delete, ...) into a guarded value such as a mutex-protected map
        - Side effects (like database writes) that happen atomically with the in-memory change via accept functions
        - Built-in exponential backoff retries, with `ErrPermanent` to stop retrying
    - Use [`patterns/stream`](https://pkg.go.dev/github.com/gostdlib/concurrency/patterns/stream) and [`patterns/stream/foreach`](https://pkg.go.dev/github.com/gostdlib/concurrency/patterns/stream/foreach) if you want:
        - A parallel `for range` over any `iter.Seq2` — running a side-effecting operation on every key/value pair
        - Adapters that bridge channels, slices, maps and `iter.Seq` into an `iter.Seq2` (`stream.Chan`, `stream.Slice`, `stream.Map`, `stream.Seq`)
        - Deadlock-free fan-out/fan-in: pair `foreach.Item` with `foreach.Order` to process in parallel and stream the results back out in input order
        - Errors collected across the run, or cancellation on the first error with `WithStopOnErr`
        - Support for OpenTelemetry spans
- `pipelines/` : A set of packages for creating streaming pipelines
    - Use [`pipelines/stagedpipe`](https://pkg.go.dev/github.com/gostdlib/concurrency/pipelines/stagedpipe) if you want:
        - A safer way to build streaming pipelines
        - Multiple independent input streams into the same pipeline for processing
        - Want concurrency and parallel pipelines
        - Use of stack for stream data or allocating on the heap
        - Routing to different processing based on data
