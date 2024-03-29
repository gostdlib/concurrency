<img src="./gears.jpg"  width="424" height="280">

# Concurrency
Packages for handling Go concurrency.

[![Go Reference](https://pkg.go.dev/badge/github.com/gostdlib/concurrency/concurrency.svg)](https://pkg.go.dev/github.com/gostdlib/concurrency/)
[![Go Report Card](https://goreportcard.com/badge/github.com/gostdlib/concurrency)](https://goreportcard.com/report/github.com/gostdlib/concurrency)

# Introduction

The packages contained within this repository provide safer primitives for doing concurrent and parallel operations. 

In addition, these packages integrate with Open Telemetry(OTEL) and provide execution traces to give insight into how your software is operating. Along with exported metrics within these packages the insights can then be used to provide deeper profiling of your software.  

If used with `gostdlib/foundation/telemetry/slog`, OTEL traces will also contain your logging messages when using the `slog` or `log` packages.

# A quick look
- `gouroutines/` : A set of packages for safer goroutine spawning and goroutine reuse
    - Use [`goroutines/pooled`](https://pkg.go.dev/github.com/gostdlib/concurrency/goroutines/pooled) if you want:
        - Reuse of goroutines instead of spawning new ones
        - The ability to limit the number of goroutines
        - The ability to bypass the goroutine limit for some tasks
    - Use [`goroutines/limited`](https://pkg.go.dev/github.com/gostdlib/concurrency/goroutines/limited) if you want:
        - A safer way to spawn goroutines with limits
        - The ability to limit the number of goroutines
        - The ability to bypass the goroutine limit for some tasks
- `pipelines/` : A set of packages for creating streaming pipelines
    - Use [`pipelines/stagedpipe`](https://pkg.go.dev/github.com/gostdlib/concurrency/pipelines/stagedpipe) if you want:
        - A safer way to build streaming pipelines
	- Multiple independent input streams into the same pipeline for processing
        - Want concurrency and parallel pipelines
        - Use of stack for stream data or allocating on the heap
        - Routing to different processing based on data
- `prim/` : A set of packages for safer concurrency primatives built on goroutine pooling
    - Use [`prim/wait`](https://pkg.go.dev/github.com/gostdlib/concurrency/prim/wait) if you want:
        - A safer version of `sync.WaitGroup` for parallel jobs
        - A parallel job runner that collects errors after all jobs complete
        - A parallel job runner that CAN stops processing on the first error
        - A parallel job runner that CAN be `Context` cancelled
        - Reuse and limiting of goroutines by supplying a [`goroutines.Pool`](https://pkg.go.dev/github.com/gostdlib/concurrency/goroutines/#Pool)
        - Support for OpenTelemetry spans
    - Use [`prim/slices`](https://pkg.go.dev/github.com/gostdlib/concurrency/prim/slices) if you want:
        - To access elements in a slice in parallel to perform some operation
        - Support for processing errors
        - Support for OpenTelemetry spans
    - Use [`prim/chans`](https://pkg.go.dev/github.com/gostdlib/concurrency/prim/chans) if you want:
        - To parallel process a chan of values
        - Support for processing errors
        - Support for OpenTelemetry spans
