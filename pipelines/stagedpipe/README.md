# StagedPipe - The Concurrent and Parallel Pipeline Framework

[![GoDoc](https://godoc.org/github.com/gostdlib/concurrency/pipelines/stagedpipe?status.svg)](https://pkg.go.dev/github.com/gostdlib/concurrency/pipelines/stagedpipe)
[![Go Report Card](https://goreportcard.com/badge/github.com/gostdlib/concurrency)](https://goreportcard.com/report/github.com/gostdlib/concurrency)

## Introduction

_Note:_ Any reference to `github.com/johnsiilver/pipelines` should be substituted for this package. That is the original place this package was developed.

Pipelining in Go can be hard to debug and can end up a little messy. Stagepipe combines semantics of Go state machines and Go pipelining to give you an efficient and easy to use pipeline framework.

This package supports:

- A concurrent pipeline that can be run in parallel
- Multiple users can use a single set of pipelines, not a pipeline setup per user
- No need to run your own goroutines, simply scale up parallelism
- Generic, so it avoids runtime type checks of data objects
- Data can be on the stack or the heap
- Retrieve Stats on how the pipeline is running
- Route requests to different stages depending on data
- Allow routing to route back to a stage or setup the pipeline as a Directed Acyllic Graph (DAG) to avoid loops via an option
- Defaults to out of order processing, but can do in-order processing via an option
- Cancelation of a set of requests on an error or ignore errors

Here is a brief introduction to standard Go pipelining, standard Go state machines and a hello world for the stagedpipe framework:

[![Introduction Video](https://i.vimeocdn.com/video/1745567750-9b634f58614db8361e28cac2a9f77cca99923c16afd05ce112cbf506a5722656-d_640?f=webp)](https://player.vimeo.com/video/879175351?badge=0&autopause=0&quality_selector=1&player_id=0&app_id=58479)

Chapters Links:

- [Introduction](https://vimeo.com//879175351)
- [Basic Go Pipelines](https://vimeo.com/879175351#t=0m16s)
- [Basic Go State Machines](https://vimeo.com/879175351#t=2m22s)
- [Why Use StagedPipe](https://vimeo.com/879175351#t=5m28s)
- [StagedPipe Hello World](https://vimeo.com/879175351#t=9m28s)

## Just jump in

This is for those of you that simply want to hit the ground running and aren't interested in a video introduction.

To help get you started, we have a `tools/` directory containing the `stagedpipe-cli`. This allows you to generate all the structure that is required to implement a pipeline.

Installation can be done with: `go install github.com/gostdlib/concurrency/pipelines/stagedpipe/tools/stagedpipe-cli@latest` from that directory.

Simply enter into your new package's directory, and type: `stagedpipe-cli -m -p "[package root]/sm"` to get:

```
├──myPipeline
        ├── main.go
        └──sm
            ├── data.go
            └── sm.go
```

Run `go mod init <path>`, `go mod tidy` and `go fmt ./...`, to get a running program:

```
├──myPipeline
        ├── go.mod
        ├── go.sum
        ├── main.go
        └──sm
            ├── data.go
            └── sm.go
```

Type `go run .` to run the basic pipeline that you can change to fit your needs.

See the comments in the file to see how to modify and extend the example pipeline for your needs.

## Basics and Best Practices

Here are the basics, if you've built off of the stagedpipe-cli skeleton code:

- Each method in your state machine intended as a `Stage` must implement the `stagedpipe.Stage` type
- `Stage` methods must be public, otherwise you will not get the expected concurrecy
- You must always drain a `RequestGroup`, even if you cancel it. This is the one way to get your pipeline stuck
- Be careful not to route in an infinite loop

Here are some best practices:

- Use bulk objects in your pipeline. These are much more efficient and easier to manage
- Do not use goroutines inside your stages. Instead, dial up the parallelism
- For pipelines with lots of stages, parallelism could start at 1. Otherwise, runtime.NumCPU() is a good starting point

Something to watch out for:

- If your data object uses the stack, remember the stack use is not visible in pprof memory traces \* This can be a big gotcha if you get `OOM` messages and are expecting to see it in the graphs
- Dialing up parallelism can make things slower if you are bound by disk (like talking to the filesystem or a database) or network IO limited \* This works best when doing everything is in memory or the data store can horizontally scale to keep up with demand

## Building an ETL Pipeline from Scratch

Ardan Labs has a great [tutorial](https://www.ardanlabs.com/blog/2021/09/extract-transform-load-in-go.html) on building a basic ETL pipeline in Go. With their permission, I have re-written their example using the stagedpipe framework.

This lesson takes about 30 minutes and I build the pipeline from scratch without using the `stagedpipe-cli` tool, so it takes longer that normal. I use a local postgres server to store data in, so if you want to follow this you will need one too.

Using the `stagedpipe-cli` removes a lot of the boilerplate here, but this is a good opportunity to explain what each of the boilerplate types and functions do.

You can download the dataset uses in this example [here](https://data.boston.gov/dataset/food-establishment-inspections)

Code for our modified version of Ardan Labs code and our version can be found [here](https://github.com/johnsiilver/concurrency/pipelines/tree/main/stagedpipe/examples/etl/bostonFoodViolations)

[![ETL Video](https://i.vimeocdn.com/video/1745612616-fbd27ad348e812476577b0d5866b3787dd1ce8821f110c977bfe14022a6a0b26-d_640?f=webp)](https://player.vimeo.com/video/879203973?h=24035c0a82)

## The Common Pipeline Pattern

Golang standard Pipelines work using the concurrency model layed out in Rob Pike's talk on [Concurrency in not Parallelism](https://www.youtube.com/watch?v=oV9rvDllKEg).

In this version, each pipeline has stages, each stage runs in parallel, and channels pass data from one stage to the next. In a single pipeline with X stages, you can have X stages running. This is called a concurrent pipeline for the purposes of this doc.

You can run multiple pipelines in parallel. So if you run Y pipelines of X stages, you can have X \* Y stages running at the same time.

```
        -> stage0 -> stage1 -> stage2 -> stage3 ->
        -> stage0 -> stage1 -> stage2 -> stage3 ->
in ->->                                            ->-> out
        -> stage0 -> stage1 -> stage2 -> stage3 ->
        -> stage0 -> stage1 -> stage2 -> stage3 ->
```

This looks similar to a standard fan in/out model, with the exception that each stage here is running concurrently with the other stages. In the above scenario, 16 workers are running at various stages and we have 4 parallel jobs.

Stages pass data through the pipeline through a series of channels. Pike also offers another concurrent model where you pass functions to works via a channel, which is great for worker dispatch.

Note: Nothing here is a criticism of Pike or his talks/methods/etc. In a more complex pipeline, I'm sure he would alter this method to control the complexity. If anything, over the years I have learned so much by taking ideas from him and hammering them into successful packages. This package uses two of his ideas together to make something I find easy to use and fast.

### The First Problem

In simple pipelines with few stages, writing a pipeline is pretty easy. In complex pipelines, you end up with a bunch of channels and go routines. When you add to the pipeline after you haven't looked at the code in a few months, you forget to call `make(chan X)` in your constructor, which causes a deadlock. You fix that, but you forgot to call `go p.stage()`, which caused another deadlock. This tends to make the code brittle.

There are certainly other methods to deal with this, but they usually lack the beauty of just looking through the stages in a single file that makes it really easy to read.

### The Second Problem

The standard type of pipelining also works great in two scenarios:

- Everything that goes through the Pipeline is related
- Each request in the Pipeline is a promise that responds to a single request

In the first scenario, no matter how many things enter the pipeline, you know know they are all related to a single call. When input ends, you shut down the input channel and the output channel shuts down when a `sync.WaitGroup` says all pipelines are done.

In the second scenario, you can keep open the pipeline for the life of the program as requests come in. Each request is a promise, which once it comes out the exit channel is sent on the included promise channel. This is costly because you have to create a channel for every request, but it also keeps open the pipelines for future use.

But what if you want to keep your pipelines running and have multiple ingress streams that each need to be routed to their individual output streams? The pipeline models above break down, either requiring each stream to have its own pipelines (which wastes resources), bulk requests that eat a lot of memory, or other unsavory methods.

### This Solution

I hate to say "the solution", because there are many ways you can solve this. But I was looking to create a framework that was elegant in how it handled this.

What I've done here is combine a state machine with a pipeline. For each stage in the state machine, we spin off 1 goroutine that is running the state machine. We receive input on a single channel and send output on a single channel. The input gets sent to any state machine that is available to process and each state machine sends to the `out` channel.

```
Four Pipelines processing
        -> Pipeline ->
        -> Pipeline ->
in ->->                ->-> out
        -> Pipeline ->
        -> Pipeline ->

Each Pipeline looks like:
        -> stages ->
        -> stages ->
in ->->               ->-> out
        -> stages ->
        -> stages ->
```

You can than concurrently run multiple pipelines. This differs from the standard model in that a full pipeline might not have all stages running, but it will have the same number of stages running. Mathmatically, we still end up in a X \* Y number of concurrent actions.

`Stage`s are constructed inside a type that implements our `StateMachine` interface. Any method on that object that is `Public` and implements `Stage` becomes a valid stage to be run. You pass the `StateMachine` to our `New()` constructor with the number of parallel pipelines (all running concurrently) that you wish to run. A good starting number is either 1 or `runtime.NumCPU()`. The more stages you have or the more blocing on IO you have, the more 1 is a great starting point.

Accessing the pipeline happens by creating a `RequestGroup`. You can simply stream values in and out of the pipeline separate from other `RequestGroup`s using the the `Submit()` method and `Out` channel.
