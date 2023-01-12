# Pools Goroutines

[![GoDoc](https://godoc.org/github.com/johnsiilver/pools/goroutines?status.svg)](https://pkg.go.dev/github.com/johnsiilver/pools/goroutines)
[![Go Report Card](https://goreportcard.com/badge/github.com/johnsiilver/pools)](https://goreportcard.com/report/github.com/johnsiilver/pools)

## Introduction

The packages contained here provide basic pooling for goroutines. These pools provide the ability to gather statistics, limit the number of goroutines in use and allow reuse of goroutines to lower allocations.

These pools are designed to avoid `any` and `sync.Mutex` to prioritize speed. 

There are two pool implementations:
* limited
* pooled

`limited` simply uses a limiter to prevent more than `X` goroutines from running at any given time. There is still a separate goroutine for each request. In some cases this can be the best choice while costing extra allocs.

`pooled` has a set of goroutines running that are reused. This cuts down allocations in the long run.

## Benchmarks

```
BenchmarkPooled-10                     6         184724028 ns/op         7140777 B/op     140006 allocs/op
BenchmarkPoolLimited-10                6         184942646 ns/op         8118250 B/op     152738 allocs/op
BenchmarkStandard-10                   1        1395970416 ns/op         6640000 B/op     120000 allocs/op
BenchmarkTunny-10                      1        1453848666 ns/op         6880048 B/op     139774 allocs/op
PASS
ok      github.com/johnsiilver/pools/goroutines/benchmarks      6.475s
```

### Test Description

The benchmarks all use runtime.NumCPUs() as the pool limit. Each goroutine calculates elliptical curve private keys several thousands times.

* BenchmarkPooled is the `pooled` package.
* BenchmarkPoolLimited is the `limited` package.
* BenchmarkStandard is simply spinning off goroutines with a limiter and waitgroup.
* BenchmarkTunny uses the populare `github.com/Jeffail/tunny` package.

*Full Disclosure*

Currently, I don't trust this benchmark. BenchmarkStandard doesn't look like I expect it to look.  I expected similar results with BenchmarkPoolLimited, but that isn't the case.  I've looked at it several times and I'm not seeing why.  I haven't gotten down to pprofing it yet. Until I understand it, I am not confident in the speed differences.  

I do know that all data comes out as expected, so I do belive the numbers for BenchmarkPooled and BenchmarkPoolLimited.
