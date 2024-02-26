// Package prim provides concurrency primatives that can be used to run concurrent
// operations in a safer manner than the stdlib with additional instrumentation
// and reuse options to prevent costly setup and teardown of goroutines (they are
// cheap, but they are not free).
package prim
