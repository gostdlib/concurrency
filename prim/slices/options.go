package slices

import (
	"fmt"

	"github.com/gostdlib/concurrency/goroutines"
	"github.com/johnsiilver/calloptions"
)

// WithStopOnErr causes the operation to stop if an error occurs. Since operations are parallel,
// this may not stop all operations. This can be used as a:
// - SliceOption
func WithStopOnErr() interface {
	SliceOption
	calloptions.CallOption
} {
	return struct {
		SliceOption
		calloptions.CallOption
	}{
		CallOption: calloptions.New(
			func(a any) error {
				switch t := a.(type) {
				case *sliceOptions:
					t.stopOnErr = true
					return nil
				}
				return fmt.Errorf("WithStopOnErr can only be used with SliceOption")
			},
		),
	}
}

// WithPool sets a goroutines.Pool and its submit options used in
// a function call. This can be used as a:
// - SliceOption
// - MapOption
// - ResultMapOption
// - ChanOption
func WithPoolOptions(pool goroutines.Pool, options ...goroutines.SubmitOption) interface {
	SliceOption
	calloptions.CallOption
} {
	return struct {
		SliceOption
		calloptions.CallOption
	}{
		CallOption: calloptions.New(
			func(a any) error {
				switch t := a.(type) {
				case *sliceOptions:
					t.poolOptions = options
					return nil
				}
				return fmt.Errorf("WithPoolOptions can only be used with SliceOption, MapOption, ResultMap, ChanOption")
			},
		),
	}
}
