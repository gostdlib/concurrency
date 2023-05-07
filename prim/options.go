package prim

import (
	"fmt"

	"github.com/gostdlib/concurrency/goroutines"
	"github.com/johnsiilver/calloptions"
)

// WithPool sets the goroutines.Pool options for the goroutines.Pool used in
// a function call. This can be used as a:
// - SliceOption
// - ResultSliceOption
// - MapOption
// - ResultMapOption
// - ChanOption
func WithPoolOptions(pool goroutines.Pool, options ...goroutines.SubmitOption) interface {
	SliceOption
	ResultSliceOption
	MapOption
	ResultMapOption
	ChanOption
	calloptions.CallOption
} {
	return struct {
		SliceOption
		ResultSliceOption
		MapOption
		ResultMapOption
		ChanOption
		calloptions.CallOption
	}{
		CallOption: calloptions.New(
			func(a any) error {
				t, ok := a.(*sliceOptions)
				if !ok {
					return fmt.Errorf("WithPoolOptions can only be used with SliceOption, ResultSlice, MapOption, ResultMap, ChanOption")
				}
				t.poolOptions = options
				return nil
			},
		),
	}
}
