package foreach_test

import (
	"fmt"
	"strconv"

	"github.com/gostdlib/base/context"
	"github.com/gostdlib/concurrency/patterns/stream"
	"github.com/gostdlib/concurrency/patterns/stream/foreach"
)

// ExampleOrder shows foreach acting as an order-retaining fan-out/fan-in: values are processed in
// parallel by Item and the results stream out of All in input order while processing is still running.
func ExampleOrder() {
	ctx := context.Background()

	words := []string{"10", "11", "12", "13", "14"}

	ord, err := foreach.New[int](ctx)
	if err != nil {
		fmt.Println(err)
		return
	}

	// fn runs in parallel for every element. Whatever order the calls finish in, Adding under the input
	// key means All yields the results in input order.
	fn := func(ctx context.Context, k int, v string) error {
		n, err := strconv.Atoi(v)
		if err != nil {
			return err
		}
		ord.Add(ctx, k, n*10)
		return nil
	}

	// Drive Item in the background so results can be consumed while processing runs. Close signals All
	// that no more values are coming.
	context.Pool(ctx).Submit(ctx, func() {
		defer ord.Close()
		if err := foreach.Item(ctx, stream.Slice(words), fn); err != nil {
			fmt.Println(err)
		}
	})

	for k, v := range ord.All(ctx) {
		fmt.Println(k, v)
	}

	// Output:
	// 0 100
	// 1 110
	// 2 120
	// 3 130
	// 4 140
}
