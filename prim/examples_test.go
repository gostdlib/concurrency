package prim

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"

	"github.com/gostdlib/concurrency/goroutines/pooled"
)

func ExampleSlice() {
	// Take every integer in the slice and add 1 to it.
	m := func(ctx context.Context, i int) (int, error) {
		return i + 1, nil
	}

	s := []int{1, 2, 3, 4, 5}

	Slice(context.Background(), s, m, nil)
	fmt.Println(s)
	// Output: [2 3 4 5 6]
}

func ExampleResultSlice() {
	// Translates the slice integers into a slice of strings using the toWords map.
	toWords := map[int]string{
		1: "Hello",
		2: "I",
		3: "am",
		4: "Macintosh",
	}
	m := func(ctx context.Context, i int) (string, error) {
		return toWords[i], nil
	}

	s := []int{1, 2, 3, 4, 5}
	r, _ := ResultSlice(context.Background(), s, m, nil)
	fmt.Println(r)
	// Output: [Hello I am Macintosh ]
}

// JustErrors illustrates the use of WaitGroup in place of a sync.WaitGroup to
// simplify goroutine counting and error handling. This example is derived from
// the sync.WaitGroup example at https://golang.org/pkg/sync/#example_WaitGroup.
// This example is derived from errgroup.Group from golang.org/x/sync/errgroup.
func ExampleWaitGroup_just_errors() {
	ctx := context.Background()
	wg := WaitGroup{}

	var urls = []string{
		"http://www.golang.org/",
		"http://www.google.com/",
		"http://www.somestupidname.com/",
	}
	for _, url := range urls {
		// Launch a goroutine to fetch the URL.
		url := url // https://golang.org/doc/faq#closures_and_goroutines
		wg.Go(ctx, func(ctx context.Context) error {
			// Fetch the URL.
			resp, err := http.Get(url)
			if err == nil {
				resp.Body.Close()
			}
			return err
		})
	}

	// Wait for all HTTP fetches to complete.
	if err := wg.Wait(ctx); err != nil {
		fmt.Println("Successfully fetched all URLs.")
	}
}

var (
	Web   = fakeSearch("web")
	Image = fakeSearch("image")
	Video = fakeSearch("video")
)

type Result string
type Search func(ctx context.Context, query string) (Result, error)

func fakeSearch(kind string) Search {
	return func(_ context.Context, query string) (Result, error) {
		return Result(fmt.Sprintf("%s result for %q", kind, query)), nil
	}
}

// Parallel illustrates the use of a Group for synchronizing a simple parallel
// task: the "Google Search 2.0" function from
// https://talks.golang.org/2012/concurrency.slide#46, augmented with a Context
// and error-handling. // This example is derived from errgroup.Group from golang.org/x/sync/errgroup.
func ExampleWaitGroup_parallel() {
	Google := func(ctx context.Context, query string) ([]Result, error) {
		wg := WaitGroup{}

		searches := []Search{Web, Image, Video}
		results := make([]Result, len(searches))
		for i, search := range searches {
			i, search := i, search // https://golang.org/doc/faq#closures_and_goroutines
			wg.Go(ctx, func(context.Context) error {
				result, err := search(ctx, query)
				if err == nil {
					results[i] = result
				}
				return err
			})
		}
		if err := wg.Wait(ctx); err != nil {
			return nil, err
		}
		return results, nil
	}

	results, err := Google(context.Background(), "golang")
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return
	}
	for _, result := range results {
		fmt.Println(result)
	}

	// Output:
	// web result for "golang"
	// image result for "golang"
	// video result for "golang"
}

// CancelOnErr illustrates how to use WaitGroup to do parallel tasks and
// cancel all remaining tasks if a single task has an error.
func ExampleWaitGroup_cancel_on_err() {
	ctx, cancel := context.WithCancel(context.Background())
	p, _ := pooled.New(10)

	wg := WaitGroup{Pool: p, CancelOnErr: cancel}

	for i := 0; i < 10000; i++ {
		i := i

		wg.Go(
			ctx,
			func(ctx context.Context) error {
				if i == 100 {
					return errors.New("error")
				}
				return nil
			},
		)
	}

	if err := wg.Wait(ctx); err != nil {
		fmt.Println(err)
	}

	// Output: error
}
