package prim

import (
	"context"
	"fmt"
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
