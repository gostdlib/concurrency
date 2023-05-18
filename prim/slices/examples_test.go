package slices

import (
	"context"
	"fmt"
)

// ExampleAccess_modify_slice_entries shows how to modify a slice's values in parrallel.
// In this example we take a slice of integers and add 1 to each value. You can make this
// more complex with checks and other operations on the values.
func ExampleAccess_modify_slice_entries() {
	// Take every integer in the slice and add 1 to it.
	m := func(ctx context.Context, i int, v int, m Modifier[int]) error {
		m(v + 1)
		return nil
	}

	s := []int{1, 2, 3, 4, 5}

	Access(context.Background(), s, m)
	fmt.Println(s)
	// Output: [2 3 4 5 6]
}

// ExampleAccess_create_new_slice shows how to create a new slice from an existing slice's values.
// In this example we take a slice of integers and translate them into a slice of strings.
// If we made the new value with an append(), we would need to use a mutex to protect the
// append() operation.
func ExampleAccess_create_new_slice() {
	// Translates the slice integers into a slice of strings using the toWords map.
	toWords := map[int]string{
		1: "Hello",
		2: "I",
		3: "am",
		4: "Macintosh",
	}
	s := []int{1, 2, 3, 4, 5}

	got := make([]string, len(s))

	a := func(ctx context.Context, i int, v int, m Modifier[int]) error {
		got[i] = toWords[v]
		return nil
	}

	Access(context.Background(), s, a)
	fmt.Println(got)
	// Output: [Hello I am Macintosh ]
}
