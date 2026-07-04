package foreach

import (
	"testing"

	"github.com/gostdlib/base/context"
	"github.com/gostdlib/base/retry/exponential"
)

// benchmarkItem measures Item's per-pair overhead over size values with a minimal ItemFunc. options
// lets variants measure the checkpoint, ordered-engine and held-bound costs.
func benchmarkItem(b *testing.B, size int, options ...Option) {
	b.Helper()

	ctx := b.Context()

	vals := ints(size)
	seq := seqOf(vals...)

	fn := func(_ context.Context, _ int, v int) (int, error) {
		return v, nil
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		n := 0
		for _, resp := range Item(ctx, seq, fn, options...) {
			if resp.Err != nil {
				b.Fatalf("benchmarkItem: got unexpected Response.Err: %v", resp.Err)
			}
			n++
		}
		if n != size {
			b.Fatalf("benchmarkItem: got %d responses, want %d", n, size)
		}
	}
	b.ReportMetric(float64(b.Elapsed().Nanoseconds())/(float64(b.N)*float64(size)), "ns/item")
}

func BenchmarkItem100(b *testing.B) {
	benchmarkItem(b, 100)
}

func BenchmarkItem10000(b *testing.B) {
	benchmarkItem(b, 10000)
}

// BenchmarkItemGate10000 measures the per-dispatch cost of the WithGate checkpoint when no ItemFunc
// ever fails, so the gate never closes.
func BenchmarkItemGate10000(b *testing.B) {
	benchmarkItem(b, 10000, WithGate(exponential.Must(exponential.New(exponential.WithTesting()))))
}

// BenchmarkItemOrdered10000 measures the input-order fan-in through the order engine.
func BenchmarkItemOrdered10000(b *testing.B) {
	benchmarkItem(b, 10000, WithOrdered())
}

// BenchmarkItemOrderedMaxHeld10000 is the ordered path with a tight held bound, so it also pays the
// waitBelow checkpoint and the yield signals.
func BenchmarkItemOrderedMaxHeld10000(b *testing.B) {
	benchmarkItem(b, 10000, WithOrdered(), WithMaxHeld(64))
}

// BenchmarkSerial10000 is the single-goroutine baseline the package doc compares Item against for
// cheap operations.
func BenchmarkSerial10000(b *testing.B) {
	ctx := b.Context()

	vals := ints(10000)
	seq := seqOf(vals...)

	fn := func(_ context.Context, _ int, v int) (int, error) {
		return v, nil
	}

	// A plain int keeps the per-item work live without an atomic Add dominating the baseline this
	// benchmark exists to measure.
	count := 0

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for k, v := range seq {
			if _, err := fn(ctx, k, v); err != nil {
				b.Fatalf("BenchmarkSerial10000: fn had unexpected error: %v", err)
			}
			count++
		}
	}
	if want := b.N * len(vals); count != want {
		b.Fatalf("BenchmarkSerial10000: processed %d items, want %d", count, want)
	}
	b.ReportMetric(float64(b.Elapsed().Nanoseconds())/(float64(b.N)*float64(len(vals))), "ns/item")
}
