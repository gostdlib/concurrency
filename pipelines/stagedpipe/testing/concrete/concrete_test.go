package concrete

import (
	"context"
	"math/rand"
	"testing"
	"time"
)

func BenchmarkPipeline(b *testing.B) {
	b.ReportAllocs()

	ctx := context.Background()

	requests := []Request{}
	for i := 0; i < 1000; i++ {
		d := []int{rand.Int(), rand.Int(), rand.Int(), 0, 0}
		requests = append(requests, Request{Ctx: ctx, Data: Data{Data: d}})
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		p := New(&sm{})
		time.Sleep(1 * time.Millisecond) // Get goroutines to start
		b.StartTimer()
		go func() {
			defer close(p.In)
			for _, r := range requests {
				p.In <- r
			}
		}()

		for r := range p.Out {
			wantSum := r.Data.Data[0] + r.Data.Data[1] + r.Data.Data[2]
			wantAvg := r.Data.Data[0] + r.Data.Data[1] + r.Data.Data[2]/3

			if wantSum != r.Data.Data[3] {
				panic("sum was bad")
			}
			if wantAvg != r.Data.Data[4] {
				panic("avg was bad")
			}
		}
	}
}
