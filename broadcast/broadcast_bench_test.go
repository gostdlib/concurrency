package broadcast

import (
	"iter"
	"testing"

	"github.com/gostdlib/base/concurrency/sync"
	"github.com/gostdlib/base/context"
)

func BenchmarkSend(b *testing.B) {
	tests := []struct {
		name        string
		subscribers int
	}{
		{
			name:        "no subscribers",
			subscribers: 0,
		},
		{
			name:        "one subscriber",
			subscribers: 1,
		},
		{
			name:        "ten subscribers",
			subscribers: 10,
		},
	}

	for _, test := range tests {
		b.Run(test.name, func(b *testing.B) {
			ctx := b.Context()

			v := &Value[int]{}
			g := sync.Group{}
			for i := 0; i < test.subscribers; i++ {
				seq := v.Subscribe(ctx)
				g.Go(ctx, func(ctx context.Context) error {
					for range seq {
					}
					return nil
				})
			}

			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				v.Send(ctx, i)
			}
			b.StopTimer()

			v.Close(ctx)
			g.Wait(ctx)
		})
	}
}

func BenchmarkSendReceive(b *testing.B) {
	ctx := b.Context()

	v := &Value[int]{}
	next, stop := iter.Pull(v.Subscribe(ctx))
	defer stop()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		v.Send(ctx, i)
		next()
	}
}

func BenchmarkSubscribe(b *testing.B) {
	ctx := b.Context()

	// The Value is closed so that ranging a subscription returns immediately. A subscription that is never
	// ranged over never gives its holder slot back, which would leave every Send() on this Value storing
	// values for a subscriber that does not exist.
	v := &Value[int]{}
	v.Close(ctx)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for range v.Subscribe(ctx) {
		}
	}
}
