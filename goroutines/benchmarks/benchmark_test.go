package benchmarks

import (
	"context"
	"crypto/elliptic"
	"crypto/rand"
	"math/big"
	"runtime"
	"sync"
	"testing"

	"github.com/Jeffail/tunny"
	"github.com/johnsiilver/pools/goroutines/limited"
	"github.com/johnsiilver/pools/goroutines/pooled"
)

var num = 10000
var limit = runtime.NumCPU()

func BenchmarkPooled(b *testing.B) {
	b.ReportAllocs()

	p, err := pooled.New(limit)
	if err != nil {
		panic(err)
	}

	answer := make([]curveData, num)
	ctx := context.Background()

	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		for i := 0; i < num; i++ {
			i := i
			p.Submit(
				ctx,
				func(ctx context.Context) {
					answer[i] = curve(ctx)
				},
			)
		}
		p.Wait()
	}
	b.StopTimer()

	for _, a := range answer {
		if len(a.priv) == 0 {
			b.Fatalf("BenchmarkPool: didn't return a curve as expected")
		}
	}
	if len(answer) != num {
		b.Fatalf("BenchmarkPool: expected more answers")
	}
}

func BenchmarkPoolLimited(b *testing.B) {
	b.ReportAllocs()

	p, err := limited.New(limit)
	if err != nil {
		panic(err)
	}

	answer := make([]curveData, num)
	ctx := context.Background()

	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		for i := 0; i < num; i++ {
			i := i
			p.Submit(
				ctx,
				func(ctx context.Context) {
					answer[i] = curve(ctx)
				},
			)
		}
		p.Wait()
	}
	b.StopTimer()

	for _, a := range answer {
		if len(a.priv) == 0 {
			b.Fatalf("BenchmarkPool: didn't return a curve as expected")
		}
	}
	if len(answer) != num {
		b.Fatalf("BenchmarkPool: expected more answers")
	}
}

func BenchmarkStandard(b *testing.B) {
	b.ReportAllocs()

	limiter := make(chan struct{}, limit)
	answer := make([]curveData, num)
	ctx := context.Background()
	wg := sync.WaitGroup{}

	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		for i := 0; i < num; i++ {
			i := i
			wg.Add(1)
			limiter <- struct{}{}
			func(ctx context.Context) {
				defer func() { <-limiter }()
				defer wg.Done()
				answer[i] = curve(ctx)
			}(ctx)
		}
		wg.Wait()
	}
	b.StopTimer()

	for _, a := range answer {
		if len(a.priv) == 0 {
			b.Fatalf("BenchmarkPool: didn't return a curve as expected")
		}
	}
	if len(answer) != num {
		b.Fatalf("BenchmarkPool: expected more answers")
	}
}

func BenchmarkTunny(b *testing.B) {
	b.ReportAllocs()

	answer := make([]curveData, num)
	ctx := context.Background()

	pool := tunny.NewFunc(
		limit,
		func(payload interface{}) interface{} {
			i := payload.(int)
			answer[i] = curve(ctx)
			return nil
		},
	)

	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		for i := 0; i < num; i++ {
			i := i
			pool.ProcessCtx(ctx, i)
		}
	}
	b.StopTimer()

	for _, a := range answer {
		if len(a.priv) == 0 {
			b.Fatalf("BenchmarkPool: didn't return a curve as expected")
		}
	}
	if len(answer) != num {
		b.Fatalf("BenchmarkPool: expected more answers")
	}
}

type curveData struct {
	priv []byte
	x, y *big.Int
}

func curve(ctx context.Context) curveData {
	priv, x, y, err := elliptic.GenerateKey(elliptic.P384(), rand.Reader)
	if err != nil {
		panic(err)
	}
	return curveData{priv, x, y}
}
