package main

import (
	"context"
	"log"
	"net/http"
	_ "net/http/pprof"
	"runtime"
	"runtime/debug"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gostdlib/concurrency/pipelines/stagedpipe"
)

func main() {
	runtime.SetBlockProfileRate(1)
	runtime.SetMutexProfileFraction(1)

	go func() {
		log.Println(http.ListenAndServe("localhost:6060", nil))
	}()

	sm := NewSM()

	p, err := stagedpipe.New(
		"etoe",
		1,
		stagedpipe.StateMachine[Data](sm),
	)
	if err != nil {
		panic(err)
	}

	g0 := p.NewRequestGroup()

	done := sync.WaitGroup{}
	done.Add(1)
	var n atomic.Uint64
	now := time.Now()
	go func() {
		defer done.Done()
		defer func() {
			log.Println("total time: ", time.Since(now))
		}()
		// For this exercise we need to drain the Out channel so things continue
		// to process, but we can just wait for the RecordSet as a whole to finish
		// instead of doing things with each Request.
		ch := g0.Out()
		t := time.NewTicker(10 * time.Second)
		for {
			select {
			case _, ok := <-ch:
				if !ok {
					return
				}
				if v := n.Add(1); v%100 == 0 {
					log.Println("processed: ", v*1000)
				}
			case <-t.C:
				log.Println("nothing came out after 10 seconds, pipeline is probably stuck")
			}
			t.Reset(10 * time.Second)
		}
	}()

	// This tries to recapture memory every minute.
	stop := make(chan chan struct{}, 1)
	go func() {
		for range time.Tick(1 * time.Minute) {
			ch := make(chan struct{})
			log.Println("Sending stop")
			stop <- ch
			runtime.GC()
			time.Sleep(5 * time.Second)
			debug.FreeOSMemory()
			log.Println("Sending go")
			ch <- struct{}{}
		}
	}()

	// const _100Million = 100000000
	const _1K = 1000 // Which does 1000 * 1000(each Request has 1000 entries)
	ctx := context.Background()

	for i := 0; i < _1K; i++ {
		select {
		case GO := <-stop:
			log.Println("Received stop")
			<-GO
			log.Println("Received go")
		default:
			if err := g0.Submit(NewRequest(ctx)); err != nil {
				panic(err)
			}
		}
	}
	log.Println("closing g0")
	g0.Close()
	done.Wait()
}
