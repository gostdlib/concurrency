package main

import (
	"context"
	"crypto/rand"
	"fmt"
	"log"

	"github.com/google/uuid"
	"github.com/gostdlib/concurrency/pipelines/stagedpipe"
)

type Data struct {
	ID    string
	Bytes [][]byte
}

type SM[T Data] struct {
	idService *SendIDClient
}

// NewSM creates a new stagepipe.StateMachine.
func NewSM() *SM[Data] {
	sm := &SM[Data]{
		idService: NewSendIDClient(),
	}
	return sm
}

// Close stops all running goroutines. This is only safe after all entries have
// been processed.
func (s *SM[T]) Close() {}

// Start implements stagedpipe.StateMachine.Start().
func (s *SM[T]) Start(req stagedpipe.Request[Data]) stagedpipe.Request[Data] {
	switch {
	case len(req.Data.Bytes) == 0:
		req.Err = fmt.Errorf("Request.Data cannot be empty")
		return req
	}

	req.Next = s.ProcID
	return req
}

func (s *SM[T]) ProcID(req stagedpipe.Request[Data]) stagedpipe.Request[Data] {
	req.Data.ID = uuid.New().String()

	req.Next = s.SendID
	return req
}

func (s *SM[T]) SendID(req stagedpipe.Request[Data]) stagedpipe.Request[Data] {
	s.idService.Send(req)

	req.Next = nil
	return req
}

type SendIDClient struct {
}

func NewSendIDClient() *SendIDClient {
	return &SendIDClient{}
}

func (s *SendIDClient) Send(req stagedpipe.Request[Data]) {

	for _, id := range req.Data.Bytes {
		reverse(id)
	}
}

func reverse(s []byte) {
	for i, j := 0, len(s)-1; i < j; i, j = i+1, j-1 {
		s[i], s[j] = s[j], s[i]
	}
}

// NewRequest creates a new Request.
func NewRequest(ctx context.Context) stagedpipe.Request[Data] {
	data := make([][]byte, 0, 1000)
	for i := 0; i < 1000; i++ {
		b := make([]byte, 1024)
		_, err := rand.Read(b)
		if err != nil {
			log.Fatalf("error while generating random bytes: %s", err)
		}
		data = append(data, b)
	}

	return stagedpipe.Request[Data]{Ctx: ctx, Data: Data{Bytes: data}}
}
