/*
Package etl contains the ETL statemachine for translating the boston food violations file
into a postgres database.

This implementation is almost 5x faster than the original (when the original is translated to
postgres from sqlite, however with sqlite they are still on par), however this is probably due
more to doing batch updates than the concurrency aspects.

The major speed inhibiters are going to be how fast we can read from disk and how fast we can write
to Postgres. Where this implementation shines is if you need to query other data sources for your
ETL and expanding the structure in a uniform way. Or if the data you are pulling is coming from
a network source that can send at speed and your writes are to a data store you can write at speed
(for example, super fast distributed data stores). This is a little overkill for this tiny ETL file.
*/
package etl

import (
	"context"
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/cenkalti/backoff"
	"github.com/jackc/pgconn"
	"github.com/jackc/pgx/v4"

	"github.com/gostdlib/concurrency/pipelines/stagedpipe"
)

// SM implements stagedpipe.StateMachine. It holds all our states for the pipeline.
type SM struct{}

// NewSM creates a new stagepipe.StateMachine from SM. db is used to create a
// transaction.
func NewSM() (*SM, error) {
	sm := &SM{}
	return sm, nil
}

// Start is the entry point (first stage) of the pipeline. In this case
// we simply send converts Row fields that are represented as strings into more
// concrete types such as int and time.Time.
func (s *SM) Start(req stagedpipe.Request[Data]) stagedpipe.Request[Data] {
	var err error

	for i, row := range req.Data.Rows {
		req.Data.Rows[i], err = row.DeString()
		if err != nil {
			req.Err = err
			return req
		}
	}

	req.Next = s.WriteDB
	return req
}

// WriteDB takes the rows in the data and writes it via our transaction to the database.
func (s *SM) WriteDB(req stagedpipe.Request[Data]) stagedpipe.Request[Data] {
	batch := &pgx.Batch{}

	for _, row := range req.Data.Rows {
		q, a := row.InsertQuery()
		batch.Queue(q, a...)
	}

	req.Data.TxMutex.Lock()
	defer req.Data.TxMutex.Unlock()

	if err := exec(req.Ctx, batch, req.Data.Tx); err != nil {
		req.Err = fmt.Errorf("error sending batch: %w", err)
		return req
	}

	req.Next = nil
	return req
}

// Close implements stagedpipe.StateMachine.Close(). It shuts down resources in the
// StateMachine that are no longer needed. This is only safe after all entries
// have been processed.
func (s *SM) Close() {
	// We don't need to do anything here, as all resources are closed at the
	// parent level.
}

// exec writes batch b to transaction tx. It uses a basic exponential backoff to backoff
// when receiving non critical error codes.
func exec(ctx context.Context, b *pgx.Batch, tx pgx.Tx) error {
	if _, ok := ctx.Deadline(); !ok {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, 1*time.Minute)
		defer cancel()
	}

	// This is the operation that will be run with exponential backoff retries.
	op := func() error {
		results := tx.SendBatch(ctx, b)
		defer results.Close()

		_, err := results.Exec()
		if err != nil {
			var pgErr *pgconn.PgError
			if errors.As(err, &pgErr) {
				if pgErr.Severity == "FATAL" {
					return backoff.Permanent(err)
				}
				// Error codes I've determined are fatal.
				switch pgErr.Code {
				case "25P02", "42703", "22P04", "22021", "42601", "42P01":
					return backoff.Permanent(err)
				}
			}
			log.Println("Batch send non-permanent error: ", err)
			return err
		}
		return nil
	}

	err := backoff.Retry(
		op,
		backoff.WithContext(
			backoff.NewExponentialBackOff(),
			ctx,
		),
	)
	if err != nil {
		return err
	}

	return nil
}
