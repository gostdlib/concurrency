package main

import (
	"bufio"
	"context"
	_ "embed"
	"encoding/csv"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"runtime"
	"sync"
	"time"

	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/jszwec/csvutil"

	"github.com/gostdlib/concurrency/pipelines/stagedpipe"
	"github.com/gostdlib/concurrency/pipelines/stagedpipe/examples/etl/bostonFoodViolations/pipelined/etl"
)

var (
	filePath = flag.String("file", "../violations.csv", "The path to the file to parse")
	connStr  = flag.String("connStr", "", "The connection string to your postgres database")
)

// concurrency is the number of concurrent pipelines we have running. Each pipeline is running
// X stages in parallel, where X is the number of stages.
var concurrency = runtime.NumCPU()

//go:embed etl/drop.sql
var dropTable string

//go:embed etl/create.sql
var createTable string

func main() {
	log.SetFlags(log.LstdFlags | log.Lshortfile)
	flag.Parse()

	ctx := context.Background()

	// Setup DB connection.
	connCtx, connCancel := context.WithTimeout(ctx, 10*time.Second)

	pool, err := pgxpool.Connect(connCtx, *connStr)
	if err != nil {
		log.Fatalf("cannot connect to Postgres: %s", err)
	}
	connCancel()

	_, err = pool.Exec(ctx, dropTable)
	if err != nil {
		log.Fatalf("could not drop an existing violations table: %s", err)
	}

	_, err = pool.Exec(ctx, createTable)
	if err != nil {
		log.Fatalf("could not drop an existing violations table: %s", err)
	}

	// Setup DB transaction.
	tx, err := pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		log.Fatalf("cannot start a transaction: %s", err)
	}
	txMutex := &sync.Mutex{}

	// Setup our file access to read in.
	f, err := newCSVBlocks[etl.Row](*filePath, 100000)
	if err != nil {
		log.Fatalf("cannot open our file: %s", err)
	}

	// Setup our pipeline.
	sm, err := etl.NewSM()
	if err != nil {
		log.Fatalf("cannot start state machine: %s", err)
	}

	pipeline, err := stagedpipe.New[etl.Data]("boston food violations", concurrency, sm)
	if err != nil {
		log.Fatalf("cannot create a pipeline: %s", err)
	}
	defer pipeline.Close()

	// Setup our request group to send our data on.
	rg := pipeline.NewRequestGroup()
	reqCtx, reqCancel := context.WithCancel(ctx)
	defer reqCancel() // Not really needed, but for consistency

	start := time.Now()

	// Gets all the output from the pipeline and checks for errors. We don't use
	// any of the output otherwise, as the pipeline writes the data to the database.
	done := make(chan error, 1)
	go func() {
		var err error
		defer func() {
			defer close(done)
			if err != nil {
				done <- err
			}
		}()

		// A RequestGroup must always drain its .Out() channel. If we receive an error and
		// want to stop processing, we can cancel the Context and wait for everything to stop.
		// Here we capture the error so that we can report it. If we get an error, we also
		// rollback the transaction.
		for out := range rg.Out() {
			if err != nil {
				continue
			}
			if out.Err != nil {
				reqCancel()
				err = out.Err
				log.Printf("pipeline had error in stream: %s", out.Err)
			}
		}
		if err != nil {
			tx.Rollback(ctx)
		}
	}()

	fmt.Println("Starting to process records...")

	// Sent blocks of data into the pipeline.
	items := 0
	for block := range f.blocks(reqCtx) {
		if block.Err != nil {
			if block.Err == context.Canceled {
				log.Println("fileBlock received context.Canceled")
				break
			}
			log.Fatalf("problem reading csv block: %s", block.Err)
		}
		items += len(block.Data)
		req := etl.NewRequest(reqCtx, etl.Data{Rows: block.Data, Tx: tx, TxMutex: txMutex})
		if err := rg.Submit(req); err != nil {
			log.Fatalf("problem submitting request to pipeline: %s", err)
		}
	}
	// Tell the pipeline that this request group is done.
	rg.Close()

	// We have processed all output.
	processingErr := <-done

	if processingErr != nil {
		fmt.Printf("Pipeline had processing error(DB transaction rolled back): %s", processingErr)
		os.Exit(1)
	}
	fmt.Println("Pipeline has completed processing")
	fmt.Printf("Processed %d records into Postgres\n", items)
	fmt.Println("Committing Transaction to Postgres...")

	if err := tx.Commit(ctx); err != nil {
		log.Fatalf("transaction commit failure: %s", err)
	}
	end := time.Since(start)

	fmt.Println("Commit complete!  We are DONE!")
	fmt.Println("Time taken: ", end)
}

// streamResp holds any streaming response object for a channel.
type streamResp[T any] struct {
	Data T
	Err  error
}

// csvBlocks reads a CSV file that must contain a header into a bufio.Reader and
// returns "items" number of decoded records into type T, where T should be a struct.
type csvBlocks[T any] struct {
	f     io.ReadCloser
	buf   *bufio.Reader
	dec   *csvutil.Decoder
	items int
}

// newCSVBlocks opens a csv file at "path" and streams results in "items" blocks that are
// decoded into type T, where T should be a struct. "items" must be >= than 100.
func newCSVBlocks[T any](path string, items int) (csvBlocks[T], error) {
	if items < 100 {
		return csvBlocks[T]{}, fmt.Errorf("newCSVBlocks() cannot be called with items set to less than 100")
	}

	f, err := os.Open(path)
	if err != nil {
		return csvBlocks[T]{}, err
	}

	buf := bufio.NewReaderSize(f, 10*1024*1024)

	r := csv.NewReader(buf)
	dec, err := csvutil.NewDecoder(r)
	if err != nil {
		return csvBlocks[T]{}, err
	}
	return csvBlocks[T]{
		f:     f,
		buf:   buf,
		items: items,
		dec:   dec,
	}, nil
}

// blocks returns a channel that sends all the blocks. The caller is responsible for
// reading all data until the channel closes, even if cancelling the Context.
func (c csvBlocks[T]) blocks(ctx context.Context) chan streamResp[[]T] {
	ch := make(chan streamResp[[]T], 1)

	go func() {
		defer close(ch)

		for {
			if ctx.Err() != nil {
				ch <- streamResp[[]T]{Err: ctx.Err()}
				return
			}

			out := []T{}

			for i := 0; i < c.items; i++ {
				var row T
				err := c.dec.Decode(&row)
				if err != nil {
					if err == io.EOF {
						if len(out) > 0 {
							ch <- streamResp[[]T]{Data: out}
						}
						return
					}
					ch <- streamResp[[]T]{Err: err}
					return
				}
				out = append(out, row)
			}
			if len(out) > 0 {
				ch <- streamResp[[]T]{Data: out}
			}
		}
	}()

	return ch
}
