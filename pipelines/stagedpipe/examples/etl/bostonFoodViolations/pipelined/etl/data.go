package etl

import (
	"context"
	_ "embed"
	"sync"
	"time"

	"github.com/jackc/pgx/v4"

	"github.com/gostdlib/concurrency/pipelines/stagedpipe"
)

// Row represents a row in the input CSV file.
// Differences from original:
//
//	Renamed some fields to Go standard, aka Violdttm became ViolDTTM
//	Re-ordered struct to get a minor space savings (that in this case won't probably matter)
type Row struct {
	Time       time.Time `db:"time"`
	ViolDTTM   string    `csv:"violdttm" db:"-"`
	Business   string    `csv:"businessname" db:"business_name"`
	LicStatus  string    `csv:"licstatus" db:"license_status"`
	Result     string    `csv:"result" db:"result"`
	ViolDesc   string    `csv:"violdesc" db:"description"`
	ViolStatus string    `csv:"violstatus" db:"status"`
	ViolLevel  string    `csv:"viollevel" db:"-"`
	Comments   string    `csv:"comments" db:"comments"`
	Address    string    `csv:"address" db:"address"`
	City       string    `csv:"city" db:"city"`
	Zip        string    `csv:"zip" db:"zip"`
	Level      int       `db:"level"`
}

// DeString converts certain Row fields such as ViolDTTM to Time and VioLevel into Level.
func (r Row) DeString() (Row, error) {
	switch r.ViolLevel {
	case "*":
		r.Level = 1
	case "**":
		r.Level = 2
	case "***":
		r.Level = 3
	default:
		r.Level = -1
	}

	if r.ViolDTTM != "" {
		var err error
		r.Time, err = time.Parse("2006-01-02 15:04:05", r.ViolDTTM)
		if err != nil {
			return r, err
		}
	}

	return r, nil
}

//go:embed insert.sql
var insertSQL string

// InsertQuery generates the query and arguments needed to insert this Row into
// the database.
func (r Row) InsertQuery() (query string, args []any) {
	a := []any{
		r.Business,
		r.LicStatus,
		r.Result,
		r.ViolDesc,
		r.Time,
		r.ViolStatus,
		r.Level,
		r.Comments,
		r.Address,
		r.City,
		r.Zip,
	}
	return insertSQL, a
}

// Data is the data that is passed through the pipeline.
type Data struct {
	// Tx is the transaction to use for the database update.
	Tx pgx.Tx
	// TxMutext protects Transaction writes.
	TxMutex *sync.Mutex
	// Rows is the input transformed into a []Row representation.
	Rows []Row
}

// NewRequest returns a new stagedpipe.Request object for use in the Pipelines.
// By convention we always have NewRequest() function.  If NewRequest can return
// an error, we also include a MustNewRequest().
func NewRequest(ctx context.Context, data Data) stagedpipe.Request[Data] {
	return stagedpipe.Request[Data]{
		Ctx:  ctx,
		Data: data,
	}
}
