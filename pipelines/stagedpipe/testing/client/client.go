// Package client provides a fake client to a fictional "identity" service
// to use in testing.
package client

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"sync/atomic"
	"time"
)

var (
	// ErrNotFound indicates that the Record for the ID given could not be found.
	ErrNotFound = fmt.Errorf("ID not found")
)

// Record holds records on a person. Before processing, we should have
// "First", "Last", and "ID" set.
type Record struct {
	// First is the first name of the person.
	First string
	// Last is the last name of the person.
	Last string
	// ID is the ID
	ID string

	// Birth is the time the person was born.
	Birth time.Time
	// BirthTown is what town the person was born in.
	BirthTown string
	// State is the state the person was born in.
	BirthState string

	// Err is a data error on this Record.
	Err error
}

// ID is a client to our fake identity service.
type ID struct {
	// itemNum is the current call number.
	itemNum atomic.Int64
	// v is the number current record number.
	v atomic.Int64
	// errAt indicates if this should error at specific call. errAt 1 will error
	// on the first call. 0 will not error at all.
	errAt int
	// notFoundAt indicates that the "notFoundAt" Record returned should return a NotFoundErr
	// in that Record (not a Call error). This number might not be hit for several Call().
	notFoundAt int
	// err if set will cause this to send this error on every call.
	err error
}

const day = 24 * time.Hour

func (i *ID) Call(ctx context.Context, recs []Record) ([]Record, error) {
	if i.err != nil {
		return recs, errors.New("error")
	}
	if i.errAt == int(i.itemNum.Add(1)) {
		return recs, errors.New("error")
	}

	for x, rec := range recs {
		n := i.v.Add(1)
		if i.notFoundAt == int(n) {
			rec.Err = ErrNotFound
			recs[x] = rec
			continue
		}
		if rec.ID == "" {
			rec.Err = fmt.Errorf("empty ID field")
			recs[x] = rec
			continue
		}
		idNum, err := strconv.Atoi(rec.ID)
		if err != nil {
			panic(err)
		}
		rec.Birth = time.Time{}.Add(time.Duration(idNum) * day)
		rec.BirthTown = "Nashville"
		rec.BirthState = "Tennessee"
		recs[x] = rec
	}
	return recs, nil
}
