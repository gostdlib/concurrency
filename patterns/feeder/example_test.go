package feeder_test

import (
	"fmt"

	"github.com/gostdlib/base/context"
	"github.com/gostdlib/concurrency/patterns/feeder"

	"github.com/gostdlib/db/safe"
	"github.com/gostdlib/db/sqlite"
	"github.com/gostdlib/db/sqlite/sqlitex"
)

// Example feeds a pipeline of operations into a Map via a Feeder.
func Example() {
	ctx := context.Background()

	m := &feeder.Map[string, int]{M: map[string]int{}}

	f, err := feeder.NewFeeder[string, int](ctx, m)
	if err != nil {
		fmt.Println("NewFeeder:", err)
		return
	}

	// A KVPipeline supplies the operations to apply, then closes the channel to signal completion.
	pipe := func(ctx context.Context) (chan feeder.KeyVal[string, int], chan struct{}, error) {
		ch := make(chan feeder.KeyVal[string, int], 2)
		ch <- feeder.KeyVal[string, int]{Op: feeder.Add, K: "alice", V: 1}
		ch <- feeder.KeyVal[string, int]{Op: feeder.Add, K: "bob", V: 2}
		close(ch)
		return ch, make(chan struct{}), nil
	}

	// Feed runs in the background; the returned channel yields the terminal error (nil on success).
	if err := <-f.Feed(ctx, pipe); err != nil {
		fmt.Println("Feed:", err)
		return
	}

	alice, _ := m.Get("alice")
	bob, _ := m.Get("bob")
	fmt.Println(alice, bob)
	// Output: 1 2
}

// ExampleMap uses a Map directly as a concurrency-safe Value. SetAccept and DeleteAccept are optional;
// a nil accept function means the change is always accepted.
func ExampleMap() {
	m := &feeder.Map[string, int]{M: map[string]int{}}

	if err := m.Set("a", 1); err != nil {
		fmt.Println("Set:", err)
		return
	}

	v, ok := m.Get("a")
	fmt.Println(v, ok)

	if err := m.Delete("a"); err != nil {
		fmt.Println("Delete:", err)
		return
	}

	_, ok = m.Get("a")
	fmt.Println(ok)
	// Output:
	// 1 true
	// false
}

// ExampleMap_databaseWrites shows how SetAccept and DeleteAccept mirror a Map to a database atomically.
// Both callbacks run while the Map holds its write lock, so each database write is serialized with the
// in-memory change it guards: the Map is only updated when the write succeeds, and a single sqlite.Conn
// is safe to share because the lock prevents concurrent use. Returning an error leaves the Map unchanged
// and, under a Feeder retry policy, the operation is retried unless the error wraps feeder.ErrPermanent.
func ExampleMap_databaseWrites() {
	conn, err := sqlite.OpenConn(":memory:")
	if err != nil {
		fmt.Println("open:", err)
		return
	}
	defer conn.Close()

	if err := sqlitex.Execute(conn, safe.NewSafe("CREATE TABLE kv (k TEXT PRIMARY KEY, v INTEGER);"), nil); err != nil {
		fmt.Println("create table:", err)
		return
	}

	m := &feeder.Map[string, int]{M: map[string]int{}}

	// SetAccept upserts the row before the in-memory value is accepted; if the write fails the Map is
	// left unchanged and the error is returned to the caller (and retried by a Feeder unless permanent).
	m.SetAccept = func(key string, val, prev int, replaced bool) (bool, error) {
		q := safe.NewSafe("INSERT INTO kv (k, v) VALUES (?, ?) ON CONFLICT(k) DO UPDATE SET v = excluded.v;")
		if err := sqlitex.Execute(conn, q, &sqlitex.ExecOptions{Args: []any{key, val}}); err != nil {
			return false, err
		}
		return true, nil
	}

	// DeleteAccept removes the row before the key leaves the Map.
	m.DeleteAccept = func(key string, prev int, found bool) (bool, error) {
		if err := sqlitex.Execute(conn, safe.NewSafe("DELETE FROM kv WHERE k = ?;"), &sqlitex.ExecOptions{Args: []any{key}}); err != nil {
			return false, err
		}
		return true, nil
	}

	for _, op := range []struct {
		key string
		val int
	}{{"alice", 1}, {"bob", 2}} {
		if err := m.Set(op.key, op.val); err != nil {
			fmt.Printf("set %s: %v\n", op.key, err)
			return
		}
	}
	if err := m.Delete("alice"); err != nil {
		fmt.Println("delete alice:", err)
		return
	}

	// Read the surviving rows back from the database to show it agrees with the Map.
	q := safe.NewSafe("SELECT k, v FROM kv ORDER BY k;")
	err = sqlitex.Execute(conn, q, &sqlitex.ExecOptions{
		ResultFunc: func(stmt *sqlite.Stmt) error {
			fmt.Printf("%s=%d\n", stmt.ColumnText(0), stmt.ColumnInt(1))
			return nil
		},
	})
	if err != nil {
		fmt.Println("select:", err)
		return
	}

	// Output:
	// bob=2
}
