package sqlitekv

import (
	"sync"

	"zombiezen.com/go/sqlite"
	"zombiezen.com/go/sqlite/sqlitex"
)

type DB struct {
	rw sync.RWMutex
	db *sqlite.Conn
}

func NewDB(conn *sqlite.Conn) *DB {
	return &DB{
		db: conn,
	}
}

func (d *DB) Close() error {
	d.rw.Lock()
	defer d.rw.Unlock()
	return d.db.Close()
}

func (d *DB) WithRead(fn func(*sqlite.Conn) error) (err error) {
	d.rw.RLock()
	err = fn(d.db)
	d.rw.RUnlock()
	return
}

func (d *DB) WithWrite(fn func(*sqlite.Conn) error) (err error) {
	d.rw.Lock()
	err = fn(d.db)
	d.rw.Unlock()
	return
}

func (d *DB) WithTx(fn func(*sqlite.Conn) error) (err error) {
	d.WithWrite(func(c *sqlite.Conn) error {
		if err := sqlitex.Execute(d.db, "BEGIN", nil); err != nil {
			return err
		}

		if err := fn(d.db); err != nil {
			if err := sqlitex.Execute(d.db, "ROLLBACK", nil); err != nil {
				return err
			}
			return err
		}

		return sqlitex.Execute(d.db, "COMMIT", nil)
	})
	return
}
