package sqlitekv

import (
	"fmt"
	"sync"
	"time"

	"github.com/eatonphil/gosqlite"
)

type Options struct {
	JournalMode       string
	BusyRetryInterval time.Duration
}

type KV struct {
	conn *gosqlite.Conn
	opts *Options

	rw sync.RWMutex
}

func Open(dbpath string, opts *Options) (kv *KV, err error) {
	if opts == nil {
		opts = &Options{
			JournalMode:       "WAL",
			BusyRetryInterval: 100 * time.Millisecond,
		}
	}

	conn, err := gosqlite.Open(dbpath)
	if err != nil {
		return
	}

	kv = &KV{conn: conn, opts: opts}

	err = kv.init()
	if err != nil {
		kv.Close()
		kv = nil
	}

	kv.conn.BusyFunc(func(count int) (retry bool) {
		time.Sleep(kv.opts.BusyRetryInterval)
		return true
	})

	return
}

func (kv *KV) WithTx(fn func() error) (err error) {
	return kv.conn.WithTx(fn)
}

func (kv *KV) init() (err error) {
	if kv.opts.JournalMode != "" {
		if err = kv.conn.Exec(fmt.Sprintf("pragma journal_mode=%s", kv.opts.JournalMode)); err != nil {
			return
		}

		if err = kv.conn.Exec("pragma synchronous=NORMAL"); err != nil {
			return
		}
	}

	return
}

func (kv *KV) Close() (err error) {
	return kv.conn.Close()
}

func (k *KV) withReadLock(fn func() error) (err error) {
	k.rw.RLock()
	err = fn()
	k.rw.RUnlock()
	return
}

func (k *KV) withWriteLock(fn func() error) (err error) {
	k.rw.Lock()
	err = fn()
	k.rw.Unlock()
	return
}

func mapSqliteError(err error) error {
	if err == nil {
		return nil
	}

	sqliteErr, ok := err.(*gosqlite.Error)
	if !ok {
		return err
	}

	code := sqliteErr.Code()

	switch code {
	case SQLITE_CONSTRAINT_UNIQUE:
		return ErrUniqueConstraint
	case SQLITE_CONSTRAINT_PRIMARYKEY:
		return ErrPrimaryConstraint
	default:
		return err
	}
}
