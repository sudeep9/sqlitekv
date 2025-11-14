package sqlitekv

import (
	"fmt"
	"sync"

	"github.com/eatonphil/gosqlite"
)

type Options struct {
	JournalMode string
}

type KV struct {
	conn *gosqlite.Conn
	opts *Options

	defaultCol *Collection

	mu sync.Mutex
}

func Open(dbpath string, opts *Options) (kv *KV, err error) {
	if opts == nil {
		opts = &Options{
			JournalMode: "",
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

	return
}

func (kv *KV) init() (err error) {
	if kv.opts.JournalMode != "" {
		if err = kv.conn.Exec(fmt.Sprintf("pragma journal_mode=%s", kv.opts.JournalMode)); err != nil {
			return
		}
	}

	if kv.defaultCol, err = kv.Collection("_default", nil); err != nil {
		return
	}

	return
}

func (kv *KV) DefaultCollection() *Collection {
	return kv.defaultCol
}

func (kv *KV) Collection(name string, opts *CollectionOptions) (col *Collection, err error) {
	col, err = newCollection(kv, name, opts)
	return
}

func (kv *KV) Close() (err error) {
	return kv.conn.Close()
}
