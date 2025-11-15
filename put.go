package sqlitekv

import (
	"context"
	"strings"
)

func (c *Collection) Put(ctx context.Context, key string, buf []byte) (rowid int64, err error) {
	err = c.kv.withWriteLock(func() error {
		rowid, err = c.put(ctx, key, buf)
		return err
	})

	return
}

func (c *Collection) put(ctx context.Context, key string, buf []byte) (rowid int64, err error) {
	level := strings.Count(key, c.opts.Delimiter)
	err = c.putStmt.Exec(key, level, buf)
	if err != nil {
		return
	}

	rowid = c.kv.conn.LastInsertRowID()
	return
}
