package sqlitekv

import (
	"context"
	"strings"
)

func (c *Collection) Put(ctx context.Context, key string, value []byte) (rowid int64, err error) {
	c.kv.mu.Lock()
	rowid, err = c.put(ctx, key, value)
	c.kv.mu.Unlock()
	return
}

func (c *Collection) put(ctx context.Context, key string, value []byte) (rowid int64, err error) {
	if c.opts.Delimiter != "" {
		level := strings.Count(key, c.opts.Delimiter)
		err = c.putStmt.Exec(key, level, value)
	} else {
		err = c.putStmt.Exec(key, value)
	}

	if err != nil {
		return
	}

	rowid = c.kv.conn.LastInsertRowID()

	return
}
