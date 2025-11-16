package sqlitekv

import (
	"context"
)

func (c *Collection) Insert(ctx context.Context, val CollectionType) (err error) {
	err = c.kv.withWriteLock(func() error {
		return c.put(ctx, val)
	})

	return
}

func (c *Collection) insert(ctx context.Context, val CollectionType) (err error) {
	args, err := c.bindInsertParams(val, false)
	if err != nil {
		return
	}

	err = c.insStmt.Exec(args...)
	if err != nil {
		return
	}

	return
}
