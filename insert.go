package sqlitekv

import (
	"context"
)

func (c *Collection) Insert(ctx context.Context, val CollectionType) (changeCount int, err error) {
	args, err := c.bindInsertParams(val, false)
	if err != nil {
		return
	}
	err = c.kv.withWriteLock(func() error {
		changeCount, err = c.insert(ctx, args)
		c.insStmt.ClearBindings()
		return err
	})
	if err != nil {
		err = mapSqliteError(err)
		return
	}

	return
}

func (c *Collection) insert(ctx context.Context, args []any) (changeCount int, err error) {
	err = c.insStmt.Exec(args...)
	if err != nil {
		return
	}

	changeCount = c.kv.conn.Changes()

	return
}
