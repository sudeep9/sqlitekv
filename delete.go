package sqlitekv

import "context"

func (c *Collection) Delete(ctx context.Context, id int64) (err error) {
	err = c.kv.withWriteLock(func() error {
		return c.delete(ctx, id)
	})
	if err != nil {
		err = mapSqliteError(err)
		return
	}

	return
}

func (c *Collection) delete(ctx context.Context, id int64) (err error) {
	err = c.delStmt.Exec(id)
	return
}
