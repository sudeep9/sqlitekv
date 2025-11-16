package sqlitekv

import (
	"context"

	"encoding/json"
)

func (c *Collection) Update(ctx context.Context, obj CollectionType) (changeCount int, err error) {
	args := make([]any, 2+len(c.opts.Columns))
	id := obj.GetId()

	if c.opts.Json {
		args[0], err = json.Marshal(obj)
	} else {
		args[0], err = obj.GetVal()
	}
	if err != nil {
		return
	}

	var ok bool
	var colVal any
	for i := range c.opts.Columns {
		colName := c.opts.Columns[i].Name
		ok, colVal, err = obj.Column(i, colName)
		if err != nil {
			return
		}

		if !ok {
			args[1+i] = nil
		} else {
			args[1+i] = colVal
		}
	}

	args[len(args)-1] = id

	err = c.kv.withWriteLock(func() error {
		changeCount, err = c.update(ctx, args)
		c.updStmt.ClearBindings()
		return err
	})
	if err != nil {
		err = mapSqliteError(err)
		return
	}
	return
}

func (c *Collection) update(ctx context.Context, args []any) (changeCount int, err error) {
	err = c.updStmt.Exec(args...)
	if err != nil {
		return
	}

	changeCount = c.kv.conn.Changes()
	return
}
