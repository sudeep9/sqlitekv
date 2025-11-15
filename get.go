package sqlitekv

import "context"

type GetFn func(id int64, key string, rawJson []byte, genColVals []any) error

func (c *Collection) Get(ctx context.Context, key string, fn GetFn) (ok bool, err error) {
	err = c.kv.withReadLock(func() error {
		ok, err = c.get(ctx, key, fn)
		if err = c.getStmt.ClearBindings(); err != nil {
			return err
		}

		if err = c.getStmt.Reset(); err != nil {
			return err
		}

		return nil
	})
	return ok, err
}

func (c *Collection) get(ctx context.Context, key string, fn GetFn) (ok bool, err error) {
	err = c.getStmt.Reset()
	if err != nil {
		return
	}

	if err = c.getStmt.Bind(key); err != nil {
		return
	}

	ok, err = c.getStmt.Step()
	if err != nil {
		return
	}

	if !ok {
		return
	}
	if fn == nil {
		return
	}

	rid, _, err := c.getStmt.ColumnInt64(0)
	if err != nil {
		return
	}

	rawBytes, err := c.getStmt.ColumnRawBytes(1)
	if err != nil {
		return
	}

	var genColVals []any
	if len(c.opts.Columns) > 0 {
		genColVals = make([]any, len(c.opts.Columns))
		for i := range c.opts.Columns {
			switch c.opts.Columns[i].Type {
			case "text":
				genColVals[i], _, err = c.getStmt.ColumnText(2 + i)
			case "integer":
				genColVals[i], _, err = c.getStmt.ColumnInt64(2 + i)
			default:
				genColVals[i] = nil
			}
		}
	}

	err = fn(rid, key, rawBytes, genColVals)

	return
}

func (c *Collection) GetById(ctx context.Context, id int64, fn GetFn) (err error) {
	c.kv.rw.RLock()
	err = c.getById(ctx, id, fn)
	c.kv.rw.RUnlock()
	return
}

func (c *Collection) getById(ctx context.Context, id int64, fn GetFn) (err error) {
	err = c.getByIdStmt.Reset()
	if err != nil {
		return
	}

	err = c.getByIdStmt.Bind(id)
	if err != nil {
		return
	}

	ok, err := c.getByIdStmt.Step()
	if err != nil {
		return
	}
	if !ok {
		return
	}

	key, _, err := c.getStmt.ColumnText(0)
	if err != nil {
		return
	}

	rawBytes, err := c.getStmt.ColumnRawBytes(1)
	if err != nil {
		return
	}

	var genColVals []any
	if len(c.opts.Columns) > 0 {
		genColVals = make([]any, len(c.opts.Columns))
		for i := range c.opts.Columns {
			switch c.opts.Columns[i].Type {
			case "text":
				genColVals[i], _, err = c.getStmt.ColumnText(2 + i)
			case "integer":
				genColVals[i], _, err = c.getStmt.ColumnInt64(2 + i)
			default:
				genColVals[i] = nil
			}
		}
	}

	err = fn(id, key, rawBytes, genColVals)

	return
}
