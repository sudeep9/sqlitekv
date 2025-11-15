package sqlitekv

import "context"

type GetFn func(int64, []byte) error

func (c *Collection) Get(ctx context.Context, key string, fn GetFn) (ok bool, err error) {
	c.kv.rw.Lock()
	ok, err = c.get(ctx, key, fn)

	if err = c.getStmt.ClearBindings(); err != nil {
		c.kv.rw.Unlock()
		return
	}

	if err = c.getStmt.Reset(); err != nil {
		c.kv.rw.Unlock()
		return
	}

	c.kv.rw.Unlock()
	return
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

	rawBytes, err := c.getStmt.ColumnRawBytes(0)
	if err != nil {
		return
	}

	if fn == nil {
		return
	}

	err = fn(0, rawBytes)
	if err != nil {
		return
	}

	return
}
