package sqlitekv

import (
	"context"

	"github.com/goccy/go-json"
	"github.com/klauspost/compress/zstd"
)

func (c *Collection) Put(ctx context.Context, val CollectionType) (changeCount int, err error) {
	args, err := c.bindInsertParams(val, true)
	if err != nil {
		return
	}
	err = c.kv.withWriteLock(func() error {
		changeCount, err = c.put(ctx, args)
		c.updStmt.ClearBindings()

		return err
	})

	if err != nil {
		err = mapSqliteError(err)
		return
	}

	return
}

func (c *Collection) put(ctx context.Context, args []any) (changes int, err error) {
	err = c.putStmt.Exec(args...)
	if err != nil {
		return
	}

	changes = c.kv.conn.Changes()

	return
}

func (c *Collection) bindInsertParams(val CollectionType, isUpdate bool) (args []any, err error) {
	args = make([]any, 2+len(c.opts.Columns))
	if isUpdate {
		args[0] = val.GetId()
	} else {
		if c.opts.AutoId {
			id := GenerateID64()
			args[0] = id
			val.SetId(id)
		} else {
			args[0] = val.GetId()
		}
	}

	var buf []byte
	if c.opts.Json {
		buf, err = json.Marshal(val)
	} else {
		buf, err = val.GetVal()
	}
	if err != nil {
		return
	}

	if c.opts.Compress {
		cbuf := Compress(buf)
		args[1] = cbuf
	} else {
		args[1] = buf
	}

	var ok bool
	var colVal any
	for i := range c.opts.Columns {
		colName := c.opts.Columns[i].Name
		ok, colVal, err = val.Column(i, colName)
		if err != nil {
			return
		}

		if !ok {
			args[2+i] = nil
		} else {
			args[2+i] = colVal
		}
	}

	return
}

// Create a writer that caches compressors.
// For this operation type we supply a nil Reader.
var encoder, _ = zstd.NewWriter(nil)

// Compress a buffer.
// If you have a destination buffer, the allocation in the call can also be eliminated.
func Compress(src []byte) []byte {
	return encoder.EncodeAll(src, make([]byte, 0, len(src)))
}
