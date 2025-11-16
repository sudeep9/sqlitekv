package sqlitekv

import (
	"context"
	"fmt"

	"github.com/eatonphil/gosqlite"
	"github.com/goccy/go-json"
)

type GetFn func(id int64, key string, rawJson []byte, genColVals []any) error

func (c *Collection) Get(ctx context.Context, obj CollectionType) (ok bool, err error) {
	return c.getWithLock(ctx, "id", obj.GetId(), obj)
}

func (c *Collection) GetUnique(ctx context.Context, colname string, colval any, obj CollectionType) (ok bool, err error) {
	return c.getWithLock(ctx, colname, colval, obj)
}

func (c *Collection) getWithLock(ctx context.Context, colname string, colval any, obj CollectionType) (ok bool, err error) {
	var stmt *gosqlite.Stmt
	if colname == "id" {
		stmt = c.getStmt
	} else {
		stmt, ok = c.uniqueStmt[colname]
		if !ok {
			return false, fmt.Errorf("no unique index for column %s", colname)
		}
	}

	err = c.kv.withReadLock(func() error {
		ok, err = c.get(ctx, stmt, colval, obj)
		if err != nil {
			return err
		}
		if err = stmt.ClearBindings(); err != nil {
			return err
		}

		if err = stmt.Reset(); err != nil {
			return err
		}

		return nil
	})
	return ok, err
}

func (c *Collection) get(ctx context.Context, stmt *gosqlite.Stmt, searchVal any, obj CollectionType) (ok bool, err error) {
	err = stmt.Reset()
	if err != nil {
		return
	}

	if err = stmt.Bind(searchVal); err != nil {
		return
	}

	ok, err = stmt.Step()
	if err != nil {
		return
	}

	if !ok {
		return
	}

	id, _, err := stmt.ColumnInt64(0)
	if err != nil {
		return
	}

	obj.SetId(id)

	rawBytes, err := stmt.ColumnRawBytes(1)
	if err != nil {
		return
	}

	if c.opts.Json {
		err = json.Unmarshal(rawBytes, obj)
	} else {
		err = obj.SetVal(rawBytes)
	}

	if err != nil {
		return
	}

	var val any
	for i := range c.opts.Columns {
		switch c.opts.Columns[i].Type {
		case "text":
			val, ok, err = stmt.ColumnText(2 + i)
		case "integer":
			val, ok, err = stmt.ColumnInt64(2 + i)
		default:
			err = fmt.Errorf("unknown type for get type=%s", c.opts.Columns[i].Type)
		}
		if err != nil {
			return
		}
		err = obj.SetColumn(i, c.opts.Columns[i].Name, ok, val)
		if err != nil {
			return
		}
	}

	return
}
