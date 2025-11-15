package sqlitekv

import (
	"context"
	"fmt"
	"strings"
)

type ListOptions struct {
	OrderBy    []string
	Descending bool
	All        bool
}

func (c *Collection) List(ctx context.Context, keyPrefix string, fn GetFn, opts ListOptions) (err error) {
	if keyPrefix == "" {
		keyPrefix = "/"
	} else {
		if !strings.HasSuffix(keyPrefix, c.opts.Delimiter) {
			keyPrefix += c.opts.Delimiter
		}
	}

	sql := strings.Builder{}
	genColSelect := c.getGeneratedColumnSelect()
	sql.WriteString(fmt.Sprintf("select id, key, json(val) %s from %s", genColSelect, c.name))

	sql.WriteString(" where key like ?")

	level := strings.Count(keyPrefix, c.opts.Delimiter)
	if !opts.All {
		sql.WriteString(fmt.Sprintf(" and level = %d", level))
	}

	if len(opts.OrderBy) > 0 {
		sql.WriteString(" order by ")
		sql.WriteString(strings.Join(opts.OrderBy, ", "))
	} else {
		sql.WriteString(" order by key")
		if opts.Descending {
			sql.WriteString(" desc")
		} else {
			sql.WriteString(" asc")
		}
	}

	keyPrefix += "%"

	err = c.kv.withReadLock(func() error {
		return c.list(ctx, keyPrefix, sql.String(), fn, opts)
	})

	return
}

func (c *Collection) list(ctx context.Context, keyPrefix, sql string, fn GetFn, opts ListOptions) (err error) {
	stmt, err := c.kv.conn.Prepare(sql)
	if err != nil {
		return
	}
	defer stmt.Close()

	err = stmt.Bind(keyPrefix)
	if err != nil {
		return
	}

	genColCount := stmt.ColumnCount() - 3

	for {
		ok, errStep := stmt.Step()
		if errStep != nil {
			err = errStep
			return
		}
		if !ok {
			break
		}

		rid, _, err := stmt.ColumnInt64(0)
		if err != nil {
			return err
		}

		key, _, err := stmt.ColumnText(1)
		if err != nil {
			return err
		}

		buf, err := stmt.ColumnRawBytes(2)
		if err != nil {
			return err
		}

		var genColVals []any

		if genColCount > 0 {
			genColVals = make([]any, genColCount)
			for i := range genColVals {
				colIndex := 3 + i
				colType := stmt.ColumnType(colIndex)

				switch colType {
				case 3:
					genColVals[i], _, err = stmt.ColumnText(colIndex)
				case 1:
					genColVals[i], _, err = stmt.ColumnInt64(colIndex)
				default:
					genColVals[i] = nil
					err = nil
				}
				if err != nil {
					return err
				}
			}
		}

		err = fn(rid, key, buf, genColVals)
		if err != nil {
			return err
		}
	}

	return
}
