package sqlitekv

import (
	"context"
	"strconv"
	"strings"

	"github.com/eatonphil/gosqlite"
)

type SelectOptions struct {
	TableAlias string
	Columns    []string
	Where      string
	OrderBy    []string
	Limit      int
}

type GetColumnValueFn func(i int) (val any, ok bool, err error)

type RowFn func(fn GetColumnValueFn) error

func (c *Collection) Select(ctx context.Context, rowfn RowFn, opts SelectOptions) (err error) {
	sql := strings.Builder{}

	sql.WriteString("select ")
	for i, col := range opts.Columns {
		if i > 0 {
			sql.WriteString(", ")
		}
		sql.WriteString(col)
	}

	sql.WriteString(" from ")
	sql.WriteString(c.name)
	if opts.TableAlias != "" {
		sql.WriteString(" as ")
		sql.WriteString(opts.TableAlias)
	}

	if opts.Where != "" {
		sql.WriteString(" where ")
		sql.WriteString(opts.Where)
	}

	if len(opts.OrderBy) > 0 {
		sql.WriteString(" order by ")
		sql.WriteString(strings.Join(opts.OrderBy, ", "))
	}

	if opts.Limit > 0 {
		sql.WriteString(" limit ")
		sql.WriteString(strconv.Itoa(opts.Limit))
	}

	err = c.kv.withReadLock(func() error {
		return c.selectNoLock(ctx, sql.String(), rowfn)
	})

	if err != nil {
		return
	}

	return
}

func getValue(stmt *gosqlite.Stmt) GetColumnValueFn {
	return func(i int) (val any, ok bool, err error) {
		colType := stmt.ColumnType(i)
		switch colType {
		case 1:
			val, ok, err = stmt.ColumnInt64(i)
		case 2:
			val, ok, err = stmt.ColumnDouble(i)
		case 3:
			val, ok, err = stmt.ColumnRawString(i)
		case 4:
			val, err = stmt.ColumnBlob(i)
			ok = true
		default:
			err = nil
			ok = false
		}
		return val, ok, err
	}
}

func (c *Collection) selectNoLock(ctx context.Context, sql string, rowFn RowFn) (err error) {
	stmt, err := c.kv.conn.Prepare(sql)
	if err != nil {
		return
	}
	defer stmt.Close()

	valueFn := getValue(stmt)

	for {
		ok, errStep := stmt.Step()
		if errStep != nil {
			err = errStep
			return
		}
		if !ok {
			break
		}

		err = rowFn(valueFn)
		if err != nil {
			return
		}
	}

	return nil
}
