package sqlitekv

import (
	"context"
	"fmt"
	"strconv"
	"strings"
)

type SelectOptions struct {
	Where   string
	OrderBy []string
	Limit   int
}

type ParseFn func(obj CollectionType) error
type RowFn func(parseFn ParseFn) error

func (c *Collection) Select(ctx context.Context, rowFn RowFn, opts SelectOptions) (err error) {
	sql := strings.Builder{}

	sql.WriteString("select id, val")
	for _, col := range c.opts.Columns {
		sql.WriteString(",")
		sql.WriteString(col.Name)
	}

	sql.WriteString(" from ")
	sql.WriteString(c.name)

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
		return c.selectNoLock(ctx, sql.String(), rowFn)
	})

	if err != nil {
		return
	}

	return
}

func (c *Collection) selectNoLock(ctx context.Context, sql string, rowFn RowFn) (err error) {
	fmt.Printf("select sql: %s\n", sql)
	stmt, err := c.kv.conn.Prepare(sql)
	if err != nil {
		return
	}
	defer stmt.Close()

	parseFn := func(obj CollectionType) error {
		return c.parseObj(stmt, obj)
	}

	for {
		ok, errStep := stmt.Step()
		if errStep != nil {
			err = errStep
			return
		}
		if !ok {
			break
		}

		err = rowFn(parseFn)
		if err != nil {
			return err
		}
	}

	return nil
}

func GetAccumulateFn[T any](out *[]*T) (fn RowFn) {
	fn = func(parseFn ParseFn) error {
		v := new(T)
		obj := any(v).(CollectionType)
		err := parseFn(obj)
		if err != nil {
			return err
		}
		*out = append(*out, v)
		return nil
	}

	return fn
}
