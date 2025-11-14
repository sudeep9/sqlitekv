package sqlitekv

import (
	"context"
	"fmt"
	"strings"
)

type ListFn func(rid int64, key string, value []byte) error

type ListOptions struct {
	Descending bool
	MaxKeyLen  int
}

func (c *Collection) List(ctx context.Context, keyPrefix string, fn ListFn) (err error) {
	c.kv.mu.Lock()
	err = c.list(ctx, keyPrefix, fn)
	c.kv.mu.Unlock()
	return
}

func (c *Collection) list(ctx context.Context, keyPrefix string, fn ListFn) (err error) {
	sql := strings.Builder{}
	sql.WriteString(fmt.Sprintf("select id, key, value from %s", c.name))

	keyPrefix += "%"
	sql.WriteString(" where key like ?")

	if c.opts.Delimiter != "" {
		level := strings.Count(keyPrefix, c.opts.Delimiter)
		sql.WriteString(fmt.Sprintf(" and level = %d", level))
	}

	sql.WriteString(" order by key")

	fmt.Printf("sql: %s\n", sql.String())
	stmt, err := c.kv.conn.Prepare(sql.String())
	if err != nil {
		return
	}
	defer stmt.Close()

	err = stmt.Bind(keyPrefix)
	if err != nil {
		return
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

		err = fn(rid, key, buf)
		if err != nil {
			return err
		}
	}

	return
}
