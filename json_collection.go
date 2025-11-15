package sqlitekv

import (
	"context"
	"fmt"
	"strings"
	"sync"

	"github.com/eatonphil/gosqlite"
)

type GeneratedColumn struct {
	Name    string
	Type    string
	Def     string
	Storage string
}

type JsonCollectionOptions struct {
	Delimiter string
	Columns   []GeneratedColumn
	Indexes   []string
}

type JsonCollection struct {
	name    string
	kv      *KV
	mu      sync.Mutex
	opts    *JsonCollectionOptions
	getStmt *gosqlite.Stmt
	putStmt *gosqlite.Stmt
	delStmt *gosqlite.Stmt
}

type JsonCollectionValue interface {
	SetRowId(id int64)
}

func NewJsonCollection(kv *KV, name string, opts *JsonCollectionOptions) (col *JsonCollection, err error) {
	if opts == nil {
		opts = &JsonCollectionOptions{
			Delimiter: "/",
			Columns:   nil,
		}
	}

	if opts.Delimiter == "" {
		opts.Delimiter = "/"
	}

	col = &JsonCollection{
		name: name,
		kv:   kv,
		opts: opts,
	}

	err = col.init()
	if err != nil {
		col = nil
	}

	return
}

func (c *JsonCollection) init() (err error) {
	genCols := strings.Builder{}
	for _, col := range c.opts.Columns {
		genCols.WriteString(fmt.Sprintf(", %s %s as (%s) %s\n", col.Name, col.Type, col.Def, col.Storage))
	}
	createSql := fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
		id integer primary key,
		key text not null unique,
		level integer,
		val text not null
		%s
	)`, c.name, genCols.String())

	if err = c.kv.conn.Exec(createSql); err != nil {
		return
	}

	if err = c.kv.conn.Exec(fmt.Sprintf(`create index if not exists %s_level_idx on %s(level)`, c.name, c.name)); err != nil {
		return
	}

	for _, idx := range c.opts.Indexes {
		createIndexSql := fmt.Sprintf(`CREATE INDEX IF NOT EXISTS %s_%s_idx ON %s (%s)`, c.name, idx, c.name, idx)
		if err = c.kv.conn.Exec(createIndexSql); err != nil {
			return
		}
	}

	c.getStmt, err = c.kv.conn.Prepare(fmt.Sprintf("SELECT id, json(val) FROM %s WHERE key = ?", c.name))
	if err != nil {
		return
	}

	c.putStmt, err = c.kv.conn.Prepare(fmt.Sprintf(`INSERT INTO %s (key, level, val) VALUES (?, ?, jsonb(?))
	ON CONFLICT(key) DO UPDATE SET val = jsonb(excluded.val)`, c.name))
	if err != nil {
		return
	}

	c.delStmt, err = c.kv.conn.Prepare(fmt.Sprintf("DELETE FROM %s WHERE key = ?", c.name))
	if err != nil {
		return
	}

	return
}

func (c *JsonCollection) Get(ctx context.Context, key string, fn GetFn) (ok bool, err error) {
	c.kv.rw.RLock()
	ok, err = c.get(ctx, key, fn)
	if err = c.getStmt.ClearBindings(); err != nil {
		c.kv.rw.RUnlock()
		return
	}

	if err = c.getStmt.Reset(); err != nil {
		c.kv.rw.RUnlock()
		return
	}

	c.kv.rw.RUnlock()
	return
}

func (c *JsonCollection) get(ctx context.Context, key string, fn GetFn) (ok bool, err error) {
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

	err = fn(rid, rawBytes)

	return
}

func (c *JsonCollection) Put(ctx context.Context, key string, buf []byte) (rowid int64, err error) {
	c.kv.rw.Lock()
	rowid, err = c.put(ctx, key, buf)
	c.kv.rw.Unlock()
	return
}

func (c *JsonCollection) put(ctx context.Context, key string, buf []byte) (rowid int64, err error) {
	level := strings.Count(key, c.opts.Delimiter)
	err = c.putStmt.Exec(key, level, buf)
	if err != nil {
		return
	}

	rowid = c.kv.conn.LastInsertRowID()
	return
}

func (c *JsonCollection) List(ctx context.Context, keyPrefix string, fn ListFn, opts ListOptions) (err error) {
	if keyPrefix == "" {
		keyPrefix = "/"
	} else {
		if !strings.HasSuffix(keyPrefix, c.opts.Delimiter) {
			keyPrefix += c.opts.Delimiter
		}
	}

	c.kv.rw.Lock()
	err = c.list(ctx, keyPrefix, fn, opts)
	c.kv.rw.Unlock()

	return
}

func (c *JsonCollection) list(ctx context.Context, keyPrefix string, fn ListFn, opts ListOptions) (err error) {
	sql := strings.Builder{}
	sql.WriteString(fmt.Sprintf("select id, key, json(val) from %s", c.name))

	sql.WriteString(" where key like ?")

	level := strings.Count(keyPrefix, c.opts.Delimiter)
	sql.WriteString(fmt.Sprintf(" and level = %d", level))

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

	stmt, err := c.kv.conn.Prepare(sql.String())
	if err != nil {
		return
	}
	defer stmt.Close()

	keyPrefix += "%"
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
