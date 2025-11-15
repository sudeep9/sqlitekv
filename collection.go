package sqlitekv

import (
	"context"
	"fmt"

	"github.com/eatonphil/gosqlite"
)

type CollectionOptions struct {
	Delimiter string
}

type Collection struct {
	name    string
	opts    *CollectionOptions
	kv      *KV
	getStmt *gosqlite.Stmt
	putStmt *gosqlite.Stmt
	delStmt *gosqlite.Stmt
}

func newCollection(kv *KV, name string, opts *CollectionOptions) (col *Collection, err error) {
	if opts == nil {
		opts = &CollectionOptions{
			Delimiter: "/",
		}
	}
	col = &Collection{
		name: name,
		opts: opts,
		kv:   kv,
	}
	err = col.init()
	return
}

func (c *Collection) init() (err error) {
	levelField := ""
	if c.opts.Delimiter != "" {
		levelField = "level integer,"
	}
	createSql := fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
		id integer primary key,
		key text not null unique,
		%s
		value blob not null
	)`, c.name, levelField)

	if err = c.kv.conn.Exec(createSql); err != nil {
		return
	}

	if c.opts.Delimiter != "" {
		createIndexSql := fmt.Sprintf(`CREATE INDEX IF NOT EXISTS %s_level_idx ON %s (level)`, c.name, c.name)
		if err = c.kv.conn.Exec(createIndexSql); err != nil {
			return
		}
	}

	c.getStmt, err = c.kv.conn.Prepare(fmt.Sprintf("SELECT value FROM %s WHERE key = ?", c.name))
	if err != nil {
		return
	}

	if c.opts.Delimiter != "" {
		c.putStmt, err = c.kv.conn.Prepare(fmt.Sprintf(`INSERT INTO %s (key, level, value) 
		VALUES (?, ?, ?)
		ON CONFLICT(key) DO UPDATE SET value = excluded.value`, c.name))
	} else {
		c.putStmt, err = c.kv.conn.Prepare(fmt.Sprintf(`INSERT INTO %s (key, value) 
		VALUES (?, ?)
		ON CONFLICT(key) DO UPDATE SET value = excluded.value`, c.name))
	}

	if err != nil {
		return
	}

	c.delStmt, err = c.kv.conn.Prepare(fmt.Sprintf("DELETE FROM %s WHERE key = ?", c.name))
	if err != nil {
		return
	}

	return
}

func (c *Collection) Delete(ctx context.Context, key string) (err error) {
	c.kv.rw.Lock()
	err = c.delete(ctx, key)
	c.kv.rw.Unlock()
	return
}

func (c *Collection) delete(ctx context.Context, key string) (err error) {
	err = c.delStmt.Exec(key)
	return
}
