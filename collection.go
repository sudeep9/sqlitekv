package sqlitekv

import (
	"context"
	"fmt"

	"github.com/eatonphil/gosqlite"
)

type CollectionOptions struct {
	Delimiter byte
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
			Delimiter: '/',
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
	createSql := fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
		id integer primary key,
		key text not null unique,
		value blob not null
	)`, c.name)

	if err = c.kv.conn.Exec(createSql); err != nil {
		return
	}

	c.getStmt, err = c.kv.conn.Prepare(fmt.Sprintf("SELECT value FROM %s WHERE key = ?", c.name))
	if err != nil {
		return
	}

	c.putStmt, err = c.kv.conn.Prepare(fmt.Sprintf(`INSERT INTO %s (key, value) 
	VALUES (?, ?)
	ON CONFLICT(key) DO UPDATE SET value = excluded.value`, c.name))
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
	c.kv.mu.Lock()
	err = c.delete(ctx, key)
	c.kv.mu.Unlock()
	return
}

func (c *Collection) delete(ctx context.Context, key string) (err error) {
	err = c.delStmt.Exec(key)
	return
}
