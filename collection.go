package sqlitekv

import (
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

type CollectionOptions struct {
	Delimiter string
	Columns   []GeneratedColumn
	Indexes   []string
	FTS       bool
}

type Collection struct {
	name        string
	kv          *KV
	mu          sync.Mutex
	opts        *CollectionOptions
	getStmt     *gosqlite.Stmt
	getByIdStmt *gosqlite.Stmt
	insStmt     *gosqlite.Stmt
	updByIdStmt *gosqlite.Stmt
	putStmt     *gosqlite.Stmt
	delStmt     *gosqlite.Stmt
}

func newCollection(kv *KV, name string, opts *CollectionOptions) (col *Collection, err error) {
	if opts == nil {
		opts = &CollectionOptions{
			Delimiter: "/",
			Columns:   nil,
		}
	}

	if opts.Delimiter == "" {
		opts.Delimiter = "/"
	}

	col = &Collection{
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

func (c *Collection) init() (err error) {
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

	genColList := strings.Builder{}
	for _, col := range c.opts.Columns {
		genColList.WriteString(", ")
		genColList.WriteString(col.Name)
	}

	c.getStmt, err = c.kv.conn.Prepare(fmt.Sprintf("SELECT id, json(val) %s FROM %s WHERE key = ?", genColList.String(), c.name))
	if err != nil {
		return
	}

	c.getByIdStmt, err = c.kv.conn.Prepare(fmt.Sprintf("SELECT key, json(val) FROM %s WHERE id = ?", c.name))
	if err != nil {
		return
	}

	c.insStmt, err = c.kv.conn.Prepare(fmt.Sprintf(`INSERT INTO %s (id, key, level, val) VALUES (?, ?, ?, jsonb(?))`, c.name))
	if err != nil {
		return
	}

	c.putStmt, err = c.kv.conn.Prepare(fmt.Sprintf(`INSERT INTO %s (key, level, val) VALUES (?, ?, jsonb(?))
	ON CONFLICT(key) DO UPDATE SET val = jsonb(excluded.val)`, c.name))
	if err != nil {
		return
	}

	c.updByIdStmt, err = c.kv.conn.Prepare(fmt.Sprintf(`UPDATE %s SET val = jsonb(?) WHERE id = ?`, c.name))
	if err != nil {
		return
	}

	c.delStmt, err = c.kv.conn.Prepare(fmt.Sprintf("DELETE FROM %s WHERE key = ?", c.name))
	if err != nil {
		return
	}

	if c.opts.FTS {
		err = c.createFTSTable()
		if err != nil {
			return
		}
	}

	return
}

func (c *Collection) getGeneratedColumnSelect() string {
	genColList := strings.Builder{}
	for _, col := range c.opts.Columns {
		genColList.WriteString(", ")
		genColList.WriteString(col.Name)
	}
	return genColList.String()
}
