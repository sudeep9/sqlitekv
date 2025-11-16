package sqlitekv

import (
	"fmt"
	"strings"
	"sync"

	"github.com/eatonphil/gosqlite"
)

type CollectionType interface {
	GetId() (id int64)
	SetId(id int64)
	GetVal() (val []byte, err error)
	SetVal(val []byte) error
	Column(i int, name string) (ok bool, val any, err error)
	SetColumn(i int, name string, ok bool, val any) error
}

type DerivedColumn struct {
	Name     string
	Type     string
	Nullable bool
	Unique   bool
}

type CollectionOptions struct {
	Columns []DerivedColumn
	Indexes []string
	AutoId  bool
	Json    bool
	FTS     *FTSOptions
}

type Collection struct {
	name    string
	kv      *KV
	mu      sync.Mutex
	opts    *CollectionOptions
	getStmt *gosqlite.Stmt
	insStmt *gosqlite.Stmt
	putStmt *gosqlite.Stmt
	delStmt *gosqlite.Stmt

	uniqueStmt map[string]*gosqlite.Stmt
}

func NewCollection(kv *KV, name string, opts *CollectionOptions) (col *Collection, err error) {
	if opts == nil {
		opts = &CollectionOptions{
			Columns: nil,
		}
	}

	col = &Collection{
		name:       name,
		kv:         kv,
		opts:       opts,
		uniqueStmt: make(map[string]*gosqlite.Stmt),
	}

	err = col.init()
	if err != nil {
		col = nil
	}

	return
}

func (c *Collection) init() (err error) {
	fmt.Printf("creating table\n")
	err = c.createTable()
	if err != nil {
		return
	}

	fmt.Printf("creating indexes\n")
	err = c.createIndexes()
	if err != nil {
		err = fmt.Errorf("create indexes failed: %w", err)
		return
	}

	fmt.Printf("preparing get statement\n")
	err = c.prepareGetStmt()
	if err != nil {
		err = fmt.Errorf("prepare get statement failed: %w", err)
		return
	}

	fmt.Printf("preparing unique statements\n")
	err = c.prepareUniqueStmts()
	if err != nil {
		err = fmt.Errorf("prepare unique statement failed: %w", err)
		return
	}

	fmt.Printf("preparing insert statement\n")
	err = c.prepareInsertStmt()
	if err != nil {
		err = fmt.Errorf("prepare insert statement failed: %w", err)
		return
	}

	fmt.Printf("preparing put statement\n")
	err = c.preparePutStmt()
	if err != nil {
		err = fmt.Errorf("prepare put statement failed: %w", err)
		return
	}

	fmt.Printf("preparing delete statement\n")
	err = c.prepareDeleteStmt()
	if err != nil {
		return
	}

	if c.opts.FTS != nil {
		err = c.createFTSTable()
		if err != nil {
			return
		}
	}

	return
}

func (c *Collection) createTable() (err error) {
	sql := strings.Builder{}

	sql.WriteString("create table if not exists ")
	sql.WriteString(c.name)
	sql.WriteString(" (\n")

	sql.WriteString("id integer primary key,\n")
	sql.WriteString("val text not null\n")

	for _, col := range c.opts.Columns {
		prop := ""
		if !col.Nullable {
			prop += " not null"
		}
		if col.Unique {
			prop += " unique"
		}

		sql.WriteString(fmt.Sprintf(",%s %s %s\n", col.Name, col.Type, prop))
	}

	sql.WriteString(")")

	fmt.Printf("create table: %s\n", sql.String())
	if err = c.kv.conn.Exec(sql.String()); err != nil {
		return
	}

	return
}

func (c *Collection) createIndexes() (err error) {
	for _, colName := range c.opts.Indexes {
		createIndexSql := fmt.Sprintf(`CREATE INDEX IF NOT EXISTS %[1]s_%[2]s_idx ON %[1]s (%[2]s)`, c.name, colName)
		fmt.Printf("index: %s", createIndexSql)
		if err = c.kv.conn.Exec(createIndexSql); err != nil {
			return
		}
	}
	return
}

func (c *Collection) makeGetStmt(colName string) (stmt *gosqlite.Stmt, err error) {
	sql := strings.Builder{}
	sql.WriteString("SELECT id, val")
	for _, col := range c.opts.Columns {
		sql.WriteString(", ")
		sql.WriteString(col.Name)
	}
	sql.WriteString(" FROM ")
	sql.WriteString(c.name)
	sql.WriteString(" WHERE ")
	sql.WriteString(colName)
	sql.WriteString(" = ?")

	stmt, err = c.kv.conn.Prepare(sql.String())
	if err != nil {
		return
	}
	return
}

func (c *Collection) prepareGetStmt() (err error) {
	c.getStmt, err = c.makeGetStmt("id")
	return
}

func (c *Collection) prepareUniqueStmts() (err error) {
	for _, col := range c.opts.Columns {
		if !col.Unique {
			continue
		}

		stmt, err := c.makeGetStmt(col.Name)
		if err != nil {
			return err
		}

		c.uniqueStmt[col.Name] = stmt
	}
	return
}

func (c *Collection) prepareInsertStmt() (err error) {
	insertSql := strings.Builder{}
	insertSql.WriteString("INSERT INTO ")
	insertSql.WriteString(c.name)
	insertSql.WriteString(" (id,val")
	for _, col := range c.opts.Columns {
		insertSql.WriteString(",")
		insertSql.WriteString(col.Name)
	}
	insertSql.WriteString(") VALUES (?,?")
	for range c.opts.Columns {
		insertSql.WriteString(",?")
	}
	insertSql.WriteString(")")

	fmt.Printf("insert sql: %s\n", insertSql.String())
	c.insStmt, err = c.kv.conn.Prepare(insertSql.String())
	if err != nil {
		return
	}

	return
}

func (c *Collection) preparePutStmt() (err error) {
	fmt.Println("preparing put statement")
	sql := strings.Builder{}
	sql.WriteString("INSERT INTO ")
	sql.WriteString(c.name)
	sql.WriteString(" (id, val")
	for _, col := range c.opts.Columns {
		sql.WriteString(",")
		sql.WriteString(col.Name)
	}
	sql.WriteString(") VALUES (?,?")
	for range c.opts.Columns {
		sql.WriteString(",?")
	}
	sql.WriteString(")")

	sql.WriteString(" ON CONFLICT(id) DO UPDATE SET val=excluded.val")
	for _, col := range c.opts.Columns {
		sql.WriteString(", ")
		sql.WriteString(col.Name)
		sql.WriteString("=excluded.")
		sql.WriteString(col.Name)
	}

	c.putStmt, err = c.kv.conn.Prepare(sql.String())

	return
}

func (c *Collection) prepareDeleteStmt() (err error) {
	c.delStmt, err = c.kv.conn.Prepare(fmt.Sprintf("DELETE FROM %s WHERE id = ?", c.name))
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
