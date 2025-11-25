package sqlitekv

import (
	"database/sql"
	"fmt"
	"strings"
)

type TableField struct {
	Name       string
	Type       string
	PrimaryKey bool
	Indexed    bool
	Nullable   bool
	Unique     bool
}

type TableOptions struct {
	Fields []TableField
}

type Table struct {
	Name      string
	opts      TableOptions
	db        *sql.DB
	stmtStore *StmtStore
}

func NewTable(db *sql.DB, name string, opts TableOptions) (t *Table, err error) {
	t = &Table{
		Name:      name,
		opts:      opts,
		db:        db,
		stmtStore: NewStmtStore(),
	}

	err = t.init()
	if err != nil {
		return
	}

	return
}

func (t *Table) init() (err error) {
	err = t.createTable()
	if err != nil {
		return
	}

	err = t.createIndexes()
	if err != nil {
		return
	}

	return
}

func (t Table) StmtStore() *StmtStore {
	return t.stmtStore
}

func (t *Table) createTable() (err error) {
	s := strings.Builder{}
	s.WriteString("CREATE TABLE IF NOT EXISTS ")
	s.WriteString(t.Name)
	s.WriteString(" (")

	for i, field := range t.opts.Fields {
		if i > 0 {
			s.WriteString(", ")
		}
		s.WriteString(field.Name)
		s.WriteString(" ")
		s.WriteString(field.Type)
		if field.PrimaryKey {
			s.WriteString(" PRIMARY KEY")
		} else {
			if !field.Nullable {
				s.WriteString(" NOT NULL")
			}
			if field.Unique {
				s.WriteString(" UNIQUE")
			}
		}
	}
	s.WriteString(")")

	fmt.Println("create table:", s.String())

	_, err = t.db.Exec(s.String())
	return
}

func (t *Table) createIndexes() (err error) {
	for _, f := range t.opts.Fields {
		if !f.Indexed {
			continue
		}

		indexName := fmt.Sprintf("%s_%s_idx", t.Name, f.Name)
		s := fmt.Sprintf("CREATE INDEX IF NOT EXISTS %s ON %s (%s)", indexName, t.Name, f.Name)
		_, err = t.db.Exec(s)
		if err != nil {
			return
		}
	}
	return
}

func (t *Table) Row(stmtName string, getSql func() string, bindargs []any, args ...any) (ok bool, err error) {
	stmt, err := t.stmtStore.GetOrCreate(t.db, stmtName, getSql)
	if err != nil {
		return
	}

	row := stmt.QueryRow(bindargs...)
	err = row.Scan(args...)
	if err != nil {
		if err == sql.ErrNoRows {
			err = nil
		}
		return
	}

	ok = true
	return
}

func (t *Table) Insert(args ...any) (rid int64, err error) {
	stmt, err := t.stmtStore.GetOrCreate(t.db, "insert", func() string {
		s := strings.Builder{}
		s.WriteString("INSERT INTO ")
		s.WriteString(t.Name)
		s.WriteString(" (")

		for i, f := range t.opts.Fields {
			if i > 0 {
				s.WriteString(", ")
			}
			s.WriteString(f.Name)
		}

		s.WriteString(") VALUES (")
		for i := range t.opts.Fields {
			if i > 0 {
				s.WriteString(", ")
			}
			s.WriteString("?")
		}
		s.WriteString(")")

		return s.String()
	})
	if err != nil {
		return
	}

	res, err := stmt.Exec(args...)
	if err != nil {
		return
	}

	rid, err = res.LastInsertId()
	return
}

func (t *Table) Upsert(args ...any) (rid int64, err error) {
	stmt, err := t.stmtStore.GetOrCreate(t.db, "upsert", func() string {
		s := strings.Builder{}
		s.WriteString("INSERT INTO ")
		s.WriteString(t.Name)
		s.WriteString(" (")

		for i, f := range t.opts.Fields {
			if i > 0 {
				s.WriteString(", ")
			}
			s.WriteString(f.Name)
		}

		s.WriteString(") VALUES (")
		for i := range t.opts.Fields {
			if i > 0 {
				s.WriteString(", ")
			}
			s.WriteString("?")
		}
		s.WriteString(")")
		s.WriteString(" ON CONFLICT(")

		first := true
		for _, f := range t.opts.Fields {
			if f.PrimaryKey {
				if !first {
					first = false
					s.WriteString(", ")
				}
				s.WriteString(f.Name)
			}
		}

		s.WriteString(") DO UPDATE SET ")

		first = true
		for _, f := range t.opts.Fields {
			if !f.PrimaryKey && !f.Unique {
				if !first {
					s.WriteString(",")
				}
				s.WriteString(" ")
				s.WriteString(f.Name)
				s.WriteString("=excluded.")
				s.WriteString(f.Name)
				first = false
			}
		}

		return s.String()
	})
	if err != nil {
		return
	}

	res, err := stmt.Exec(args...)
	if err != nil {
		return
	}

	rid, err = res.LastInsertId()
	return
}

func (t *Table) Update(updateSql string, args ...any) (affectedCount int64, err error) {
	res, err := t.db.Exec(updateSql, args...)
	if err != nil {
		return
	}

	affectedCount, err = res.RowsAffected()
	return
}

func (t *Table) Select(selectSql string, bindargs []any, handleRow func(row *sql.Rows) error) (err error) {
	rows, err := t.db.Query(selectSql, bindargs...)
	if err != nil {
		return
	}
	defer rows.Close()

	for rows.Next() {
		err = handleRow(rows)
		if err != nil {
			return
		}
	}

	return
}

func (t *Table) SelectUsingStmt(stmtName string, getSql func() string, bindargs []any, handleRow func(row *sql.Rows) error) (err error) {
	stmt, err := t.stmtStore.GetOrCreate(t.db, stmtName, getSql)
	if err != nil {
		return
	}

	rows, err := stmt.Query(bindargs...)
	if err != nil {
		return
	}
	defer rows.Close()

	for rows.Next() {
		err = handleRow(rows)
		if err != nil {
			return
		}
	}

	return
}
