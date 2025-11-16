package sqlitekv

import (
	"context"
	"fmt"
	"strings"
)

type FTSOptions struct {
	Columns     []string
	ExcludeKeys []string
}

func (c *Collection) createFTSTable() (err error) {
	fmt.Println("creating FTS virtual table and triggers")
	columnListStr := ""
	for _, col := range c.opts.FTS.Columns {
		columnListStr += col + ","
	}

	err = c.kv.conn.Exec(fmt.Sprintf(`create virtual table if not exists %[1]s_ft using fts5(
	   %[2]s
	   val,
	   content='',
	   content_rowid='id',
	   contentless_delete=1,
	   tokenize='trigram'
	)`, c.name, columnListStr))
	if err != nil {
		return
	}

	insertColListStr := ""
	for _, col := range c.opts.FTS.Columns {
		insertColListStr += fmt.Sprintf(",new.%s", col)
	}

	valStr := "new.val"

	if len(c.opts.FTS.ExcludeKeys) > 0 {
		valStr = "json_remove(new.val"
		for _, key := range c.opts.FTS.ExcludeKeys {
			valStr += fmt.Sprintf(",'$.%s'", key)
		}
		valStr += ")"
	}

	updStr := ""
	for _, col := range c.opts.FTS.Columns {
		updStr += fmt.Sprintf(", %[1]s=old.%[1]s", col)
	}

	err = c.kv.conn.Exec(fmt.Sprintf(`
		CREATE TRIGGER IF NOT EXISTS %[1]s_ai AFTER INSERT ON %[1]s BEGIN
			INSERT INTO %[1]s_ft(rowid, %[5]s val) VALUES (new.id %[2]s, %[3]s);
		END;

		CREATE TRIGGER IF NOT EXISTS %[1]s_ad AFTER DELETE ON %[1]s BEGIN
			DELETE FROM %[1]s_ft WHERE rowid = old.id;
		END;

		CREATE TRIGGER IF NOT EXISTS %[1]s_au AFTER UPDATE ON %[1]s BEGIN
			update %[1]s_ft set val = %[3]s %[4]s where rowid = old.id;
		END;
	`, c.name, insertColListStr, valStr, updStr, columnListStr))

	return
}

func (c *Collection) Search(ctx context.Context, query string, rowfn RowFn) (err error) {
	colListStr := strings.Builder{}
	for _, col := range c.opts.Columns {
		colListStr.WriteString(", c.")
		colListStr.WriteString(col.Name)
	}

	sql := fmt.Sprintf(`
		select c.id, c.val %[3]s
		from %[1]s c join %[1]s_ft f on c.id = f.rowid where f.val match '%[2]s'`,
		c.name, query, colListStr.String())

	err = c.kv.withReadLock(func() error {
		return c.selectNoLock(ctx, sql, rowfn)
	})
	if err != nil {
		return
	}

	return
}

/*

create virtual table if not exists ft using fts5 (
	val,
	content='',
	content_rowid='id',
	contentless_delete=1,
	tokenize='trigram'
);

CREATE TRIGGER gen_ai AFTER INSERT ON gen BEGIN
  INSERT INTO ft(rowid, val) VALUES (new.id, new.val);
END;

CREATE TRIGGER gen_ad AFTER DELETE ON gen BEGIN
  DELETE FROM ft WHERE rowid = old.id;
END;

CREATE TRIGGER gen_au AFTER UPDATE ON gen BEGIN
  update ft set val = new.val where rowid = old.id;
END;

*/
