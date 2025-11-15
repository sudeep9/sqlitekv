package sqlitekv

import (
	"context"
	"fmt"
)

func (c *Collection) createFTSTable() (err error) {
	err = c.kv.conn.Exec(fmt.Sprintf(`create virtual table if not exists %s_ft using fts5(
	   col,
	   val,
	   content='',
	   content_rowid='id',
	   contentless_delete=1,
	   tokenize='trigram'
	)`, c.name))
	if err != nil {
		return
	}

	err = c.kv.conn.Exec(fmt.Sprintf(`
		CREATE TRIGGER IF NOT EXISTS %[1]s_ai AFTER INSERT ON %[1]s BEGIN
			INSERT INTO %[1]s_ft(rowid, val) VALUES (new.id, json_remove(new.val, '$._m'));
		END;

		CREATE TRIGGER IF NOT EXISTS %[1]s_ad AFTER DELETE ON %[1]s BEGIN
			DELETE FROM %[1]s_ft WHERE rowid = old.id;
		END;

		CREATE TRIGGER IF NOT EXISTS %[1]s_au AFTER UPDATE ON %[1]s BEGIN
			update %[1]s_ft set val = json_remove(new.val, '$._m') where rowid = old.id;
		END;
	`, c.name))

	return
}

func (c *Collection) Search(ctx context.Context, query string, rowfn RowFn) (err error) {
	/*
		select c.id, c.key, json(c.val)
		from patient c join patient_ft f on c.id = f.rowid where f.val match '14699'
	*/
	sql := fmt.Sprintf(`
		select c.id, c.key, json(c.val) 
		from %[1]s c join %[1]s_ft f on c.id = f.rowid where f.val match '%[2]s'`,
		c.name, query)

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
