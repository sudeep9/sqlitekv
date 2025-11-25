package sqlitekv

import (
	"database/sql"
)

type Dict struct {
	Buf []byte
	Ver uint8
}

type DictCollection struct {
	db *sql.DB
}

func NewDictCollection(db *sql.DB) (c *DictCollection, err error) {
	c = &DictCollection{db: db}
	err = c.init()
	return
}

func (c *DictCollection) init() (err error) {
	_, err = c.db.Exec(`
	CREATE TABLE IF NOT EXISTS comp_dict (
		key TEXT NOT NULL,
		ver INTEGER NOT NULL,
		dict blob NOT NULL,
		PRIMARY KEY (key, ver)
	)`)
	if err != nil {
		return
	}

	return
}

func (c *DictCollection) Insert(key string, d Dict) (err error) {
	_, err = c.db.Exec(`INSERT INTO comp_dict (key, ver, dict) VALUES (?, ?, ?)
		ON CONFLICT(key, ver) DO UPDATE SET dict=excluded.dict`, key, d.Ver, d.Buf)
	return
}

func (c *DictCollection) Get(key string, ver uint8) (ok bool, dict Dict, err error) {
	row := c.db.QueryRow(`SELECT ver, dict FROM comp_dict WHERE key = ? AND ver = ?`, key, ver)
	err = row.Scan(&dict.Ver, &dict.Buf)
	if err == nil {
		ok = true
		return
	}

	if err == sql.ErrNoRows {
		err = nil
	}
	return
}

func (c *DictCollection) GetMaxVersion(key string) (maxVer uint8, err error) {
	var n *int64
	row := c.db.QueryRow(`SELECT MAX(ver) FROM comp_dict WHERE key = ?`, key)
	err = row.Scan(&n)
	if err == sql.ErrNoRows {
		err = nil
		return
	}
	if n != nil {
		maxVer = uint8(*n)
	}
	return
}

func (c *DictCollection) GetLatest(key string) (ok bool, d Dict, err error) {
	row := c.db.QueryRow(`SELECT ver, dict 
	FROM comp_dict 
	WHERE key = ? ORDER BY ver DESC LIMIT 1`, key)

	err = row.Scan(&d.Ver, &d.Buf)
	if err == nil {
		ok = true
		return
	}

	if err == sql.ErrNoRows {
		err = nil
	}
	return
}

func (c *DictCollection) DeleteDict(key string, ver int64) (err error) {
	_, err = c.db.Exec(`DELETE FROM comp_dict WHERE key = ? AND ver = ?`, key, ver)
	return
}

func (c *DictCollection) DeleteAllVersions(key string) (err error) {
	_, err = c.db.Exec(`DELETE FROM comp_dict WHERE key = ?`, key)
	return
}
