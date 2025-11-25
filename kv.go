package sqlitekv

import (
	"database/sql"
	"fmt"
	"strings"
)

type KeyValField[T any] struct {
	Name     string
	Type     string
	Unique   bool
	Nullable bool
	Indexed  bool
	Get      func(t *T) any
	GetPtr   func(t *T) any
}

type KeyValOptions[T any] struct {
	KeyField    *KeyValField[T]
	Fields      []*KeyValField[T]
	Enc         *Encoder
	OnInsert    func(*T)
	OnUpdate    func(*T)
	Validate    func(*T) error
	Compression bool
	UseDict     bool
}

type KeyVal[T any] struct {
	db            *sql.DB
	opts          KeyValOptions[T]
	tab           *Table
	flagsField    *KeyValField[T]
	valField      *KeyValField[T]
	latestDictVer uint8
	encodeOpt     EncodeOptions
	decodeOpts    DecodeOptions
}

func NewKeyVal[T any](db *sql.DB, name string, opts KeyValOptions[T]) (kv *KeyVal[T], err error) {
	flagsField := &KeyValField[T]{
		Name: "flags",
		Type: "INTEGER",
	}
	valField := &KeyValField[T]{
		Name: "val",
		Type: "BLOB",
	}

	tableFields := make([]TableField, 0, len(opts.Fields)+2)
	tableFields = append(tableFields, TableField{
		Name:       opts.KeyField.Name,
		Type:       opts.KeyField.Type,
		Unique:     true,
		Nullable:   false,
		Indexed:    false,
		PrimaryKey: true,
	})

	tableFields = append(tableFields, TableField{
		Name: flagsField.Name,
		Type: flagsField.Type,
	})

	for _, f := range opts.Fields {
		tableFields = append(tableFields, TableField{
			Name:       f.Name,
			Type:       f.Type,
			Unique:     f.Unique,
			Nullable:   f.Nullable,
			Indexed:    f.Indexed,
			PrimaryKey: false,
		})
	}

	tableFields = append(tableFields, TableField{
		Name: valField.Name,
		Type: valField.Type,
	})

	tab, err := NewTable(db, name, TableOptions{
		Fields: tableFields,
	})
	if err != nil {
		return
	}

	kv = &KeyVal[T]{
		db:         db,
		opts:       opts,
		tab:        tab,
		flagsField: flagsField,
		valField:   valField,
	}

	if kv.opts.Compression && kv.opts.UseDict {
		dictColl := kv.opts.Enc.DictCollection()
		if dictColl == nil {
			err = fmt.Errorf("encoder must have a dict collection to use dictionary compression")
			return
		}

		kv.latestDictVer, err = dictColl.GetMaxVersion(kv.tab.Name)
		if err != nil {
			return
		}
	}

	if kv.latestDictVer == 0 && kv.opts.UseDict {
		kv.opts.UseDict = false
	}

	kv.encodeOpt = EncodeOptions{
		DictKey:  kv.tab.Name,
		Compress: kv.opts.Compression,
		UseDict:  kv.opts.UseDict,
		DictVer:  kv.latestDictVer,
	}

	kv.decodeOpts = DecodeOptions{
		DictKey: kv.tab.Name,
	}

	return
}

func (kv *KeyVal[T]) Table() *Table {
	return kv.tab
}

func (kv *KeyVal[T]) makeInsertArgs(flags int64, buf []byte, obj *T) (args []any) {
	args = make([]any, len(kv.opts.Fields)+3)
	args[0] = kv.opts.KeyField.Get(obj)
	args[1] = flags
	for i, field := range kv.opts.Fields {
		args[i+2] = field.Get(obj)
	}
	args[len(kv.opts.Fields)+2] = buf
	return
}

func (kv *KeyVal[T]) Get(pkey any, obj *T) (ok bool, err error) {
	return kv.getUnique(kv.opts.KeyField.Name, pkey, obj)
}

func (kv *KeyVal[T]) GetUnique(columnName string, pkey any, obj *T) (ok bool, err error) {
	return kv.getUnique(columnName, pkey, obj)
}

func (kv *KeyVal[T]) getUnique(columnName string, pkey any, obj *T) (ok bool, err error) {
	scanArgs := make([]any, len(kv.opts.Fields)+3)
	var flags int64
	var buf []byte

	scanArgs[0] = kv.opts.KeyField.GetPtr(obj)
	scanArgs[1] = &flags
	for i, field := range kv.opts.Fields {
		scanArgs[i+2] = field.GetPtr(obj)
	}
	scanArgs[len(kv.opts.Fields)+2] = &buf

	ok, err = kv.tab.Row(columnName, func() string {
		s := strings.Builder{}
		s.WriteString("SELECT ")
		s.WriteString(kv.opts.KeyField.Name)
		s.WriteString(", flags")

		for _, field := range kv.opts.Fields {
			s.WriteString(", ")
			s.WriteString(field.Name)
		}

		s.WriteString(", val")
		s.WriteString(" FROM ")
		s.WriteString(kv.tab.Name)
		s.WriteString(" WHERE flags & 1 = 0 AND ")
		s.WriteString(columnName)
		s.WriteString(" = ?")
		return s.String()
	}, []any{pkey}, scanArgs...)
	if !ok || err != nil {
		return
	}

	err = kv.opts.Enc.Decode(buf, obj, flags, kv.decodeOpts)
	if err != nil {
		return
	}

	return
}

func (kv *KeyVal[T]) Insert(obj *T) (rid int64, err error) {
	if kv.opts.Validate != nil {
		err = kv.opts.Validate(obj)
		if err != nil {
			return
		}
	}

	if kv.opts.OnInsert != nil {
		kv.opts.OnInsert(obj)
	}

	flags, buf, err := kv.opts.Enc.Encode(obj, kv.encodeOpt)
	if err != nil {
		return
	}

	args := kv.makeInsertArgs(flags, buf, obj)
	rid, err = kv.tab.Insert(args...)

	return
}

func (kv *KeyVal[T]) Upsert(obj *T) (err error) {
	if kv.opts.Validate != nil {
		err = kv.opts.Validate(obj)
		if err != nil {
			return
		}
	}

	if kv.opts.OnUpdate != nil {
		kv.opts.OnUpdate(obj)
	}

	flags, buf, err := kv.opts.Enc.Encode(obj, kv.encodeOpt)
	if err != nil {
		return
	}

	args := kv.makeInsertArgs(flags, buf, obj)
	_, err = kv.tab.Upsert(args...)

	return
}

type SelectOptions[T any] struct {
	StmtName string
	Where    string
	Limit    int
	Order    string
}

func (kv *KeyVal[T]) Select(bindargs []any, opts SelectOptions[T]) (list []*T, err error) {
	list = make([]*T, 0)

	getSql := func() string {
		s := strings.Builder{}
		s.WriteString("SELECT ")
		s.WriteString(kv.opts.KeyField.Name)
		s.WriteString(", flags")
		for _, f := range kv.opts.Fields {
			s.WriteString(", ")
			s.WriteString(f.Name)
		}
		s.WriteString(",val")
		s.WriteString(" FROM ")
		s.WriteString(kv.tab.Name)

		s.WriteString(" where")
		if opts.Where != "" {
			s.WriteString(" (")
			s.WriteString(opts.Where)
			s.WriteString(") AND")
		}
		s.WriteString(" flags & 1 = 0")

		if opts.Order != "" {
			s.WriteString(" ORDER BY ")
			s.WriteString(opts.Order)
		}
		if opts.Limit > 0 {
			s.WriteString(" LIMIT ")
			s.WriteString(fmt.Sprintf("%d", opts.Limit))
		}
		return s.String()
	}

	rowFn := func(rows *sql.Rows) (err error) {
		var flags int64
		var buf []byte
		scanArgs := make([]any, len(kv.opts.Fields)+3)
		obj := new(T)

		scanArgs[0] = kv.opts.KeyField.GetPtr(obj)
		scanArgs[1] = &flags
		for i, field := range kv.opts.Fields {
			scanArgs[i+2] = field.GetPtr(obj)
		}
		scanArgs[len(kv.opts.Fields)+2] = &buf

		err = rows.Scan(scanArgs...)
		if err != nil {
			return
		}

		err = kv.opts.Enc.Decode(buf, obj, flags, kv.decodeOpts)
		if err != nil {
			return
		}

		list = append(list, obj)
		return

	}

	if opts.StmtName == "" {
		err = kv.tab.Select(getSql(), bindargs, rowFn)
	} else {
		err = kv.tab.SelectUsingStmt(opts.StmtName, getSql, bindargs, rowFn)
	}

	return
}

func (kv *KeyVal[T]) SoftDelete(pkey any) (affectedCount int64, err error) {
	stmt, err := kv.tab.StmtStore().GetOrCreate(kv.db, "soft_delete_pkey", func() string {
		return fmt.Sprintf(`update %s set flags=flags | 1 where %s=? and flags & 1 = 0`,
			kv.tab.Name, kv.opts.KeyField.Name)
	})
	if err != nil {
		return
	}

	res, err := stmt.Exec(pkey)
	if err != nil {
		return
	}

	affectedCount, err = res.RowsAffected()
	return
}

func (kv *KeyVal[T]) Delete(pkey any) (affectedCount int64, err error) {
	stmt, err := kv.tab.StmtStore().GetOrCreate(kv.db, "delete_pkey", func() string {
		return fmt.Sprintf("delete from %s where %s=?", kv.tab.Name, kv.opts.KeyField.Name)
	})
	if err != nil {
		return
	}

	res, err := stmt.Exec(pkey)
	if err != nil {
		return
	}

	affectedCount, err = res.RowsAffected()

	return
}

func (kv *KeyVal[T]) Train(limit int) (err error) {
	selectSql := fmt.Sprintf("SELECT flags, val FROM %s WHERE (flags & 1 = 0) LIMIT %d",
		kv.tab.Name, limit)
	rows, err := kv.db.Query(selectSql)
	if err != nil {
		return
	}
	defer rows.Close()

	d, err := kv.opts.Enc.TrainWithRows(kv.db, kv.tab.Name, selectSql)
	if err != nil {
		return
	}

	kv.latestDictVer = d.Ver
	kv.encodeOpt.DictVer = d.Ver

	return
}
