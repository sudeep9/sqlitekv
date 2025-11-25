package sqlitekv

import (
	"database/sql"
	"fmt"
	"sync"

	"github.com/fxamacker/cbor/v2"
	"github.com/valyala/gozstd"
)

const (
	EncodeSoftDelete   = 0x1
	EncodeFlagCompress = 0x2
	EncodeFlagUseDict  = 0x4
	DictIDMask         = 0xff00
)

type EncodeOptions struct {
	Compress bool
	UseDict  bool
	DictKey  string
	DictVer  uint8
}

type DecodeOptions struct {
	DictKey string
}

func IsCompressed(flags int64) bool {
	return flags&EncodeFlagCompress != 0
}

func IsDictCompressed(flags int64) bool {
	return flags&EncodeFlagUseDict != 0
}

type ZstdDict struct {
	CDict *gozstd.CDict
	DDict *gozstd.DDict
}

type DictStore struct {
	rw    sync.RWMutex
	dicts map[string]map[uint8]*ZstdDict
}

func newDictStore() (s *DictStore) {
	s = &DictStore{
		dicts: make(map[string]map[uint8]*ZstdDict),
	}
	return
}

func (s *DictStore) getZstdDict(key string, ver uint8) (zdict *ZstdDict) {
	s.rw.RLock()
	verMap, ok := s.dicts[key]
	if !ok {
		s.rw.RUnlock()
		return
	}

	zdict, ok = verMap[ver]
	if ok {
		s.rw.RUnlock()
		return
	}

	s.rw.RUnlock()
	return
}

func (s *DictStore) GetCDict(key string, ver uint8) (cdict *gozstd.CDict) {
	zdict := s.getZstdDict(key, ver)
	if zdict == nil {
		return nil
	}

	return zdict.CDict
}

func (s *DictStore) GetDDict(key string, ver uint8) (ddict *gozstd.DDict) {
	zdict := s.getZstdDict(key, ver)
	if zdict == nil {
		return nil
	}

	return zdict.DDict
}

func (s *DictStore) SetDict(key string, ver uint8, buf []byte) (err error) {
	s.rw.Lock()
	cdict, err := gozstd.NewCDict(buf)
	if err != nil {
		s.rw.Unlock()
		return
	}

	ddict, err := gozstd.NewDDict(buf)
	if err != nil {
		s.rw.Unlock()
		return
	}

	zdict := &ZstdDict{
		CDict: cdict,
		DDict: ddict,
	}

	verMap, ok := s.dicts[key]
	if !ok {
		verMap = make(map[uint8]*ZstdDict)
		s.dicts[key] = verMap
	}
	verMap[ver] = zdict
	s.rw.Unlock()

	return
}

type Encoder struct {
	dictColl *DictCollection
	store    *DictStore
}

func NewEncoder(dictColl *DictCollection) (e *Encoder) {
	e = &Encoder{
		dictColl: dictColl,
		store:    newDictStore(),
	}
	return
}

func (e *Encoder) DictCollection() *DictCollection {
	return e.dictColl
}

func (e *Encoder) Encode(obj any, opts EncodeOptions) (flags int64, ebuf []byte, err error) {
	buf, err := cbor.Marshal(obj)
	if err != nil {
		return
	}

	if !opts.Compress {
		ebuf = buf
		return
	}

	if !opts.UseDict {
		flags |= EncodeFlagCompress
		ebuf = gozstd.Compress(nil, buf)
		return
	}

	if opts.DictVer == 0 || opts.DictKey == "" || e.dictColl == nil {
		err = fmt.Errorf("dict key or version must be specified for dictionary compression")
		return
	}

	cdict := e.store.GetCDict(opts.DictKey, opts.DictVer)
	if cdict == nil {
		ok, d, err := e.dictColl.Get(opts.DictKey, opts.DictVer)
		if err != nil {
			return 0, nil, err
		}
		if !ok {
			err = fmt.Errorf("no dictionary found for key: %s, ver=%d", opts.DictKey, opts.DictVer)
			return 0, nil, err
		}

		err = e.store.SetDict(opts.DictKey, opts.DictVer, d.Buf)
		if err != nil {
			return 0, nil, err
		}

		cdict = e.store.GetCDict(opts.DictKey, opts.DictVer)
	}

	flags |= EncodeFlagCompress
	flags |= EncodeFlagUseDict
	ver := int64(opts.DictVer) << 8
	flags = flags | ver
	ebuf = gozstd.CompressDict(nil, buf, cdict)

	return
}

func (e *Encoder) DecodeBuf(src []byte, flags int64, opts DecodeOptions) (buf []byte, err error) {
	if !IsCompressed(flags) {
		buf = src
		return
	}

	if !IsDictCompressed(flags) {
		buf, err = gozstd.Decompress(nil, src)
		if err != nil {
			return nil, err
		}
	}

	if e.dictColl == nil {
		err = fmt.Errorf("dictionary collection is not initialized")
		return nil, err
	}

	dictVer := uint8((flags >> 8) & 0xFF)
	if opts.DictKey == "" {
		err = fmt.Errorf("missing dict key for decoding")
		return
	}

	ddict := e.store.GetDDict(opts.DictKey, dictVer)

	if ddict == nil {
		ok, d, err := e.dictColl.Get(opts.DictKey, dictVer)
		if err != nil {
			return nil, err
		}
		if !ok {
			err = fmt.Errorf("no dictionary found for key: %s, ver=%d", opts.DictKey, dictVer)
			return nil, err
		}

		err = e.store.SetDict(opts.DictKey, dictVer, d.Buf)
		if err != nil {
			return nil, err
		}

		ddict = e.store.GetDDict(opts.DictKey, dictVer)
	}

	buf, err = gozstd.DecompressDict(nil, src, ddict)
	return
}

func (e *Encoder) Decode(src []byte, destObj any, flags int64, opts DecodeOptions) (err error) {
	if !IsCompressed(flags) {
		err = cbor.Unmarshal(src, destObj)
		return
	}

	buf, err := e.DecodeBuf(src, flags, opts)
	if err != nil {
		return
	}

	err = cbor.Unmarshal(buf, destObj)
	return
}

func (e *Encoder) train(key string, samples [][]byte) (d Dict, err error) {
	maxVer, err := e.dictColl.GetMaxVersion(key)
	if err != nil {
		return
	}

	d.Ver = maxVer + 1
	d.Buf = gozstd.BuildDict(samples, 112640)
	err = e.dictColl.Insert(key, d)
	return
}

func (e *Encoder) TrainWithRows(db *sql.DB, key string, selectSql string) (d Dict, err error) {
	rows, err := db.Query(selectSql)
	if err != nil {
		return d, err
	}
	defer rows.Close()

	var samples [][]byte
	for rows.Next() {
		var flags int64
		var val []byte
		err = rows.Scan(&flags, &val)
		if err != nil {
			return d, err
		}

		buf, err := e.DecodeBuf(val, flags, DecodeOptions{DictKey: key})
		if err != nil {
			return d, err
		}

		samples = append(samples, buf)
	}

	return e.train(key, samples)
}
