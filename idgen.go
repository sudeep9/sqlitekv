package sqlitekv

import (
	"crypto/rand"
	"encoding/binary"
)

func GenerateID64() int64 {
	var b [8]byte
	if _, err := rand.Read(b[3:]); err != nil {
		panic(err)
	}
	n := int64(binary.BigEndian.Uint64(b[:]))
	n = n & 0x000000FFFFFFFFFF
	//return int64(binary.LittleEndian.Uint64(b[:]) & 0x7FFFFFFFFFFFFFFF)
	return n
}
