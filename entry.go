package bitcask_db

import "encoding/binary"

const (
	entryHeaderSize        = 10
	PUT             uint16 = iota
	DEL
)

type Entry struct {
	Key       []byte
	Value     []byte
	KeySize   uint32
	ValueSize uint32
	Mark      uint16
}

func NewEntry(key, value []byte, mark uint16) *Entry {
	return &Entry{
		Key:       key,
		Value:     value,
		KeySize:   uint32(len(key)),
		ValueSize: uint32(len(value)),
		Mark:      mark,
	}
}

func (entry *Entry) GetSize() int64 {
	return int64(entryHeaderSize + entry.KeySize + entry.ValueSize)
}

func (entry *Entry) Encode() ([]byte, error) {
	buf := make([]byte, entry.GetSize())
	binary.BigEndian.PutUint32(buf[0:4], entry.KeySize)
	binary.BigEndian.PutUint32(buf[4:8], entry.ValueSize)
	binary.BigEndian.PutUint16(buf[8:10], entry.Mark)
	copy(buf[entryHeaderSize:entryHeaderSize+entry.KeySize], entry.Key)
	copy(buf[entryHeaderSize+entry.KeySize:], entry.Value)
	return buf, nil
}

func Decode(buf []byte) (*Entry, error) {
	keySize := binary.BigEndian.Uint32(buf[0:4])
	valueSize := binary.BigEndian.Uint32(buf[4:8])
	mark := binary.BigEndian.Uint16(buf[8:10])
	return &Entry{
		KeySize:   keySize,
		ValueSize: valueSize,
		Mark:      mark,
	}, nil
}
