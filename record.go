package kvstore

import (
	"encoding/binary"
	"hash/crc32"
)

type record struct {
	timestamp uint32
	key       []byte
	value     []byte
	tombstone bool
}

func newRecord(timestamp uint32, key string, value []byte) *record {
	return &record{
		timestamp: timestamp,
		key:       []byte(key),
		value:     value,
		tombstone: len(value) == 0,
	}
}

func (r *record) encode() []byte {
	key, value, timestamp := r.key, r.value, r.timestamp
	toMerge := make([][]byte, 0, 6)
	toMerge = append(toMerge, toBytes(createChksum(toBytes(timestamp), toBytes(uint32(len(key))), toBytes(uint32(len(value))), key, value)))
	toMerge = append(toMerge, toBytes(uint32(timestamp)))
	toMerge = append(toMerge, toBytes(uint32(len(key))))
	toMerge = append(toMerge, toBytes(uint32(len(value))))
	toMerge = append(toMerge, key)
	toMerge = append(toMerge, value)
	return mergeArrays(toMerge, 16+len(key)+len(value))
}

func decode(b []byte) (*record, error) {
	chksum, timestamp, keySize, valueSize := decodeHeader(b)
	return decodeBody(chksum, timestamp, keySize, valueSize, b[16:])
}

func decodeBody(chksum, timestamp, keySize, valueSize uint32, b []byte) (*record, error) {
	key := b[0:keySize]
	value := b[keySize:]
	newChksum := createChksum(toBytes(timestamp), toBytes(uint32(len(key))), toBytes(uint32(len(value))), key, value)
	if chksum != newChksum {
		return nil, ErrCorruptRecord
	}
	return newRecord(timestamp, string(key), value), nil
}

func decodeHeader(arr []byte) (uint32, uint32, uint32, uint32) {
	chksum := toUint32(arr[0:4])
	timestamp := toUint32(arr[4:8])
	keySize := toUint32(arr[8:12])
	valueSize := toUint32(arr[12:16])
	return chksum, timestamp, keySize, valueSize
}

func createChksum(timestamp []byte, keySize []byte, valueSize []byte, key []byte, value []byte) uint32 {
	hasher := crc32.NewIEEE()
	hasher.Write(timestamp)
	hasher.Write(keySize)
	hasher.Write(valueSize)
	hasher.Write(key)
	hasher.Write(value)
	return hasher.Sum32()
}

func toBytes(a uint32) []byte {
	arr := make([]byte, 4)
	binary.LittleEndian.PutUint32(arr, a)
	return arr
}

func toUint32(b []byte) uint32 {
	return binary.LittleEndian.Uint32(b)
}

func mergeArrays(arrays [][]byte, totalLength int) []byte {
	result := make([]byte, totalLength)
	offset := 0
	for _, arr := range arrays {
		offset += copy(result[offset:], arr)
	}
	return result
}
