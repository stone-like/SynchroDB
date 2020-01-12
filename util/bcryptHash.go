package util

import (
	"crypto/md5"
	"encoding/binary"
)

//この問題点とは同じ物を使っても一意なkeyが生成されないこと、例えばtemptable0だったら毎回同じhashが生成されてくれないと困る
func ByteToUint64(hashbytes [16]byte) uint64 {
	var bytes []byte
	for _, v := range hashbytes {
		bytes = append(bytes, v)
	}

	i := binary.BigEndian.Uint64(bytes)

	return i
}

func Hash(pageID uint64, tableName string) uint64 {
	bytes := ConcatinatePageIDAndTableName(pageID, tableName)
	hash := md5.Sum(bytes)

	//byteからuint64にする、direcoryでのmaskのために必要

	return ByteToUint64(hash)
}
func ConcatinatePageIDAndTableName(pageID uint64, tableName string) []byte {
	tableBytes := []byte(tableName)
	bytes := make([]byte, 8)
	binary.BigEndian.PutUint64(bytes, pageID)
	tableBytes = append(tableBytes, bytes...)
	return tableBytes
}
