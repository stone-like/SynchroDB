package disk

import (
	"testing"

	p "github.com/stonelike/synchroDB/disk/page"
	tup "github.com/stonelike/synchroDB/disk/tuple"
)

const (
	dirName   = `C:\Users\user\Desktop\DB`
	tableName = `tempTable`
)

func TestWriteAndReadMaxPid(t *testing.T) {
	disk := NewDiskManager()

	var maxpid uint64 = 1
	disk.WriteMaxPid(maxpid, dirName, tableName)

	readpid, err := disk.ReadMaxPid(dirName, tableName)
	if err != nil {
		t.Error("error")
	}
	if readpid != 1 {
		t.Error("error")
	}
}
func TestWriteAndReadPage(t *testing.T) {
	disk := NewDiskManager()

	var pageID uint64 = 1
	page := p.NewPage()
	var tupleData = []interface{}{"go", "tuple"}
	tuple := tup.NewTuple(tupleData)
	page.Tuples[0] = *tuple
	disk.WriteDisk(dirName, tableName, pageID, page)

	page, err := disk.ReadDisk(dirName, tableName, pageID)
	if err != nil {
		t.Errorf("error %s", err)
	}

	if page.Tuples[0].Data[0].String_ != "go" {
		t.Errorf("accutual: %s,want:%s", page.Tuples[0].Data[0].String_, "go")
	}
}
