package disk

import (
	"encoding/json"
	"io/ioutil"
	"os"
	"path/filepath"
	"strconv"

	p "github.com/stonelike/synchroDB/disk/page"
)

type MaxPid struct {
	Maxpid uint64 `json:maxpid`
}

//永続化処理,dirname,tablenamepageとpageIDがいる
type DiskManager struct {
}

func NewDiskManager() *DiskManager {
	return &DiskManager{}
}

func (d *DiskManager) ReadMaxPid(dirName, tableName string) (uint64, error) {
	path := filepath.Join(dirName, tableName)
	if _, err := os.Stat(path); err != nil {
		err := os.MkdirAll(path, 0777)
		if err != nil {
			panic(err)
		}
	}

	bytes, err := ioutil.ReadFile(path + "maxpid")
	if err != nil {
		return 0, err
	}

	var MaxPid = &MaxPid{}
	json.Unmarshal(bytes, MaxPid)

	return MaxPid.Maxpid, nil
}

func (d *DiskManager) WriteMaxPid(pageID uint64, dirName, tableName string) error {

	MaxPid := &MaxPid{
		Maxpid: pageID,
	}

	b, err := json.Marshal(MaxPid)
	if err != nil {
		return err
	}

	path := filepath.Join(dirName, tableName)

	if _, err := os.Stat(path); err != nil {
		err := os.MkdirAll(path, 0777)
		if err != nil {
			panic(err)
		}
	}
	return ioutil.WriteFile(path+"maxpid", b[:], 0644)
}

//ReadDiskのときは*Pageを返す
func (d *DiskManager) WriteDisk(dirName, tableName string, pageID uint64, page *p.Page) error {
	//dirName/tableName/pageIDにpageのbinaryを書き込む

	b, err := p.SerializePage(page)
	if err != nil {
		return err
	}
    
	path := filepath.Join(dirName, tableName)

	if _, err := os.Stat(path); err != nil {
		err := os.MkdirAll(path, 0777)
		if err != nil {
			panic(err)
		}
	}
    pageString := strconv.FormatUint(pageID, 10)
    path = filepath.Join(path,pageString)

	
	return ioutil.WriteFile(path, b[:], 0644)
}

func (d *DiskManager) ReadDisk(dirName, tableName string, pageID uint64) (*p.Page, error) {
	pageString := strconv.FormatUint(pageID, 10)
	path := filepath.Join(dirName, tableName,pageString)
	if _, err := os.Stat(path); err != nil {
		return nil, err
	}
	

	bytes, err := ioutil.ReadFile(path)
	if err != nil {
		return nil, err
	}

	var b [16384]byte
	copy(b[:], bytes)
	return p.DeserializePage(b)
}
