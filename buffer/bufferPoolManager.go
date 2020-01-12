package buffer

import (
	//page内でtupleをimportしてしまっているためtupleのままだとimportcollisionが起こってしまう

	lru "github.com/stonelike/synchroDB/buffer/lruReplacer"
	disk "github.com/stonelike/synchroDB/disk"
	p "github.com/stonelike/synchroDB/disk/page"
	"github.com/stonelike/synchroDB/disk/tuple"
	util "github.com/stonelike/synchroDB/util"
)

//ここは後でユーザーが作れるようにする
const (
	dirname   = `C:\Users\user\Desktop\DB`
	tableName = `tempTable`
)

//これexHashのcacheリストはそのままキャッシュだけど、別にsync.Poolでfreelistをつくってpage構造体を使いまわす？

//diskManager,logManagerをのちのち入れる,LRUを初期化すれば以下のexhashとかも初期化できる
type BufferPoolManager struct {
	lruCache    *lru.LRUCache
	diskManager *disk.DiskManager
	maxPidMap   map[string]uint64
}

func NewBufferPoolManager(cap uint64) *BufferPoolManager {

	//tableごとにmaxpidは違うので空ma@だけ用意しておいて、キャッシュとして使えるようにする
	return &BufferPoolManager{
		lruCache:    lru.NewLRU(cap),
		diskManager: disk.NewDiskManager(),
		maxPidMap:   make(map[string]uint64),
	}
}

func (b *BufferPoolManager) FetchAll(tableName string, wantEmpty bool) []*tuple.Tuple {
	tupleSlice := b.lruCache.GetAll(tableName, wantEmpty)
	return tupleSlice
}

func (b *BufferPoolManager) FetchPage(pageID uint64, tableName string) (*p.Page, error) {
	// uint64Key := util.Hash(pageID,tableName)

	pageD := b.lruCache.Get(pageID, tableName)
	//ここでcacheから帰ってきたらそのままpdからpageをとって返す
	if pageD != nil {
		//pinCountを増やす
		pd := pageD.(*p.PageDescriptor)
		return pd.Page, nil
	}
	//cacheにpdがなかった場合,diskManagerからpageをとってくる
	page, err := b.diskManager.ReadDisk(dirname, tableName, pageID)
	if err != nil {
		return nil, err
	}
	//とってきたpageをcacheにおいてあげる
	b.InsertPage(pageID, tableName, page)

	//pageを返す
	return page, nil

}

func (b *BufferPoolManager) InsertPage(pageID uint64, tableName string, page *p.Page) {
	// uint64Key := util.Hash(pageID,tableName)

	//まずpdを作ってPageとつなげる,isDirtyはtupleを入れたときにtrueになる
	pd := &p.PageDescriptor{
		IsDirty:   false,
		PageID:    pageID,
		Page:      page,
		TableName: tableName,
		PinCount:  0, //pinCountはreadしたときのみ,だけど同期制御してればよさそう?
	}
	//victimがあり、Dirtyならdiskに永続化する
	victim := b.lruCache.Put(pageID, tableName, pd)

	if victim != nil {
		victimPageD := victim.(*p.PageDescriptor)

		if victimPageD.IsDirty {

			victimTableName := victimPageD.TableName
			victimPageID := victimPageD.PageID
			victimPage := victimPageD.Page
			//diskへ
			b.diskManager.WriteDisk(dirname, victimTableName, victimPageID, victimPage)
		}
	}
	//maxPidがtable共通になってしまっているのがバグ、maxpidもtableごと
	b.maxPidMap[tableName]++
}

func (b *BufferPoolManager) InsertTuple(tableName string, t *tuple.Tuple) {
	//tupleの時は空いてるpageを探して、なければ新しくpageを作って入れるでいいかな？
	pInterface, number := b.lruCache.GetFreePD(tableName) //freepageをとってきて、tupleを追加してあげればいいかな？、あとdirtyflagを立ててあげる

	pd, ok := pInterface.(*p.PageDescriptor)

	if ok {
		pd.IsDirty = true

		pd.Page.Tuples[number] = *t

		//ここでpageがfullになったらfreemapからdelete
		isEmpty, _ := pd.Page.HasEmptyTuple()
		if !isEmpty {
			pageID := pd.PageID
			hashKey := util.Hash(pageID, tableName)
			b.lruCache.FreeMapDelete(tableName, hashKey)
		}
		return
	}

	//もしemptyがないならinsertPage
	page := p.NewPage()
	page.Tuples[0] = *t

	//もしtableごとのmaxPidが0でもしdiskにあったらdiskのmaxpidで上書き
	pageID := b.maxPidMap[tableName]

	if pageID == 0 {
		diskPageID, err := b.diskManager.ReadMaxPid(dirname, tableName)
		if err == nil {
			pageID = diskPageID
		}
	}
	b.InsertPage(pageID, tableName, page)

}

//exitの時にmaxpidを永続化するようにする
