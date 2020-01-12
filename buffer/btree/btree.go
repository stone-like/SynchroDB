package btree

//btreeのルールとしてnodeの要素はm-1まで、rootを除いて枝は(m+1)/2個以上
// const (
// 	degree   = 3 //=maxChild
// 	minChild = (degree + 1) / 2
// 	maxItem  = degree - 1
// )

import (
	"fmt"
	"sort"
)

type Item interface {
	Less(than Item) bool
}

type Int int

func (a Int) Less(b Item) bool {
	return a < b.(Int)
}

type Items []Item //これをレシーバーにとるときは(i　*Items)で*iのように利用する

func (s *Items) Insert(item Item, index int) {
	//→に従って大きくなるように並べないといけない

	//まだ空きがあるとき
	(*s) = append((*s), nil)
	(*s)[index] = item
}

func (s *Items) FindIndex(item Item) (int, bool) {
	//sortはitemsから持ってきたitemより大きいやつの最小indexを返す,で返ってきたslice[i] ==23ならすでに存在している
	i := sort.Search(len(*s), func(i int) bool { return item.Less((*s)[i]) })

	//boolはもうitemsに存在してる時true
	//itemにequalをつけるか、下のようにするか
	if i > 0 && !(*s)[i-1].Less(item) {
		return i - 1, true
	}
	return i, false
}

//NodeはItemとChildrenを持つ

type Node struct {
	items    Items
	children Children
}

func NewNode() *Node {
	return &Node{}
}

func (n *Node) Insert(item Item, maxItem int) {

	//ここで入れるindexを探さないといけない、itemが入ってないnewNodeは０が返ってくる
	index, isAlreadyExit := n.items.FindIndex(item)
	//すでに存在するなら
	if isAlreadyExit {
		//update
		// oldItem := n.items[index]
		n.items[index] = item
		return
		// return oldItem
	}

	if len(n.children) != 0 {
		if len(n.children[index].items) == maxItem {
			n.SplitChild(maxItem, n.children[index])
			//split後なのでindexを再計算しないといけない
			index = n.recalucurateIndex(index, item)
		}

		n.children[index].Insert(item, maxItem)
		return
	}

	//もうchildがないならここに入れる
	n.items.Insert(item, index)
}

func (n *Node) recalucurateIndex(index int, item Item) int {
	fmt.Printf("index %d", index)
	bottomUpItem := n.items[index]
	switch {
	case item.Less(bottomUpItem):
		//そのまま
	case bottomUpItem.Less(item):
		index++
	}
	return index
}

func (n *Node) SplitChild(maxItem int, targetNode *Node) {
	i := maxItem / 2 //ここでsplit
	item := targetNode.items[i]
	nextNode := NewNode()
	//maxItemが２のとき、splitは1でするのでitems[2:]はoutofIndexErrorがでてしまう
	// if maxItem != 2 {
	// 	next.items = append(next.items, n.items[i+1:]...)
	// }
	//targetNodeを上書きする前にnextNodeにtargetNodeの情報を入れてあげないとだめ
	nextNode.items = append(nextNode.items, targetNode.items[i+1:]...)
	targetNode.items = targetNode.items[:i]

	if len(targetNode.children) > 0 {

		nextNode.children = append(nextNode.children, targetNode.children[i+1:]...)

		targetNode.children = targetNode.children[:i+1]
	}

	n.items = append(n.items, item)
	sort.Slice(n.items, func(i, j int) bool { return n.items[i].Less(n.items[j]) })

	//targetはもとからくっついてるので追加しなくていい
	n.children = append(n.children, nextNode)
	sort.Slice(n.children, func(i, j int) bool { return n.children[i].items[0].Less(n.children[j].items[0]) })
	//ここ追加した後にsortが要るっぽい

}

func (n *Node) Remove(item Item, minItem int) {
	//まずindexを取得
	index, itemInThisNode := n.items.FindIndex(item)

	if itemInThisNode {
		if len(n.children) == 0 {
			//このノードに存在していて、leafなら
			n.removeFromLeaf(index)
		} else {
			n.removeFromNonLeaf(index, minItem)
		}
		return
	}
	//このノードにないとき
	if len(n.children) == 0 {
		//leafならそもそもdelete対象が存在しないので終わり
		return
	}

	//もってきたindexがこのNodeのitemの一番最後か？最後ならflagがtrue
	//このflagはfillの後にitem総数が変化したか使う
	var flag bool
	if index == len(n.items) {
		flag = true
	} else {
		flag = false
	}

	if n.isChildFew(index, minItem) {
		//fewのとき
		n.Fill(index, minItem)
	}

	if flag && index > len(n.items) {
		//mergeされたなら一つ前のchildrenへ
		n.children[index-1].Remove(item, minItem)
	} else {
		n.children[index].Remove(item, minItem)

	}

	return
}

func (n *Node) isChildFew(index, minItem int) bool {
	return len(n.children[index].items) == minItem
}

func (n *Node) removeFromLeaf(index int) {
	n.items = DeleteItem(n.items, index)
}

func (n *Node) removeFromNonLeaf(index, minItem int) {

	item := n.items[index]

	if !n.isChildFew(index, minItem) {
		pred := n.getPred(index)
		n.items[index] = pred
		n.children[index].Remove(pred, minItem)

		return
	}

	if !n.isChildFew(index+1, minItem) {
		succ := n.getSucc(index)
		n.items[index] = succ
		n.children[index+1].Remove(succ, minItem)
		return
	}

	//どっちのchildrenもfewのとき,mergeの時は一応親がfewでも大丈夫だけどitemが一つになってかつ、子がfewの時はmerge後のchildを親に持ってこなければならず、removeも親でやり直し

	if len(n.items) == 1 {
		n.merge(index, minItem)
		n.Remove(item, minItem)
	} else {
		n.merge(index, minItem)
		n.children[index].Remove(item, minItem)
	}

	return
}

func (n *Node) getPred(index int) Item {
	cur := n.children[index]
	for !(len(cur.children) == 0) {
		cur = cur.children[len(cur.children)]
	}

	return cur.items[len(cur.items)-1]
}

func (n *Node) getSucc(index int) Item {
	cur := n.children[index+1]
	for !(len(cur.children) == 0) {
		cur = cur.children[0]
	}

	return cur.items[0]
}

//siblingからchildへのcopy
func (n *Node) merge(index, minItem int) {
	//mergeってことはchildもsiblingもminItem個しかない
	child := n.children[index]
	sibling := n.children[index+1]

	//mergeではこのnode.items[index]もmerge対象
	child.items = append(child.items, n.items[index])

	child.items = append(child.items, sibling.items[:]...)

	if len(sibling.children) != 0 {
		child.children = append(child.children, sibling.children[:]...)
	}
	//mergeした分、n.itemsとn.childrenをずらしていく、これによって親が消えてしまう時がある
	n.items = DeleteItem(n.items, index)
	n.children = DeleteChild(n.children, index+1)

	//親が消えてしまったときは
	if len(n.items) == 0 {
		//親が消えるときは形が親がitem一つのchild[0]のみと形が決まっている
		n.items = append(n.items, child.items...)
		tempChildren := child.children
		n.children = DeleteChild(n.children, 0)
		n.children = append(n.children, tempChildren...)

	}

}

func DeleteItem(s Items, i int) Items {
	s = append(s[:i], s[i+1:]...)
	n := make(Items, len(s))
	copy(n, s)
	return n
}

func DeleteChild(s Children, i int) Children {
	s = append(s[:i], s[i+1:]...)
	n := make(Children, len(s))
	copy(n, s)
	return n
}

func (n *Node) Fill(index, minItem int) {
	//fillをするのはこのNodeにdeleteするやつがなくてn.children[index]がfewのとき

	//prevChildから探す
	if index != 0 && !n.isChildFew(index-1, minItem) {
		n.borrowFromPrev(index)
		return
	}

	if index != len(n.items) && !n.isChildFew(index+1, minItem) {
		n.borrowFromNext(index)
		return
	}

	//前も次もfewだった時,mergeするなら端以外
	if index != len(n.items) {
		n.merge(index, minItem)
	} else {
		n.merge(index-1, minItem)
	}
}

func (n *Node) borrowFromPrev(index int) {
	child := n.children[index]
	sibling := n.children[index-1]

	lastSiblingItemIndex := len(sibling.items) - 1
	var lastSiblingChildIndex int
	if len(sibling.children) != 0 {
		lastSiblingChildIndex = len(sibling.children) - 1
	}

	child.items = append(child.items, n.items[index-1])
	sort.Slice(child.items, func(i, j int) bool { return child.items[i].Less(child.items[j]) })

	if len(sibling.children) != 0 {
		child.children = append(child.children, sibling.children[lastSiblingChildIndex])

		sort.Slice(child.children, func(i, j int) bool { return child.children[i].items[0].Less(child.children[j].items[0]) })
	}

	n.items[index-1] = sibling.items[lastSiblingItemIndex]

	//siblingのdeleteをして終わり
	sibling.items = DeleteItem(sibling.items, lastSiblingItemIndex)

	if len(sibling.children) != 0 {
		sibling.children = DeleteChild(sibling.children, lastSiblingChildIndex)
	}

}

func (n *Node) borrowFromNext(index int) {
	child := n.children[index]
	sibling := n.children[index+1]

	child.items = append(child.items, n.items[index])
	sort.Slice(child.items, func(i, j int) bool { return child.items[i].Less(child.items[j]) })

	if len(sibling.children) != 0 {
		child.children = append(child.children, sibling.children[0])

		sort.Slice(child.children, func(i, j int) bool { return child.children[i].items[0].Less(child.children[j].items[0]) })
	}

	n.items[index] = sibling.items[0]

	//siblingのdeleteをして終わり
	sibling.items = DeleteItem(sibling.items, 0)

	if len(sibling.children) != 0 {
		sibling.children = DeleteChild(sibling.children, 0)
	}

}

func (n *Node) Search(item Item) (Item, bool) {
	index, itemInThisNode := n.items.FindIndex(item)

	if itemInThisNode {
		return n.items[index], true
	}

	if len(n.children) == 0 {
		//このNodeにもなく、子供が存在していなかったら
		return nil, false
	}

	return n.children[index].Search(item)

}

type Children []*Node

// func (c *Children) Insert(index int, node *Node) {
// 	*c = append((*c), nil)
// 	if index < len(*c) {
// 		//挿入するchildとその隣をswapしてあげればいい,新しく拡張したところがindex+1でそこに挿入するやつのとなりを入れてあげる
// 		copy((*c)[index+1:], (*c)[index:])
// 	}
// 	(*c)[index] = node
// }

type BTree struct {
	degree int
	length int
	root   *Node
	// freelist []*Node
}

func (t *BTree) maxItem() int {
	return t.degree - 1
}

//rootを除くnodeの最低保持item
func (t *BTree) minItem() int {
	return t.minChild() - 1
}

func (t *BTree) minChild() int {
	return (t.degree + 1) / 2 //例えばdegが4だと2.5になってgolangでこれをsliceに入れると切り捨てされて2になるのでok
}
func (t *BTree) maxChild() int {
	return t.degree
}

func NewBTree(degree int) *BTree {
	return &BTree{
		degree: degree,
		root:   nil,
		length: 0,
	}
}

func (t *BTree) Search(item Item) (Item, bool) {
	if t.root == nil {
		return nil, false
	}

	return t.root.Search(item)
}

func (t *BTree) Insert(item Item) {
	if t.root == nil {
		//新しいnodeをrootに割り当ててそのnodeにinsert
		t.root = NewNode()
		t.root.items = append(t.root.items, item)
		t.length++
		return
	}
	//トップダウン式にfullを消していくことにする、insertするときにfullだったらsplitしてからinsert、これをnodeに当たるたびにやっていく

	if len(t.root.items) >= t.maxItem() {
		//split,どこでsplitするかなんだけど真ん中でsplitすればいいから
		s := NewNode()
		s.children = append(s.children, t.root)
		s.SplitChild(t.maxItem(), t.root)
		t.root = s

	}

	//rootのsplitが終わったら普通にinsert
	t.root.Insert(item, t.maxItem())

	t.length++

}

func (t *BTree) Remove(item Item) {
	if t.root == nil {
		return
	}

	//minItemはfewかどうかを判断するのに使う
	t.root.Remove(item, t.minItem())
	//rootが０itemになることはない気がするけど

}
