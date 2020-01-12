package tuple

import (
	"github.com/gogo/protobuf/proto"
)

//column制限を８、stringの最大文字数を100として、alignmentを1024とする,今は制限かけてるけどもしoverしたらそのときにreturnしてerror返すようにする？
//もっと高度なやり方ならこのあたりいろいろ解決できるんだけど今回はとりあえず完成させたい
//overするやつに対しては代わりにテーブル拡張ページへのポインタを入れるとか、
//今回みたいにalignしないできっちりcolumn一つごとに計算してinlineかそうでないかをわけて取得するとか

func NewTuple(values []interface{}) *Tuple {
	var t Tuple
	t.UpdateNum = 1
	var tData *TupleData

	for _, v := range values {
		switch column := v.(type) {

		case int:

			tData = &TupleData{
				Type:   TupleData_INT,
				Number: *proto.Int32(int32(column)),
			}
		case string:
			tData = &TupleData{
				Type:    TupleData_STRING,
				String_: *proto.String(column),
			}
		}

		t.Data = append(t.Data, tData)
	}
	return &t
}

func SerializeTuple(t *Tuple) ([1024]byte, error) {
	buff, err := proto.Marshal(t)
	if err != nil {
		return [1024]byte{}, err
	}
	var b [1024]byte //1024byteにalign
	copy(b[:], buff)
	return b, nil
}

func DeserializeTuple(b [1024]byte) (*Tuple, error) {
	var t Tuple
	err := proto.Unmarshal(b[:], &t)
	//何も入っていないbyteをparseしようとするとerrorになる
	if err != nil {
		//エラーだったら空tupleを返せばいいか・・・？
		return &t, err
	}
	return &t, nil
}

// func TestTuple() {
// 	var testArray []*TupleData
// 	tempArray := []string{"magia", "bossgiant"}
// 	fmt.Print(tempArray)
// 	for _, v := range tempArray {
// 		testArray = append(testArray, &TupleData{
// 			Type:    TupleData_STRING, //6byte
// 			String_: *proto.String(v),
// 		})
// 	}
// 	testByte := &Tuple{
// 		MinTxId: 1, //2byte
// 		MaxTxId: 2, //2byte
// 		Data:    testArray,
// 	}

// 	buff, err := proto.Marshal(testByte)
// 	if err != nil {
// 		return
// 	}
// 	fmt.Print("serialized: ", len(buff)) //1000個でやったところ4874byteなので数次第では128byteを普通にoverする,column数は一つのTupleを128byte制限にすると対して取れない、30個取れればいいほうとなってしまう、でもcolumnがstringかintかでまた変わってしまうような・・・？
// 	//intの場合常に4byte
// 	//stringが文字数によって変化してしまう、一文字1byteかな100文字制限にして
// 	//column制限を8にすると最悪のケースだと全部stringの100文字で初期の4+（8×(100+6))で852byteが最悪のケース
// 	//pageを16kbとして＝16384byteなので・・・全然足りなくない？1pageに2つしかtupleおけないんだが・・・これ最悪のケースにalignするの・・・？,そんなことしたら相当な無駄遣いだけど・・・
// 	//そんな時に1pageの半分を超えるようだったらtoastとか使うみたい,なら結局alignするのはどれくらいにすればいいんだろ,とりあえず16384/16=1024なので1024bytealignするようにしてみた、なので1pageには最大16個tupleが入る

// }
