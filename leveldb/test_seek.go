package main

import (
	"fmt"
	"github.com/GalaIO/bsc-tools/utils"
	"github.com/ethereum/go-ethereum/ethdb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/iterator"
)

func main() {
	db, err := leveldb.New("./testdb", 1000, 10, "test", false)
	utils.PanicErr(err)
	defer db.Close()

	putKeyVal(db, "key11", "shrink1")
	putKeyVal(db, "key111", "val11")
	putKeyVal(db, "key112", "val12")
	putKeyVal(db, "key121", "val13")
	putKeyVal(db, "key122", "val14")

	putKeyVal(db, "key21", "shrink2")
	putKeyVal(db, "key211", "val21")
	putKeyVal(db, "key212", "val22")
	putKeyVal(db, "key221", "val23")
	putKeyVal(db, "key222", "val24")

	// seeker
	seekNextKV(db, []byte("key"), []byte(""))
	seekNextKV(db, []byte("key1"), []byte(""))
	seekNextKV(db, []byte("key2"), []byte(""))
	seekNextKV(db, []byte("key1"), []byte("11"))
	seekNextKV(db, []byte("key1"), []byte("111"))
	seekNextKV(db, []byte("key1"), []byte("1111"))
	seekKV(db, []byte("key1"), []byte("222"))
}

func putKeyVal(db *leveldb.Database, key, val string) {
	//db.Put([]byte(key), []byte(val))
	db.Put(expandKey([]byte(key)), []byte(val))
}

func expandKey(raw []byte) []byte {
	if len(raw) < 6 {
		ret := []byte("FFFFFF")
		copy(ret, raw)
		return ret
	}
	return raw
}

func seekNextKV(db *leveldb.Database, prefix []byte, start []byte) {
	fmt.Println("start seek", "prefix", string(prefix), "start", string(start))
	iter1 := db.NewIterator(prefix, start)
	iter := iter1.(iterator.Iterator)
	defer iter.Release()
	for iter.Next() {
		fmt.Println("iter next", string(iter.Key()), string(iter.Value()))
	}
	//for iter.Prev() {
	//	fmt.Println("iter prev", string(iter.Key()), string(iter.Value()))
	//}
	fmt.Println("----------------------------------------")
}

func seekKV(db *leveldb.Database, prefix []byte, start []byte) {
	fmt.Println("start seek", "prefix", string(prefix), "start", string(start))
	iter1 := db.NewIterator(prefix, nil)
	iter := iter1.(iterator.Iterator)
	defer iter.Release()
	fmt.Println("iter seek", iter.Seek(append(prefix, start...)), string(iter.Key()), string(iter.Value()))
	fmt.Println("iter prev", iter.Prev(), string(iter.Key()), string(iter.Value()))
	//for iter.Next() {
	//	fmt.Println("iter next", string(iter.Key()), string(iter.Value()))
	//}
	//for iter.Prev() {
	//	fmt.Println("iter prev", string(iter.Key()), string(iter.Value()))
	//}
	fmt.Println("----------------------------------------")
}
