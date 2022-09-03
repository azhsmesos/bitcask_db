package bitcask_db

import (
	"fmt"
	"testing"
	"time"
)

func TestNewStoreFile(t *testing.T) {
	db, err := Open("miniDB")
	if err != nil {
		fmt.Println(err)
	}
	fmt.Println(db.storeFile, ", ", db.dirPath)
}

func TestDBPut(t *testing.T) {
	key := "key"
	value := "value"
	db, err := Open("miniDB")
	if err != nil {
		panic(err)
	}
	startTime := time.Now().Unix()
	for i := 0; i < 10000000; i++ {
		err := db.Put([]byte(key), []byte(value))
		if err != nil {
			panic(err)
		}
	}
	curTime := time.Duration(startTime)
	fmt.Println("time: ", curTime)
}

func TestDBGet(t *testing.T) {
	key := "key"
	db, err := Open("miniDB")
	if err != nil {
		panic(err)
	}
	value, err := db.Get([]byte(key))
	if err != nil {
		panic(err)
	}
	fmt.Println("value: ", string(value))
}

func TestDBMerge(t *testing.T) {
	db, err := Open("miniDB")
	if err != nil {
		panic(err)
	}
	err = db.MergeTree()
	if err != nil {
		panic(err)
	}
	fmt.Println("merge success")

}
