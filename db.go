package bitcask_db

import (
	"fmt"
	"io"
	"os"
	"sync"
)

type BitCaskDB struct {
	indexMap  map[string]int64
	storeFile *StoreFile
	dirPath   string
	rwm       sync.RWMutex
}

func Open(dirPath string) (*BitCaskDB, error) {
	if _, err := os.Stat(dirPath); os.IsNotExist(err) {
		if err := os.MkdirAll(dirPath, os.ModePerm); err != nil {
			return nil, err
		}
	}

	storeFile, err := NewDBFile(dirPath)
	if err != nil {
		return nil, err
	}

	db := &BitCaskDB{
		indexMap:  make(map[string]int64),
		storeFile: storeFile,
		dirPath:   dirPath,
	}

	db.loadIndex()
	return db, err
}

func (db *BitCaskDB) loadIndex() {
	if db.storeFile == nil {
		fmt.Println("the storeFile is nil")
		return
	}

	var offset int64
	for {
		entry, err := db.storeFile.Read(offset)
		if err != nil {
			if err == io.EOF {
				break
			}
			return
		}
		db.indexMap[string(entry.Key)] = offset
		if entry.Mark == DEL {
			delete(db.indexMap, string(entry.Key))
		}
		offset += entry.GetSize()
	}
	return
}

func (db *BitCaskDB) Put(key []byte, Value []byte) (err error) {
	if len(key) == 0 {
		return
	}

	db.rwm.Lock()
	defer db.rwm.Unlock()
	offset := db.storeFile.Offset
	entry := NewEntry(key, Value, PUT)
	err = db.storeFile.Write(entry)
	db.indexMap[string(key)] = offset
	return
}

func (db *BitCaskDB) Get(key []byte) (value []byte, err error) {
	if len(key) == 0 {
		return
	}
	db.rwm.RLock()
	defer db.rwm.RUnlock()

	offset, ok := db.indexMap[string(key)]
	if !ok {
		return
	}
	var entry *Entry
	entry, err = db.storeFile.Read(offset)
	if err != nil && err != io.EOF {
		return
	}
	if entry != nil && entry.Mark != DEL {
		value = entry.Value
	}
	return
}

func (db *BitCaskDB) Del(key []byte) (err error) {
	if len(key) == 0 {
		return
	}

	db.rwm.Lock()
	defer db.rwm.Unlock()
	_, ok := db.indexMap[string(key)]
	if !ok {
		return
	}
	entry := NewEntry(key, nil, DEL)
	err = db.storeFile.Write(entry)
	if err != nil {
		return
	}
	delete(db.indexMap, string(key))
	return
}

// 模拟rosedb的reclaim 个人实现较为简单
func (db *BitCaskDB) MergeTree() error {
	if db.storeFile.Offset == 0 {
		return nil
	}
	var (
		validEntris []*Entry
		offset      int64 = 0
	)
	for {
		entry, err := db.storeFile.Read(offset)
		if err != nil {
			if err == io.EOF {
				break
			}
			return err
		}
		if nowOffset, ok := db.indexMap[string(entry.Key)]; ok && nowOffset == offset {
			validEntris = append(validEntris, entry)
		}
		offset += entry.GetSize()
	}
	if len(validEntris) > 0 {
		mergeFile, err := NewMergeFile(db.dirPath)
		if err != nil {
			return err
		}
		defer os.Remove(mergeFile.File.Name())
		for _, entry := range validEntris {
			writeOffset := mergeFile.Offset
			err := mergeFile.Write(entry)
			if err != nil {
				return err
			}
			db.indexMap[string(entry.Key)] = writeOffset
		}
		// copy
		storeFileName := db.storeFile.File.Name()
		db.storeFile.File.Close()
		os.Remove(storeFileName)

		mergeFileName := mergeFile.File.Name()
		mergeFile.File.Close()
		os.Rename(mergeFileName, db.dirPath+string(os.PathSeparator)+Filename)
		db.storeFile = mergeFile
	}
	return nil
}
