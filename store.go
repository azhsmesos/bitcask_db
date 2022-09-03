package bitcask_db

import "os"

const (
	Filename      = "storeDB.data"
	MergeFilename = "storeDB_merge.data"
)

type StoreFile struct {
	File   *os.File
	Offset int64
}

/**
O_RDONLY	打开只读文件
O_WRONLY	打开直接文件
O_RDWR	打开既可以读取又可以写入文件
O_APPEND	写入文件时将数据追加到文件尾部
O_CREATE	如果文件不存在则创建一个新的文件
O_EXCL	文件必须不存在，然后会创建一个新的文件
O_SYNC	打开同步I/O
O_TRUNC	文件打开时可以截断

0644:  -rw-r--r--
*/
func newInternal(fileName string) (*StoreFile, error) {
	file, err := os.OpenFile(fileName, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, err
	}
	stat, err := os.Stat(fileName)
	if err != nil {
		return nil, err
	}
	return &StoreFile{
		File:   file,
		Offset: stat.Size(),
	}, nil
}

func NewDBFile(path string) (*StoreFile, error) {
	filename := path + string(os.PathSeparator) + Filename
	return newInternal(filename)
}

func NewMergeFile(path string) (*StoreFile, error) {
	filename := path + string(os.PathSeparator) + MergeFilename
	return newInternal(filename)
}

func (file *StoreFile) Read(offset int64) (entry *Entry, err error) {
	// 读取metadata
	buf := make([]byte, entryHeaderSize)
	if _, err = file.File.ReadAt(buf, offset); err != nil {
		return
	}

	if entry, err = Decode(buf); err != nil {
		return
	}
	offset += entryHeaderSize
	// 读取key
	if entry.KeySize > 0 {
		key := make([]byte, entry.KeySize)
		if _, err := file.File.ReadAt(key, offset); err != nil {
			return nil, err
		}
		entry.Key = key
	}
	offset += int64(entry.KeySize)
	// 读取value
	if entry.ValueSize > 0 {
		value := make([]byte, entry.ValueSize)
		if _, err = file.File.ReadAt(value, offset); err != nil {
			return
		}
		entry.Value = value
	}
	return
}

func (file *StoreFile) Write(entry *Entry) (err error) {
	entryData, err := entry.Encode()
	if err != nil {
		return
	}
	_, err = file.File.WriteAt(entryData, file.Offset)
	file.Offset += entry.GetSize()
	return
}
