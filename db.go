package kvstore

import (
	"errors"
	"fmt"
	"io"
	"path/filepath"
	"strconv"
	"time"
)

type DB struct {
	memstore     *keydir
	activeFileID int
	path         string
	files        map[int]*datafile
}

const extension string = ".dat"
const maxFileSize int64 = 1 << 20

func Open(path string) (*DB, error) {
	err := ensureDir(path)
	if err != nil {
		return nil, err
	}
	fileIDs, err := listFileIDs(path, extension)
	if err != nil {
		return nil, err
	}
	db, err := newDB(path, fileIDs)
	if err != nil {
		return nil, err
	}
	err = db.rebuild(fileIDs)
	if err != nil {
		return nil, err
	}
	return db, nil
}

func newDB(path string, fileIDs []int) (*DB, error) {
	var activeFileID int = 1
	if len(fileIDs) > 0 {
		activeFileID = fileIDs[len(fileIDs)-1]
	} else {
		fileIDs = append(fileIDs, activeFileID)
	}
	files := make(map[int]*datafile)
	for _, id := range fileIDs {
		joinedPath := filepath.Join(path, strconv.Itoa(id)+extension)
		var file *datafile
		var err error
		if activeFileID != id {
			file, err = openDataFile(joinedPath, id)
		} else {
			file, err = openActiveFile(joinedPath, id)
		}
		if err != nil {
			return nil, err
		}
		files[id] = file
	}
	return &DB{
		memstore:     newKeyDir(),
		activeFileID: activeFileID,
		path:         path,
		files:        files,
	}, nil
}

func (db *DB) rebuild(fileIDs []int) error {
	for _, id := range fileIDs {
		file := db.files[id]
		var offset int64 = 0
		for {
			h, err := file.read(offset, 16)
			if errors.Is(err, io.EOF) {
				break
			}
			if errors.Is(err, io.ErrUnexpectedEOF) {
				if id == db.activeFileID {
					break
				} else {
					return err
				}
			}
			if err != nil {
				return err
			}
			chksum, timestamp, keySize, valueSize := decodeHeader(h)
			b, err := file.read(offset+16, keySize+valueSize)
			if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
				if id == db.activeFileID {
					break
				} else {
					return err
				}
			}
			if err != nil {
				return err
			}
			record, err := decodeBody(chksum, timestamp, keySize, valueSize, b)
			if err != nil {
				if id == db.activeFileID {
					break
				} else {
					return err
				}
			}
			if record.tombstone {
				db.memstore.delete(string(record.key))
			} else {
				db.memstore.put(string(record.key), id, offset, uint32(16+len(b)), record.timestamp)
			}
			offset += int64(16 + len(b))
		}
		if id == db.activeFileID {
			file.offset = offset
			err := file.file.Truncate(file.offset)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func (db *DB) Write(key string, value []byte) error {
	if len(value) == 0 {
		return ErrEmptyValue
	}
	record := newRecord(uint32(time.Now().Unix()), key, value)
	arr := record.encode()
	activeFile := db.files[db.activeFileID]
	writeOffset, writtenSize, err := activeFile.write(arr)
	if err != nil {
		return err
	}
	db.memstore.put(key, db.activeFileID, writeOffset, writtenSize, record.timestamp)
	err = db.rotate()
	if err != nil {
		fmt.Printf("error in file rotation: %s\n", err)
	}
	return nil
}

func (db *DB) Read(key string) ([]byte, error) {
	entry, err := db.memstore.read(key)
	if err != nil {
		return nil, err
	}
	file, ok := db.files[entry.fileID]
	if !ok {
		return nil, ErrFileNotFound
	}
	b, err := file.read(entry.offset, entry.size)
	if err != nil {
		return nil, err
	}
	record, err := decode(b)
	if err != nil {
		return nil, err
	}
	return record.value, nil
}

func (db *DB) Delete(key string) error {
	record := newRecord(uint32(time.Now().Unix()), key, []byte{})
	arr := record.encode()
	activeFile := db.files[db.activeFileID]
	_, _, err := activeFile.write(arr)
	if err != nil {
		return err
	}
	db.memstore.delete(key)
	err = db.rotate()
	if err != nil {
		fmt.Printf("error in file rotation: %s\n", err)
	}
	return nil
}

func (db *DB) rotate() (err error) {
	activeFile := db.files[db.activeFileID]
	if activeFile.offset < maxFileSize {
		return
	}
	newFileId := activeFile.id + 1
	newFilePath := filepath.Join(db.path, strconv.Itoa(newFileId)+extension)
	newFile, err := openActiveFile(newFilePath, newFileId)
	var roFile *datafile
	if err != nil {
		return
	}
	defer func() {
		if err == nil {
			return
		}
		newFile.close()
		removeFile(newFilePath)
		if roFile != nil {
			roFile.close()
		}
	}()
	roFile, err = openDataFile(activeFile.path, activeFile.id)
	if err != nil {
		return
	}
	db.files[newFileId] = newFile
	db.activeFileID = newFileId
	db.files[activeFile.id] = roFile
	activeFile.close()
	return
}
