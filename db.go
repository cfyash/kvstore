package kvstore

import (
	"errors"
	"fmt"
	"io"
	"path/filepath"
	"slices"
	"strconv"
	"sync"
	"time"
)

type DB struct {
	memstore     *keydir
	activeFileID int
	path         string
	files        map[int]*datafile
	mu           sync.RWMutex
	mergeMu      sync.Mutex
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

func (db *DB) Write(key string, value []byte) error {
	if len(value) == 0 {
		return ErrEmptyValue
	}
	record := newRecord(uint32(time.Now().Unix()), key, value)
	arr := record.encode()
	db.mu.Lock()
	defer db.mu.Unlock()
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
	db.mu.RLock()
	defer db.mu.RUnlock()
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
	db.mu.Lock()
	defer db.mu.Unlock()
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

func (db *DB) scanFile(file *datafile, op func(rec *record, offset int64, size uint32, id int) error) (int64, error) {
	offset := int64(0)
	for {
		h, err := file.read(offset, 16)
		if errors.Is(err, io.EOF) {
			return offset, nil
		}
		if err != nil {
			return offset, err
		}
		chksum, timestamp, keySize, valueSize := decodeHeader(h)
		b, err := file.read(offset+16, keySize+valueSize)
		if err != nil {
			if errors.Is(err, io.EOF) {
				return offset, io.ErrUnexpectedEOF
			}
			return offset, err
		}
		record, err := decodeBody(chksum, timestamp, keySize, valueSize, b)
		if err != nil {
			return offset, err
		}
		err = op(record, offset, uint32(16+len(b)), file.id)
		if err != nil {
			return offset, err
		}
		offset += int64(16 + len(b))
	}
}

func (db *DB) merge() error {
	ok := db.mergeMu.TryLock()
	if !ok {
		return ErrParallelMergeRunning
	}
	defer db.mergeMu.Unlock()
	db.mu.RLock()
	toBeMerged := make([]int, 0, len(db.files))
	for fileID, _ := range db.files {
		if fileID == db.activeFileID {
			continue
		}
		toBeMerged = append(toBeMerged, fileID)
	}
	db.mu.RUnlock()
	if len(toBeMerged) == 0 {
		return nil
	}
	slices.Sort(toBeMerged)
	mergeFileId := toBeMerged[0]
	tempKeyDir := newMergeKeyDir()
	tempFilePath := filepath.Join(db.path, "tempMerge"+extension)
	tempFile, err := openMergeFile(tempFilePath, mergeFileId)
	if err != nil {
		return err
	}
	defer func() {
		tempFile.close()
		removeFile(tempFilePath)
	}()
	op := func(rec *record, offset int64, size uint32, id int) error {
		db.mu.RLock()
		defer db.mu.RUnlock()
		if rec.tombstone {
			return nil
		}
		entry, err := db.memstore.read(string(rec.key))
		if err != nil && errors.Is(err, ErrNotFound) {
			return nil
		}
		if id == entry.fileID && offset == entry.offset && entry.size == size && entry.timestamp == rec.timestamp {
			writeOffset, _, err := tempFile.write(rec.encode())
			if err != nil {
				return err
			}
			tempKeyDir.put(string(rec.key), tempFile.id, writeOffset, size, rec.timestamp, entry)
		}
		return nil
	}
	files := make([]*datafile, len(toBeMerged))
	db.mu.RLock()
	for i := range len(files) {
		files[i] = db.files[toBeMerged[i]]
	}
	db.mu.RUnlock()
	for _, file := range files {
		_, err := db.scanFile(file, op)
		if err != nil {
			return err
		}
	}
	db.mu.Lock()
	defer db.mu.Unlock()
	destFile := db.files[mergeFileId]
	destPath := destFile.path
	err = move(tempFile, destFile)
	if err != nil {
		return err
	}
	roTempFile, err := openDataFile(destPath, mergeFileId)
	if err != nil {
		return err
	}
	db.files[mergeFileId] = roTempFile
	db.memstore.copyMergeKeyDir(tempKeyDir)
	for _, id := range toBeMerged {
		if id == mergeFileId {
			continue
		}
		f := db.files[id]
		delete(db.files, id)
		f.close()
		removeFile(f.path)
	}
	return nil
}

func (db *DB) rebuild(fileIDs []int) error {
	op := func(rec *record, offset int64, size uint32, id int) error {
		if rec.tombstone {
			db.memstore.delete(string(rec.key))
		} else {
			db.memstore.put(string(rec.key), id, offset, size, rec.timestamp)
		}
		return nil
	}
	for _, id := range fileIDs {
		file := db.files[id]
		offset, err := db.scanFile(file, op)
		if err != nil {
			if !errors.Is(err, ErrCorruptRecord) && !errors.Is(err, io.ErrUnexpectedEOF) {
				return err
			}

			if id != db.activeFileID {
				return err
			}

			if err := file.file.Truncate(offset); err != nil {
				return err
			}
		}
		if id == db.activeFileID {
			file.offset = offset
		}
	}
	return nil
}
