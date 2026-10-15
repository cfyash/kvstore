package kvstore

import (
	"io"
	"os"
	"slices"
	"strconv"
	"strings"
)

type datafile struct {
	id     int
	path   string
	offset int64
	file   *os.File
}

func (f *datafile) write(arr []byte) (int64, uint32, error) {
	n, err := f.file.WriteAt(arr, f.offset)
	if err != nil {
		return -1, 0, err
	}
	writeOffset := f.offset
	f.offset = f.offset + int64(n)
	return writeOffset, uint32(n), nil
}

func (f *datafile) read(offset int64, size uint32) ([]byte, error) {
	b := make([]byte, size)
	_, err := io.ReadFull(io.NewSectionReader(f.file, offset, int64(size)), b)
	return b, err
}

func openDataFile(path string, id int) (*datafile, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	return &datafile{
		id:     id,
		path:   path,
		offset: 0,
		file:   file,
	}, nil
}

func openActiveFile(path string, id int) (*datafile, error) {
	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return nil, err
	}
	return &datafile{
		id:     id,
		path:   path,
		offset: 0,
		file:   file,
	}, nil
}

func ensureDir(path string) error {
	return os.MkdirAll(path, 0755)
}

func listFileIDs(path string, ext string) ([]int, error) {
	entries, err := os.ReadDir(path)
	if err != nil {
		return nil, err
	}
	files := []int{}
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		filename := entry.Name()
		prefix, found := strings.CutSuffix(filename, ext)
		if !found {
			continue
		}
		num, err := strconv.Atoi(prefix)
		if err != nil {
			continue
		}
		files = append(files, num)
	}
	slices.Sort(files)
	return files, nil
}

func (f *datafile) close() {
	if f == nil {
		return
	}
	f.file.Close()
}

func removeFile(path string) {
	os.Remove(path)
}
