package kvstore

import "errors"

var (
	ErrNotFound             error = errors.New("resource not found")
	ErrNilError             error = errors.New("nil resource")
	ErrCorruptRecord        error = errors.New("chksum match failed")
	ErrFileNotFound         error = errors.New("file not found")
	ErrEmptyValue           error = errors.New("value is empty")
	ErrParallelMergeRunning error = errors.New("parallel merge running")
)
