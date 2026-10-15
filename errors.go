package kvstore

import "errors"

var (
	ErrNotFound            = errors.New("resource not found")
	ErrCorruptRecord error = errors.New("chksum match failed")
	ErrFileNotFound        = errors.New("file not found")
	ErrEmptyValue    error = errors.New("value is empty")
)
