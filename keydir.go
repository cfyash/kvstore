package kvstore

type keydir struct {
	store map[string]entry
}

func newKeyDir() *keydir {
	return &keydir{make(map[string]entry)}
}

type entry struct {
	fileID    int
	offset    int64
	size      uint32
	timestamp uint32
}

func newEntry(fileID int, offset int64, size uint32, timestamp uint32) entry {
	return entry{
		fileID:    fileID,
		offset:    offset,
		size:      size,
		timestamp: timestamp,
	}
}

func (k *keydir) put(key string, fileID int, offset int64, size uint32, timestamp uint32) {
	k.store[key] = newEntry(fileID, offset, size, timestamp)
}

func (k *keydir) delete(key string) {
	delete(k.store, key)
}

func (k *keydir) read(key string) (entry, error) {
	entr, ok := k.store[key]
	if !ok {
		return entry{}, ErrNotFound
	}
	return entr, nil
}
