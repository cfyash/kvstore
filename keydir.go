package kvstore

type keydir struct {
	store map[string]entry
}

type mergekeydir struct {
	store map[string]mergeEntry
}

func newKeyDir() *keydir {
	return &keydir{make(map[string]entry)}
}

type mergeEntry struct {
	originalEntry entry
	newEntry      entry
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

func newMergeEntry(fileID int, offset int64, size uint32, timestamp uint32, originalEntry entry) mergeEntry {
	return mergeEntry{
		originalEntry: originalEntry,
		newEntry:      newEntry(fileID, offset, size, timestamp),
	}
}

func newMergeKeyDir() *mergekeydir {
	return &mergekeydir{
		store: make(map[string]mergeEntry),
	}
}

func (k *keydir) put(key string, fileID int, offset int64, size uint32, timestamp uint32) {
	k.store[key] = newEntry(fileID, offset, size, timestamp)
}

func (k *mergekeydir) put(key string, fileID int, offset int64, size uint32, timestamp uint32, originalEntry entry) {
	k.store[key] = newMergeEntry(fileID, offset, size, timestamp, originalEntry)
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

func (k *keydir) copyMergeKeyDir(mk *mergekeydir) {
	for key, mEntry := range mk.store {
		entry, ok := k.store[key]
		if !ok {
			continue
		}
		if entry.fileID == mEntry.originalEntry.fileID &&
			entry.offset == mEntry.originalEntry.offset &&
			entry.timestamp == mEntry.originalEntry.timestamp &&
			entry.size == mEntry.originalEntry.size {
			k.store[key] = mEntry.newEntry
		}
	}
}
