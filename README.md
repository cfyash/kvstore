# kvstore

A key-value store written in Go, modelled on [Bitcask](https://riak.com/assets/bitcask-intro.pdf), with a TCP server that speaks the Redis protocol (RESP). You can talk to it with `redis-cli` and benchmark it with `redis-benchmark`.

This is a learning project, built to understand log-structured storage, crash recovery, network framing and Go concurrency from first principles. It uses only the standard library.

## Quick start

```sh
go run ./cmd/kv
# 2026/10/17 00:00:00 listening on 127.0.0.1:6380
```

In another terminal:

```sh
redis-cli -p 6380
127.0.0.1:6380> PING
PONG
127.0.0.1:6380> SET foo bar
OK
127.0.0.1:6380> GET foo
"bar"
127.0.0.1:6380> DEL foo
(integer) 1
127.0.0.1:6380> GET foo
(nil)
```

Data is stored in `./data`. Stop the server with Ctrl+C. It finishes in-flight commands, syncs the active file and exits. A second Ctrl+C kills it immediately.

## Commands

| Command | Reply |
| --- | --- |
| `PING` | `+PONG` |
| `SET key value` | `+OK` |
| `GET key` | bulk string, or `$-1` (nil) if missing |
| `DEL key` | `:1` if deleted, `:0` if missing |

Command names are case-insensitive. Only these four commands exist, and a command may have at most 3 parts.

## How storage works

```text
              in memory                                on disk (data/)
  ┌────────────────────────────────┐      ┌───────────┐ ┌───────────┐ ┌──────────────┐
  │ keydir  (map[string]entry)     │      │  1.dat    │ │  2.dat    │ │  3.dat       │
  │  "foo" → {file 3, off 120, …}  │─────▶│ read-only │ │ read-only │ │ active (rw)  │
  │  "bar" → {file 1, off 0,   …}  │      └───────────┘ └───────────┘ └──────────────┘
  └────────────────────────────────┘                                    appends go here
```

- **Append-only log.** Every `SET` and `DEL` appends a record to the active data file. Nothing on disk is ever overwritten in place.
- **keydir.** An in-memory hash map from each live key to the file id, offset, size and timestamp of its latest record. A `GET` is one map lookup plus one positioned read (`ReadAt`).
- **Deletes** append a *tombstone*, which is a record with an empty value, and remove the key from the keydir. Because an empty value means "deleted", empty values can't be stored.
- **Rotation.** When the active file reaches 1 MiB (`maxFileSize`), it is reopened read-only and a new active file `N+1.dat` is created.
- **Recovery.** `Open` scans every data file in id order and replays the records into the keydir. A torn or corrupt record at the tail of the *active* file, which is the expected result of a crash mid-write, is truncated away. Corruption in an older, immutable file is an error.
- **Merge (compaction).** A background goroutine wakes every 10 minutes (`mergeInterval`). If there are at least 3 immutable files (`mergeThreshold`), it copies only the live records from all of them into one file and swaps that in under the name of the lowest-id file. Each copied record is checked against the keydir entry it was copied from, so writes that land during the merge are never lost. Tombstones are dropped, because every older file is merged away at the same time.

### The merge swap (crash and error safety)

The swap is ordered so that every step that can fail happens before the point of no return:

| # | Step | If it fails |
| --- | --- | --- |
| 1 | fsync `tempMerge.dat` | abort; nothing has changed (never retry fsync on the same fd) |
| 2 | close it, reopen it read-only | abort; nothing has changed |
| 3 | `rename(tempMerge.dat → 1.dat)` | abort; old `1.dat` is still open and in use |
| 4 | swap `db.files[1]` and the keydir together, then close the old handle | cannot fail (a close error is only logged) |
| 5 | fsync the data directory | return the error and skip step 6; memory is already consistent |
| 6 | delete files `2…N` in ascending order | a failed delete leaves a harmless leftover |

Why this works:

- **Rename keeps open handles valid.** A file descriptor refers to an inode, not a name. The old `1.dat` stays readable until its last descriptor is closed, and the read-only handle on the temp file becomes the new `1.dat` the moment the rename lands.
- **The directory needs its own fsync.** A rename or delete changes the directory's entries, not the file's data, so it isn't durable until the directory itself is fsynced. The deletes must not reach disk before the rename does.
- **Leftover files are safe to replay.** If a crash leaves some of `2…N` behind, `rebuild` replays the merged `1.dat` first and then the leftovers in id order. For every key, the newest version (or tombstone) still wins.
- **Delete order matters.** Ascending order means a tombstone file is never deleted while an older file holding the value it deleted still exists. Otherwise a deleted key could come back after a crash.

### Record format

All integers are little-endian `uint32`.

```text
┌──────────┬───────────┬──────────┬────────────┬───────────┬─────────────┐
│ crc32    │ timestamp │ key size │ value size │ key       │ value       │
│ 4 bytes  │ 4 bytes   │ 4 bytes  │ 4 bytes    │ key size  │ value size  │
└──────────┴───────────┴──────────┴────────────┴───────────┴─────────────┘
```

The CRC32 (IEEE) covers everything after it. A mismatch on read gives `ErrCorruptRecord`.

### Concurrency

- `sync.RWMutex` on the DB: reads take the read lock and run in parallel, while writes, deletes, rotation and the merge swap take the write lock.
- A separate `mergeMu` with `TryLock` ensures only one merge runs at a time (`ErrParallelMergeRunning`).
- The merge loop is a goroutine with a `time.Ticker` and a `done` channel, started by `Open` after rebuild. `Close` closes `done` and waits on a `sync.WaitGroup` before touching any file, so a merge can never run against closed files.

## How the server works

- **One goroutine per connection.** `Accept` loops in `Serve`, and each connection gets its own `bufio.Reader`/`bufio.Writer`.
- **Pull parser.** `readCommand` reads one RESP array of bulk strings. It uses `ReadSlice('\n')` for headers and `io.ReadFull` for payloads, so commands split across TCP packets, or several commands in one packet, are both handled.
- **Pipelining.** Replies are flushed only when the read buffer is empty. A batch of pipelined commands gets its replies in one write instead of one syscall per command.
- **Limits.** Bulk strings are capped at 1 MiB (`maxBulkLen`), and the length is checked *before* allocating. Header lines are capped by the 4 KiB bufio buffer, and idle connections are closed after 30s.

**Error handling:**

| Situation | Behaviour |
| --- | --- |
| Bad command (unknown name, wrong arity) | `-ERR …`, connection stays open |
| Bad framing (not an array, bad length, missing `\r\n`, too large) | `-ERR …`, then close. The stream can't be resynced. |
| Client disconnects / idle timeout | close silently |

**Graceful shutdown** on SIGINT/SIGTERM:

1. `signal.NotifyContext` cancels the context.
2. The server sets a shutdown flag (`atomic.Bool`) and closes the listener, so `Accept` returns and no new connections come in.
3. Every tracked connection gets `SetReadDeadline(time.Now())`, which wakes goroutines parked in `Read`. Busy connections finish their current command, see the flag and exit. Pending replies are flushed on every exit path.
4. A `sync.WaitGroup` waits for all connection goroutines. Then `DB.Close()` stops the merge goroutine, waiting for any in-flight merge to finish, syncs the active file, closes all files and returns every error joined.

The connection loop resets its deadline *before* checking the flag, and shutdown sets the flag *before* setting deadlines. This order guarantees that a connection can't miss both signals.

## Project layout

```text
cmd/kv/main.go       entry point: open DB, listen on :6380, handle signals, shutdown
db.go                DB: Open, Read, Write, Delete, rotate, merge, rebuild, Close
datafile.go          file handle wrapper: positioned reads and writes, open modes, sync
record.go            record encode/decode and CRC
keydir.go            in-memory index (and the merge-time index)
errors.go            exported sentinel errors
server/server.go     TCP server, command dispatch, graceful shutdown
server/resp.go       RESP parser and reply formatters
server/server_test.go  end-to-end tests over real TCP connections
```

## Testing

```sh
go test -race ./...
```

The server tests start a real server on a random port with a temp data dir. They cover:

- every command and reply type
- command errors that keep the connection open
- 11 kinds of malformed framing that must close it
- torn commands
- a 500-command pipeline
- byte-by-byte split reads
- 50 concurrent clients
- the value size limit

## Benchmarks

`redis-benchmark -p 6380 -t set,get` on a MacBook Air, over localhost:

| | requests/sec |
| --- | --- |
| SET | ~133k |
| GET | ~170k |
| SET, pipelined (`-P 16`) | ~377k |
| GET, pipelined (`-P 16`) | ~562k |

Writes are not fsynced per command, so these numbers reflect page-cache writes. Durability comes from the sync on shutdown and the OS's own writeback.

## Known limitations

- Writes aren't fsynced individually: a power loss can lose recent writes. Recovery still leaves the log consistent.
- The data directory is fsynced after the merge rename but not after each individual delete. That relies on the filesystem persisting the deletes in order, which journaling filesystems such as APFS and ext4 do in practice, but POSIX does not guarantee.
- The merge interval and threshold are compile-time constants.
- No hint files, so startup time grows with total log size, since every record is replayed.
- Timestamps have one-second resolution.
- The whole keydir must fit in memory. This is inherent to Bitcask.
- Only `PING`, `SET`, `GET` and `DEL` are supported, with no expiry, no `MSET`, and no auth. The bind address `127.0.0.1:6380` is hardcoded.
