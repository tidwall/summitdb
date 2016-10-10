package raftfastlog

import (
	"bufio"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"sync"
	"time"

	"github.com/hashicorp/raft"
)

type Level int

const (
	Low    Level = -1
	Medium Level = 0
	High   Level = 1
)

// An error indicating a given key does not exist
var ErrKeyNotFound = errors.New("not found")
var ErrClosed = errors.New("closed")
var ErrShrinking = errors.New("shrink in progress")

const minShrinkSize = 64 * 1024 * 1024

const (
	cmdSet         = '(' // Key+Val
	cmdDel         = ')' // Key
	cmdStoreLogs   = '[' // Count+Log,Log...  Log: Idx+Term+Type+Data
	cmdDeleteRange = ']' // Min+Max
)

// FastLogStore provides access to FastLogDB for Raft to store and retrieve
// log entries. It also provides key/value storage, and can be used as
// a LogStore and StableStore.
type FastLogStore struct {
	mu         sync.RWMutex
	path       string
	durability Level
	file       *os.File
	kvm        map[string][]byte
	lvm        map[uint64]*raft.Log
	closed     bool
	bsize      int
	size       int
	dirty      bool
	buf        []byte
	limits     bool
	min, max   uint64
	log        io.Writer
	shrinking  bool
	persist    bool
}

// NewFastLogStore takes a file path and returns a connected Raft backend.
func NewFastLogStore(path string, durability Level, logOutput io.Writer) (*FastLogStore, error) {
	// create the new store
	b := &FastLogStore{
		path:       path,
		durability: durability,
		kvm:        make(map[string][]byte),
		lvm:        make(map[uint64]*raft.Log),
		limits:     true,
		log:        logOutput,
		persist:    path != ":memory:",
	}
	if b.persist {
		// open file
		var err error
		b.file, err = os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0666)
		if err != nil {
			return nil, err
		}
		// load file
		if err := func() error {
			if b.log != nil {
				start := time.Now()
				defer func() {
					fmt.Fprintf(b.log, "%s [VERB] store: Loading store completed: %s\n",
						time.Now().Format("2006/01/02 15:04:05"), time.Now().Sub(start).String())
				}()
			}
			num := make([]byte, 8)
			rd := bufio.NewReader(b.file)
			for {
				c, err := rd.ReadByte()
				if err != nil {
					if err == io.EOF {
						break
					}
					b.file.Close()
					return err
				}
				switch c {
				default:
					return errors.New("invalid database")
				case cmdSet, cmdDel:
					if _, err := io.ReadFull(rd, num); err != nil {
						return err
					}
					key := make([]byte, int(binary.LittleEndian.Uint64(num)))
					if _, err := io.ReadFull(rd, key); err != nil {
						return err
					}
					if c == cmdSet {
						if _, err := io.ReadFull(rd, num); err != nil {
							return err
						}
						val := make([]byte, int(binary.LittleEndian.Uint64(num)))
						if _, err := io.ReadFull(rd, val); err != nil {
							return err
						}
						b.kvm[string(key)] = val
					} else {
						delete(b.kvm, string(key))
					}
				case cmdStoreLogs:
					if _, err := io.ReadFull(rd, num); err != nil {
						return err
					}
					count := int(binary.LittleEndian.Uint64(num))
					for i := 0; i < count; i++ {
						if err := func() error {
							var log raft.Log
							if _, err := io.ReadFull(rd, num); err != nil {
								return err
							}
							log.Index = binary.LittleEndian.Uint64(num)
							if _, err := io.ReadFull(rd, num); err != nil {
								return err
							}
							log.Term = binary.LittleEndian.Uint64(num)
							c, err := rd.ReadByte()
							if err != nil {
								return err
							}
							log.Type = raft.LogType(c)
							if _, err := io.ReadFull(rd, num); err != nil {
								return err
							}
							log.Data = make([]byte, int(binary.LittleEndian.Uint64(num)))
							if _, err := io.ReadFull(rd, log.Data); err != nil {
								return err
							}
							if b.limits {
								if b.min == 0 {
									b.min, b.max = log.Index, log.Index
								} else if log.Index < b.min {
									b.min = log.Index
								} else if log.Index > b.max {
									b.max = log.Index
								}
							}
							b.lvm[log.Index] = &log
							return nil
						}(); err != nil {
							return err
						}
					}
				case cmdDeleteRange:
					if _, err := io.ReadFull(rd, num); err != nil {
						return err
					}
					min := binary.LittleEndian.Uint64(num)
					if _, err := io.ReadFull(rd, num); err != nil {
						return err
					}
					max := binary.LittleEndian.Uint64(num)
					for i := min; i < max; i++ {
						delete(b.lvm, i)
					}
					b.limits = false
				}
			}
			pos, err := b.file.Seek(0, 1)
			if err != nil {
				return err
			}
			b.bsize = int(pos)
			b.size = int(pos)
			return nil
		}(); err != nil {
			b.file.Close()
			return nil, err
		}
		go b.run()
	}
	return b, nil
}

// Close is used to gracefully close the DB connection.
func (b *FastLogStore) Close() error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		return ErrClosed
	}
	b.closed = true
	if b.persist {
		b.file.Sync()
		b.file.Close()
	}
	return nil
}

func (b *FastLogStore) run() {
	for {
		time.Sleep(time.Second)
		done := func() bool {
			b.mu.Lock()
			if b.closed {
				b.mu.Unlock()
				return true
			}
			if b.durability == Medium && b.dirty {
				b.file.Sync()
				b.dirty = false
			}
			shrink := (b.bsize < minShrinkSize && b.size > minShrinkSize) ||
				(b.bsize > minShrinkSize && b.size > b.bsize*2)
			b.mu.Unlock()
			if shrink {
				b.Shrink()
			}
			return false
		}()
		if done {
			return
		}
	}
}

func (b *FastLogStore) Shrink() error {
	b.mu.Lock()
	if !b.persist {
		b.mu.Unlock()
		return nil
	}
	if b.shrinking {
		b.mu.Unlock()
		return ErrShrinking
	}
	b.shrinking = true
	b.mu.Unlock()
	defer func() {
		b.mu.Lock()
		b.shrinking = false
		b.mu.Unlock()
	}()
	start := time.Now()
	err := b.shrink()
	if b.log != nil {
		if err != nil {
			fmt.Fprintf(b.log, "%s [WARN] store: Shrink failed: %v\n",
				time.Now().Format("2006/01/02 15:04:05"), err)
		} else {
			fmt.Fprintf(b.log, "%s [VERB] store: Shrink completed: %v\n",
				time.Now().Format("2006/01/02 15:04:05"), time.Now().Sub(start).String())
		}
	}
	return err
}

func (b *FastLogStore) shrink() error {
	var buf []byte
	b.mu.RLock()
	// shrink operation
	pos := b.size // record the current file position
	// just read all keys at once. there shouldn't be too many.
	for key, val := range b.kvm {
		buf = bufferSet(buf, []byte(key), val)
	}
	// read all log indexes at once
	var i int
	idxs := make([]uint64, len(b.lvm))
	for idx := range b.lvm {
		idxs[i] = idx
		i++
	}
	b.mu.RUnlock()
	// create the new file
	npath := b.path + ".shrink"
	nf, err := os.Create(npath)
	if err != nil {
		return err
	}
	defer func() {
		nf.Close()
		os.RemoveAll(npath)
	}()
	// read chunks of logs at a time. releasing lock occasionally.
	b.mu.RLock()
	num := make([]byte, 8)
	binary.LittleEndian.PutUint64(num, 1)
	var buffered int
	for _, idx := range idxs {
		if log, ok := b.lvm[idx]; ok {
			buf = append(buf, cmdStoreLogs)
			buf = append(buf, num...)
			buf = bufferLog(buf, log)
			buffered++
			// flush every 64MB or 1000 items
			if len(buf) > 64*1024*1024 || buffered == 1000 {
				b.mu.RUnlock()
				if _, err := nf.Write(buf); err != nil {
					return err
				}
				buf = buf[:0]
				buffered = 0
				b.mu.RLock()
			}
		}
	}
	if len(buf) > 0 {
		b.mu.RUnlock()
		if _, err := nf.Write(buf); err != nil {
			return err
		}
		buf = buf[:0]
		b.mu.RLock()
	}
	b.mu.RUnlock()
	err = func() error {
		// write the tail of the database
		// this is a two run process, first run will read as much as possible
		// with a lock. this allows for sets and get to continue.
		// the second run will lock and finish reading any remaining.

		// run 1
		of, err := os.Open(b.path)
		if err != nil {
			return err
		}
		defer of.Close()
		if _, err := of.Seek(int64(pos), 0); err != nil {
			return err
		}
		copied, err := io.Copy(nf, of)
		if err != nil {
			return err
		}
		// run 2
		b.mu.Lock()
		defer b.mu.Unlock()
		of, err = os.Open(b.path)
		if err != nil {
			return err
		}
		defer of.Close()
		if _, err := of.Seek(int64(pos+int(copied)), 0); err != nil {
			return err
		}
		if _, err := io.Copy(nf, of); err != nil {
			return err
		}
		// close all the files
		of.Close()
		nf.Close()
		b.file.Close()
		if err := os.Rename(b.path+".shrink", b.path); err != nil {
			panic("shrink failed: " + err.Error())
			return err
		}
		b.file, err = os.OpenFile(b.path, os.O_CREATE|os.O_RDWR, 0666)
		if err != nil {
			panic("shrink failed: " + err.Error())
			return err
		}
		size, err := b.file.Seek(0, 2)
		if err != nil {
			panic("shrink failed: " + err.Error())
			return err
		}
		b.bsize = int(size)
		b.size = int(size)
		return nil
	}()
	if err != nil {
		return err
	}
	return nil
}

func (b *FastLogStore) fillLimits() {
	b.min, b.max = 0, 0
	for idx := range b.lvm {
		if b.min == 0 {
			b.min, b.max = idx, idx
		} else if idx < b.min {
			b.min = idx
		} else if idx > b.max {
			b.max = idx
		}
	}
	b.limits = true
}

// FirstIndex returns the first known index from the Raft log.
func (b *FastLogStore) FirstIndex() (uint64, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		return 0, ErrClosed
	}
	if b.limits {
		return b.min, nil
	}
	b.fillLimits()
	return b.min, nil
}

// LastIndex returns the last known index from the Raft log.
func (b *FastLogStore) LastIndex() (uint64, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		return 0, ErrClosed
	}
	if b.limits {
		return b.max, nil
	}
	b.fillLimits()
	return b.max, nil
}

// GetLog is used to retrieve a log from FastLogDB at a given index.
func (b *FastLogStore) GetLog(idx uint64, log *raft.Log) error {
	b.mu.RLock()
	defer b.mu.RUnlock()
	if b.closed {
		return ErrClosed
	}
	vlog := b.lvm[idx]
	if vlog == nil {
		return raft.ErrLogNotFound
	}
	*log = *vlog
	return nil
}

// StoreLog is used to store a single raft log
func (b *FastLogStore) StoreLog(log *raft.Log) error {
	return b.StoreLogs([]*raft.Log{log})
}

func (b *FastLogStore) writeBuf() error {
	if _, err := b.file.Write(b.buf); err != nil {
		return err
	}
	b.size += len(b.buf)
	if b.durability == High {
		b.file.Sync()
	} else if b.durability == Medium {
		b.dirty = true
	}
	return nil
}
func bufferLog(buf []byte, log *raft.Log) []byte {
	var num = make([]byte, 8)
	binary.LittleEndian.PutUint64(num, log.Index)
	buf = append(buf, num...)
	binary.LittleEndian.PutUint64(num, log.Term)
	buf = append(buf, num...)
	buf = append(buf, byte(log.Type))
	binary.LittleEndian.PutUint64(num, uint64(len(log.Data)))
	buf = append(buf, num...)
	buf = append(buf, log.Data...)
	return buf
}

// StoreLogs is used to store a set of raft logs
func (b *FastLogStore) StoreLogs(logs []*raft.Log) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		return ErrClosed
	}
	if b.persist {
		var num = make([]byte, 8)
		b.buf = b.buf[:0]
		b.buf = append(b.buf, cmdStoreLogs)
		binary.LittleEndian.PutUint64(num, uint64(len(logs)))
		b.buf = append(b.buf, num...)
		for _, log := range logs {
			b.buf = bufferLog(b.buf, log)
		}
		if err := b.writeBuf(); err != nil {
			return err
		}
	}
	for _, log := range logs {
		b.lvm[log.Index] = log
		if b.limits {
			if b.min == 0 {
				b.min, b.max = log.Index, log.Index
			} else if log.Index < b.min {
				b.min = log.Index
			} else if log.Index > b.max {
				b.max = log.Index
			}
		}
	}
	return nil
}

// DeleteRange is used to delete logs within a given range inclusively.
func (b *FastLogStore) DeleteRange(min, max uint64) error {
	if b.log != nil {
		start := time.Now()
		defer func() {
			fmt.Fprintf(b.log, "%s [VERB] store: Deleting range %d-%d completed: %s\n",
				time.Now().Format("2006/01/02 15:04:05"), min, max, time.Now().Sub(start).String())
		}()
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		return ErrClosed
	}
	if b.persist {
		var num = make([]byte, 8)
		b.buf = b.buf[:0]
		b.buf = append(b.buf, cmdDeleteRange)
		binary.LittleEndian.PutUint64(num, min)
		b.buf = append(b.buf, num...)
		binary.LittleEndian.PutUint64(num, max)
		b.buf = append(b.buf, num...)
		if err := b.writeBuf(); err != nil {
			return err
		}
	}
	for i := min; i <= max; i++ {
		delete(b.lvm, i)
	}
	b.limits = false
	return nil
}
func bufferSet(buf []byte, k, v []byte) []byte {
	var num = make([]byte, 8)
	buf = append(buf, cmdSet)
	binary.LittleEndian.PutUint64(num, uint64(len(k)))
	buf = append(buf, num...)
	buf = append(buf, k...)
	binary.LittleEndian.PutUint64(num, uint64(len(v)))
	buf = append(buf, num...)
	buf = append(buf, v...)
	return buf
}

// Set is used to set a key/value set outside of the raft log
func (b *FastLogStore) Set(k, v []byte) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		return ErrClosed
	}
	if b.persist {
		b.buf = b.buf[:0]
		b.buf = bufferSet(b.buf, k, v)
		if err := b.writeBuf(); err != nil {
			return err
		}
	}
	b.kvm[string(k)] = v
	return nil
}

// Get is used to retrieve a value from the k/v store by key
func (b *FastLogStore) Get(k []byte) ([]byte, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	if b.closed {
		return nil, ErrClosed
	}
	if val, ok := b.kvm[string(k)]; ok {
		return val, nil
	}
	return nil, ErrKeyNotFound
}

// SetUint64 is like Set, but handles uint64 values
func (b *FastLogStore) SetUint64(key []byte, val uint64) error {
	data := make([]byte, 8)
	binary.LittleEndian.PutUint64(data, val)
	return b.Set(key, data)
}

// GetUint64 is like Get, but handles uint64 values
func (b *FastLogStore) GetUint64(key []byte) (uint64, error) {
	val, err := b.Get(key)
	if err != nil {
		return 0, err
	}
	if len(val) != 8 {
		return 0, errors.New("invalid number")
	}
	return binary.LittleEndian.Uint64(val), nil
}

// Peers returns raft peers
func (b *FastLogStore) Peers() ([]string, error) {
	var peers []string
	val, err := b.Get([]byte("peers"))
	if err != nil {
		if err == ErrKeyNotFound {
			return []string{}, nil
		}
		return nil, err
	}
	if err := json.Unmarshal(val, &peers); err != nil {
		return nil, err
	}
	return peers, nil
}

// SetPeers sets raft peers
func (b *FastLogStore) SetPeers(peers []string) error {
	data, err := json.Marshal(peers)
	if err != nil {
		return err
	}
	return b.Set([]byte("peers"), data)
}
