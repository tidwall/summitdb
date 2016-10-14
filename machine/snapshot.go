package machine

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/tidwall/buntdb"
)

// Restore restores a snapshot
func (m *Machine) Restore(rd io.Reader) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	// read the snapshot into a new machine.
	// the new machine will have the entire keyspace, but will be missing
	// indexes and scripts.
	nm := &Machine{}
	if err := nm.reopenBlankDB(rd, func(keys []string) { m.onExpired(keys) }); err != nil {
		return err
	}

	// rebuild the indexes
	if err := nm.db.Update(func(tx *buntdb.Tx) error {
		var metas []string
		if err := tx.AscendGreaterOrEqual("", indexKeyPrefix, func(key, val string) bool {
			if !strings.HasPrefix(key, indexKeyPrefix) {
				return false
			}
			metas = append(metas, key, val)
			return true
		}); err != nil {
			return err
		}
		for i := 0; i < len(metas); i += 2 {
			name := metas[i]
			var rargs indexArgs
			if err := json.Unmarshal([]byte(metas[i+1]), &rargs); err != nil {
				return fmt.Errorf("parsing index '%v': %v", name, err)
			}
			if err := dbSetIndex(tx, rargs); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		return err
	}

	// close and delete the previous file
	m.db.Close()
	os.RemoveAll(m.file)

	// set the important fields to the new machine, file, and script machine.
	m.db = nm.db
	m.file = nm.file

	return nil
}

// Snapshot creates a snapshot
func (m *Machine) Snapshot(wr io.Writer) error {
	var pos int64
	var file string
	err := func() error {
		m.mu.RLock()
		defer m.mu.RUnlock()
		f, err := os.Open(m.file)
		if err != nil {
			return err
		}
		defer f.Close()
		pos, err = f.Seek(0, 2)
		if err != nil {
			return err
		}
		file = m.file
		return nil
	}()
	if err != nil {
		return err
	}
	// write the file data
	f, err := os.Open(file)
	if err != nil {
		return err
	}
	defer f.Close()
	if _, err := io.CopyN(wr, f, pos); err != nil {
		return err
	}
	return nil
}
