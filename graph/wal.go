package graph

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"hash/crc32"
	"os"
	"path/filepath"
)

// Op is one logical mutation. It is the unit written to the WAL and the unit
// stored in snapshots, so recovery is simply "apply ops in order".
type Op struct {
	LSN   uint64         `json:"lsn,omitempty"`
	Kind  string         `json:"k"` // node | edge | set | delnode | deledge | index
	ID    uint64         `json:"id,omitempty"`
	Label string         `json:"l,omitempty"` // node label, edge type or index label
	From  uint64         `json:"f,omitempty"`
	To    uint64         `json:"t,omitempty"`
	Props map[string]any `json:"p,omitempty"`
	Key   string         `json:"key,omitempty"`
	Val   any            `json:"v,omitempty"`
}

// WAL is an append-only log. Record layout: [len u32][crc32 u32][json payload].
type WAL struct {
	f    *os.File
	sync bool
}

func openWAL(path string, sync bool) (*WAL, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0o644)
	if err != nil {
		return nil, err
	}
	return &WAL{f: f, sync: sync}, nil
}

// Append writes a batch of ops with a single write call (group commit).
func (w *WAL) Append(ops []Op) error {
	var buf []byte
	for _, op := range ops {
		payload, err := json.Marshal(op)
		if err != nil {
			return err
		}
		var hdr [8]byte
		binary.LittleEndian.PutUint32(hdr[0:], uint32(len(payload)))
		binary.LittleEndian.PutUint32(hdr[4:], crc32.ChecksumIEEE(payload))
		buf = append(buf, hdr[:]...)
		buf = append(buf, payload...)
	}
	if _, err := w.f.Write(buf); err != nil {
		return err
	}
	if w.sync {
		return w.f.Sync()
	}
	return nil
}

func (w *WAL) Truncate() error { return w.f.Truncate(0) }
func (w *WAL) Close() error    { return w.f.Close() }

// replayWAL feeds every intact record to fn and returns the offset of the
// last valid byte. A torn or corrupt tail (crash mid-write) is ignored.
func replayWAL(path string, fn func(Op)) (int64, error) {
	b, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	off := 0
	for off+8 <= len(b) {
		n := int(binary.LittleEndian.Uint32(b[off:]))
		crc := binary.LittleEndian.Uint32(b[off+4:])
		if off+8+n > len(b) {
			break
		}
		payload := b[off+8 : off+8+n]
		if crc32.ChecksumIEEE(payload) != crc {
			break
		}
		var op Op
		if json.Unmarshal(payload, &op) != nil {
			break
		}
		fn(op)
		off += 8 + n
	}
	return int64(off), nil
}

// ObjectStore is the storage-layer boundary. FSStore is the local
// implementation; an S3/MinIO adapter only needs these two methods.
type ObjectStore interface {
	Put(key string, data []byte) error
	Get(key string) ([]byte, error)
}

type FSStore struct{ Dir string }

// Put is atomic: write to a temp file, fsync, then rename over the target.
func (s FSStore) Put(key string, data []byte) error {
	tmp := filepath.Join(s.Dir, key+".tmp")
	f, err := os.Create(tmp)
	if err != nil {
		return err
	}
	if _, err = f.Write(data); err == nil {
		err = f.Sync()
	}
	f.Close()
	if err != nil {
		return err
	}
	return os.Rename(tmp, filepath.Join(s.Dir, key))
}

func (s FSStore) Get(key string) ([]byte, error) {
	return os.ReadFile(filepath.Join(s.Dir, key))
}
