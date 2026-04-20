package db

import (
	"errors"
	"fmt"
	"os"
	"strconv"

	treedb "github.com/snissn/gomap/TreeDB"
	treedbkv "github.com/snissn/gomap/TreeDB/integration/kvstoreadapter"
	"github.com/snissn/gomap/TreeDB/tree"
	treedbadapter "github.com/snissn/gomap/kvstore/adapters/treedb"
)

func init() {
	dbCreator := func(name, dir string) (DB, error) {
		return NewTreeDB(name, dir)
	}
	registerDBCreator(TreeDBBackend, dbCreator)
}

// TreeDB is a TreeDB backend.
type TreeDB struct {
	db                     *treedb.DB
	kv                     *treedbadapter.DB
	snap                   treedb.Snapshot
	reuseReads             bool
	readBuf                []byte
	forceCheckpointOnWrite bool
}

var _ DB = (*TreeDB)(nil)

const envTreeDBForceCheckpointOnWrite = "TREEDB_FORCE_CHECKPOINT_ON_WRITE"
const envTreeDBOpenProfile = treedbkv.EnvOpenProfile

func (d *TreeDB) PinSnapshot() {
	if d.snap != nil {
		d.snap.Close()
	}
	d.snap = d.db.AcquireSnapshot()
}

func (d *TreeDB) UnpinSnapshot() {
	if d.snap != nil {
		d.snap.Close()
		d.snap = nil
	}
}

func forceCheckpointOnWriteFromEnv() bool {
	raw, ok := os.LookupEnv(envTreeDBForceCheckpointOnWrite)
	if !ok {
		return false
	}
	on, err := strconv.ParseBool(raw)
	if err != nil {
		return false
	}
	return on
}

func (d *TreeDB) maybeCheckpointAfterWrite() error {
	if d == nil || !d.forceCheckpointOnWrite || d.kv == nil {
		return nil
	}
	return d.kv.Checkpoint()
}

func NewTreeDB(name, dir string) (*TreeDB, error) {
	return NewTreeDBAdapter(dir, name)
}

func NewTreeDBAdapter(dir string, name string) (*TreeDB, error) {
	opened, err := treedbkv.Open(treedbkv.OpenConfig{
		ParentDir:         dir,
		Name:              name,
		AdapterName:       "TreeDB",
		DefaultProfile:    treedb.ProfileWALOnFast,
		DefaultKeepRecent: 1,
		ProfileEnvKey:     envTreeDBOpenProfile,
	})
	if err != nil {
		return nil, err
	}

	adapter := &TreeDB{
		db:                     opened.DB,
		kv:                     opened.KV,
		reuseReads:             false,
		forceCheckpointOnWrite: forceCheckpointOnWriteFromEnv(),
	}
	return adapter, nil
}

// Get implements DB.
func (d *TreeDB) Get(key []byte) ([]byte, error) {
	if len(key) == 0 {
		return nil, errKeyEmpty
	}
	if d.snap != nil {
		val, err := d.snap.GetUnsafe(key)
		if err != nil {
			if errors.Is(err, tree.ErrKeyNotFound) {
				return nil, nil
			}
			return nil, err
		}
		return val, nil
	}
	if d.db == nil {
		return nil, treedb.ErrClosed
	}
	if d.reuseReads {
		val, err := d.db.GetAppend(key, d.readBuf[:0])
		if err != nil {
			if errors.Is(err, tree.ErrKeyNotFound) {
				return nil, nil
			}
			return nil, err
		}
		d.readBuf = val[:0]
		return val, nil
	}
	return d.kv.GetUnsafe(key)
}

// GetAppend fetches the value of the given key into dst when supported.
// Missing keys return (nil, nil) to match DB.Get semantics.
func (d *TreeDB) GetAppend(key, dst []byte) ([]byte, error) {
	if len(key) == 0 {
		return nil, errKeyEmpty
	}
	if d.snap != nil {
		val, err := d.snap.GetAppend(key, dst)
		if err != nil {
			if errors.Is(err, tree.ErrKeyNotFound) {
				return nil, nil
			}
			return nil, err
		}
		return val, nil
	}
	if d.db == nil {
		return nil, treedb.ErrClosed
	}
	val, err := d.db.GetAppend(key, dst)
	if err != nil {
		if errors.Is(err, tree.ErrKeyNotFound) {
			return nil, nil
		}
		return nil, err
	}
	return val, nil
}

// Has implements DB.
func (d *TreeDB) Has(key []byte) (bool, error) {
	if len(key) == 0 {
		return false, errKeyEmpty
	}
	if d.snap != nil {
		return d.snap.Has(key)
	}
	if d.kv == nil {
		return false, treedb.ErrClosed
	}
	return d.kv.Has(key)
}

// Set implements DB.
func (d *TreeDB) Set(key, value []byte) error {
	if len(key) == 0 {
		return errKeyEmpty
	}
	if value == nil {
		return errValueNil
	}
	if d.kv == nil {
		return treedb.ErrClosed
	}
	if err := d.kv.Set(key, value); err != nil {
		return err
	}
	return d.maybeCheckpointAfterWrite()
}

// SetSync implements DB.
func (d *TreeDB) SetSync(key, value []byte) error {
	if len(key) == 0 {
		return errKeyEmpty
	}
	if value == nil {
		return errValueNil
	}
	if d.kv == nil {
		return treedb.ErrClosed
	}
	if err := d.kv.SetSync(key, value); err != nil {
		return err
	}
	return d.maybeCheckpointAfterWrite()
}

// Delete implements DB.
func (d *TreeDB) Delete(key []byte) error {
	if len(key) == 0 {
		return errKeyEmpty
	}
	if d.kv == nil {
		return treedb.ErrClosed
	}
	if err := d.kv.Delete(key); err != nil {
		return err
	}
	return d.maybeCheckpointAfterWrite()
}

// DeleteSync implements DB.
func (d *TreeDB) DeleteSync(key []byte) error {
	if len(key) == 0 {
		return errKeyEmpty
	}
	if d.kv == nil {
		return treedb.ErrClosed
	}
	if err := d.kv.DeleteSync(key); err != nil {
		return err
	}
	return d.maybeCheckpointAfterWrite()
}

// Iterator implements DB.
func (d *TreeDB) Iterator(start, end []byte) (Iterator, error) {
	if (start != nil && len(start) == 0) || (end != nil && len(end) == 0) {
		return nil, errKeyEmpty
	}
	if d.kv == nil {
		return nil, treedb.ErrClosed
	}
	it, err := d.kv.Iterator(start, end)
	if err != nil {
		return nil, err
	}
	return &coreIterator{iter: it, start: start, end: end}, nil
}

// ReverseIterator implements DB.
func (d *TreeDB) ReverseIterator(start, end []byte) (Iterator, error) {
	if (start != nil && len(start) == 0) || (end != nil && len(end) == 0) {
		return nil, errKeyEmpty
	}
	if d.kv == nil {
		return nil, treedb.ErrClosed
	}
	it, err := d.kv.ReverseIterator(start, end)
	if err != nil {
		return nil, err
	}
	return &coreIterator{iter: it, start: start, end: end}, nil
}

// Close implements DB.
func (d *TreeDB) Close() error {
	if d.db == nil {
		return nil
	}
	d.UnpinSnapshot()
	err := d.db.Close()
	d.db = nil
	d.kv = nil
	return err
}

// NewBatch implements DB.
func (d *TreeDB) NewBatch() Batch {
	if d.kv == nil {
		return &coreBatch{db: d, err: treedb.ErrClosed}
	}
	kb, err := d.kv.NewBatch()
	if err != nil {
		return &coreBatch{db: d, err: err}
	}
	return &coreBatch{db: d, kb: kb}
}

// Print implements DB.
func (d *TreeDB) Print() error {
	itr, err := d.Iterator(nil, nil)
	if err != nil {
		return err
	}
	defer itr.Close()
	for ; itr.Valid(); itr.Next() {
		key := itr.Key()
		value := itr.Value()
		fmt.Printf("[%X]:\t[%X]\n", key, value)
	}
	return nil
}

// Stats implements DB.
func (d *TreeDB) Stats() map[string]string {
	if d.kv == nil {
		return nil
	}
	return d.kv.Stats()
}

// Compact implements DB.
func (d *TreeDB) Compact(_, _ []byte) error {
	if d.kv == nil {
		return treedb.ErrClosed
	}
	return d.kv.Checkpoint()
}
