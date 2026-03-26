package db

import "github.com/snissn/gomap/kvstore"

type coreBatch struct {
	db   *TreeDB
	kb   kvstore.Batch
	done bool
}

var _ Batch = (*coreBatch)(nil)

type batchSetViewer interface {
	SetView(key, value []byte) error
}

type batchDeleteViewer interface {
	DeleteView(key []byte) error
}

// Set implements Batch.
func (b *coreBatch) Set(key, value []byte) error {
	if len(key) == 0 {
		return errKeyEmpty
	}
	if value == nil {
		return errValueNil
	}
	if b.done || b.kb == nil {
		return errBatchClosed
	}
	return b.kb.Set(key, value)
}

// SetView records a Put without forcing another key/value copy when the
// underlying kv batch supports view semantics. Callers must keep key/value
// immutable until Write/WriteSync/Close.
func (b *coreBatch) SetView(key, value []byte) error {
	if len(key) == 0 {
		return errKeyEmpty
	}
	if value == nil {
		return errValueNil
	}
	if b.done || b.kb == nil {
		return errBatchClosed
	}
	if sv, ok := b.kb.(batchSetViewer); ok {
		return sv.SetView(key, value)
	}
	return b.kb.Set(key, value)
}

// Delete implements Batch.
func (b *coreBatch) Delete(key []byte) error {
	if len(key) == 0 {
		return errKeyEmpty
	}
	if b.done || b.kb == nil {
		return errBatchClosed
	}
	return b.kb.Delete(key)
}

// DeleteView records a Delete without forcing another key copy when the
// underlying kv batch supports view semantics. Callers must keep key immutable
// until Write/WriteSync/Close.
func (b *coreBatch) DeleteView(key []byte) error {
	if len(key) == 0 {
		return errKeyEmpty
	}
	if b.done || b.kb == nil {
		return errBatchClosed
	}
	if dv, ok := b.kb.(batchDeleteViewer); ok {
		return dv.DeleteView(key)
	}
	return b.kb.Delete(key)
}

// Write implements Batch.
func (b *coreBatch) Write() error {
	if b.done || b.kb == nil {
		return errBatchClosed
	}
	b.done = true
	if err := b.kb.Commit(); err != nil {
		return err
	}
	if b.db != nil {
		return b.db.maybeCheckpointAfterWrite()
	}
	return nil
}

// WriteSync implements Batch.
func (b *coreBatch) WriteSync() error {
	if b.done || b.kb == nil {
		return errBatchClosed
	}
	b.done = true
	if err := b.kb.CommitSync(); err != nil {
		return err
	}
	if b.db != nil {
		return b.db.maybeCheckpointAfterWrite()
	}
	return nil
}

// Close implements Batch.
func (b *coreBatch) Close() error {
	if b.kb == nil {
		b.done = true
		return nil
	}
	alreadyDone := b.done
	err := b.kb.Close()
	b.kb = nil
	b.done = true
	if alreadyDone {
		return nil
	}
	return err
}
