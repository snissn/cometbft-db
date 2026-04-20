package db

import (
	"testing"

	"github.com/snissn/gomap/kvstore"
)

type countingIterator struct {
	valid      bool
	key        []byte
	value      []byte
	keyCalls   int
	valueCalls int
}

func (it *countingIterator) Valid() bool                 { return it.valid }
func (it *countingIterator) Next()                       { it.valid = false }
func (it *countingIterator) Error() error                { return nil }
func (it *countingIterator) Close() error                { return nil }
func (it *countingIterator) Key() []byte                 { it.keyCalls++; return it.key }
func (it *countingIterator) Value() []byte               { it.valueCalls++; return it.value }
func (it *countingIterator) KeyCopy(dst []byte) []byte   { return append(dst[:0], it.Key()...) }
func (it *countingIterator) ValueCopy(dst []byte) []byte { return append(dst[:0], it.Value()...) }

var _ kvstore.Iterator = (*countingIterator)(nil)

func TestCoreIteratorCachesKeyAndValueUntilNext(t *testing.T) {
	src := &countingIterator{
		valid: true,
		key:   []byte("key"),
		value: []byte("value"),
	}
	it := &coreIterator{iter: src}

	if got := string(it.Key()); got != "key" {
		t.Fatalf("Key=%q want key", got)
	}
	if got := string(it.Key()); got != "key" {
		t.Fatalf("Key second=%q want key", got)
	}
	if got := string(it.Value()); got != "value" {
		t.Fatalf("Value=%q want value", got)
	}
	if got := string(it.Value()); got != "value" {
		t.Fatalf("Value second=%q want value", got)
	}
	if src.keyCalls != 1 {
		t.Fatalf("key calls=%d want 1", src.keyCalls)
	}
	if src.valueCalls != 1 {
		t.Fatalf("value calls=%d want 1", src.valueCalls)
	}
}
