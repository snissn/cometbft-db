package db

import (
	"bytes"
	"testing"
)

type appendRecorderDB struct {
	*MemDB
	appendCalls int
	lastKey     []byte
}

func (db *appendRecorderDB) GetAppend(key, dst []byte) ([]byte, error) {
	db.appendCalls++
	db.lastKey = append(db.lastKey[:0], key...)
	value, err := db.Get(key)
	if err != nil || value == nil {
		return nil, err
	}
	dst = append(dst[:0], value...)
	return dst, nil
}

func TestPrefixDBGetAppendUsesUnderlyingGetter(t *testing.T) {
	base := &appendRecorderDB{MemDB: NewMemDB()}
	if err := base.Set(bz("key1"), bz("value1")); err != nil {
		t.Fatalf("seed: %v", err)
	}
	pdb := NewPrefixDB(base, bz("key"))
	got, err := pdb.GetAppend(bz("1"), make([]byte, 0, 16))
	if err != nil {
		t.Fatalf("GetAppend: %v", err)
	}
	if !bytes.Equal(got, bz("value1")) {
		t.Fatalf("got %q want %q", got, bz("value1"))
	}
	if base.appendCalls != 1 {
		t.Fatalf("append calls=%d want 1", base.appendCalls)
	}
	if !bytes.Equal(base.lastKey, bz("key1")) {
		t.Fatalf("last key=%q want %q", base.lastKey, bz("key1"))
	}
}

func TestPrefixDBGetAppendFallsBackToGet(t *testing.T) {
	base := mockDBWithStuff(t)
	pdb := NewPrefixDB(base, bz("key"))
	dst := []byte("scratch")
	got, err := pdb.GetAppend(bz("2"), dst)
	if err != nil {
		t.Fatalf("GetAppend: %v", err)
	}
	if !bytes.Equal(got, bz("value2")) {
		t.Fatalf("got %q want %q", got, bz("value2"))
	}
}
