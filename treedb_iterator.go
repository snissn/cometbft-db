package db

import "github.com/snissn/gomap/kvstore"

type keyArena struct {
	buf []byte
}

func newKeyArena(capacity int) keyArena {
	if capacity <= 0 {
		capacity = 64 * 1024
	}
	return keyArena{buf: make([]byte, 0, capacity)}
}

func (a *keyArena) Copy(key []byte) ([]byte, bool) {
	if len(key) > cap(a.buf)-len(a.buf) {
		return nil, false
	}
	off := len(a.buf)
	a.buf = append(a.buf, key...)
	return a.buf[off : off+len(key)], true
}

type coreIterator struct {
	iter  kvstore.Iterator
	start []byte
	end   []byte

	keyArena keyArena
	valArena keyArena
}

var _ Iterator = (*coreIterator)(nil)

// Domain implements Iterator.
func (it *coreIterator) Domain() (start, end []byte) { return it.start, it.end }

// Valid implements Iterator.
func (it *coreIterator) Valid() bool { return it.iter.Valid() }

// Next implements Iterator.
func (it *coreIterator) Next() {
	it.assertIsValid()
	it.iter.Next()
}

// Key implements Iterator.
func (it *coreIterator) Key() []byte {
	it.assertIsValid()
	if it.keyArena.buf == nil {
		it.keyArena = newKeyArena(64 * 1024)
	}
	key := it.iter.Key()
	out, ok := it.keyArena.Copy(key)
	if ok {
		return out
	}
	out = make([]byte, len(key))
	copy(out, key)
	return out
}

// Value implements Iterator.
func (it *coreIterator) Value() []byte {
	it.assertIsValid()
	if it.valArena.buf == nil {
		it.valArena = newKeyArena(256 * 1024)
	}
	val := it.iter.Value()
	out, ok := it.valArena.Copy(val)
	if ok {
		return out
	}
	out = make([]byte, len(val))
	copy(out, val)
	return out
}

// Error implements Iterator.
func (it *coreIterator) Error() error { return it.iter.Error() }

// Close implements Iterator.
func (it *coreIterator) Close() error { return it.iter.Close() }

func (it *coreIterator) assertIsValid() {
	if !it.Valid() {
		panic("iterator is invalid")
	}
}

type materializedReverseIterator struct {
	start []byte
	end   []byte
	keys  [][]byte
	vals  [][]byte
	idx   int
	err   error
}

func newMaterializedReverseIterator(start, end []byte, src kvstore.Iterator) (*materializedReverseIterator, error) {
	defer src.Close()
	keys := make([][]byte, 0, 128)
	vals := make([][]byte, 0, 128)
	for ; src.Valid(); src.Next() {
		k := src.Key()
		v := src.Value()
		keys = append(keys, append([]byte(nil), k...))
		vals = append(vals, append([]byte(nil), v...))
	}
	if err := src.Error(); err != nil {
		return nil, err
	}
	return &materializedReverseIterator{
		start: start,
		end:   end,
		keys:  keys,
		vals:  vals,
		idx:   len(keys) - 1,
	}, nil
}

func (it *materializedReverseIterator) Domain() (start, end []byte) { return it.start, it.end }

func (it *materializedReverseIterator) Valid() bool {
	return it != nil && it.err == nil && it.idx >= 0 && it.idx < len(it.keys)
}

func (it *materializedReverseIterator) Next() {
	it.assertIsValid()
	it.idx--
}

func (it *materializedReverseIterator) Key() []byte {
	it.assertIsValid()
	return it.keys[it.idx]
}

func (it *materializedReverseIterator) Value() []byte {
	it.assertIsValid()
	return it.vals[it.idx]
}

func (it *materializedReverseIterator) Error() error { return it.err }

func (it *materializedReverseIterator) Close() error {
	if it == nil {
		return nil
	}
	it.keys = nil
	it.vals = nil
	it.idx = -1
	return nil
}

func (it *materializedReverseIterator) assertIsValid() {
	if !it.Valid() {
		panic("iterator is invalid")
	}
}
