package db

import (
	"errors"
	"testing"

	"github.com/snissn/gomap/kvstore"
)

type stubKVBatchWithView struct {
	setCalls        int
	setViewCalls    int
	deleteCalls     int
	deleteViewCalls int
	commitErr       error
	commitSyncErr   error
	closeErr        error
}

func (s *stubKVBatchWithView) Set(_, _ []byte) error     { s.setCalls++; return nil }
func (s *stubKVBatchWithView) Delete(_ []byte) error     { s.deleteCalls++; return nil }
func (s *stubKVBatchWithView) Commit() error             { return s.commitErr }
func (s *stubKVBatchWithView) CommitSync() error         { return s.commitSyncErr }
func (s *stubKVBatchWithView) Close() error              { return s.closeErr }
func (s *stubKVBatchWithView) SetView(_, _ []byte) error { s.setViewCalls++; return nil }
func (s *stubKVBatchWithView) DeleteView(_ []byte) error { s.deleteViewCalls++; return nil }

var _ kvstore.Batch = (*stubKVBatchWithView)(nil)

type stubKVBatchNoView struct {
	setCalls    int
	deleteCalls int
}

func (s *stubKVBatchNoView) Set(_, _ []byte) error { s.setCalls++; return nil }
func (s *stubKVBatchNoView) Delete(_ []byte) error { s.deleteCalls++; return nil }
func (s *stubKVBatchNoView) Commit() error         { return nil }
func (s *stubKVBatchNoView) CommitSync() error     { return nil }
func (s *stubKVBatchNoView) Close() error          { return nil }

var _ kvstore.Batch = (*stubKVBatchNoView)(nil)

func TestCoreBatchSetUsesUnderlyingView(t *testing.T) {
	stub := &stubKVBatchWithView{}
	b := &coreBatch{kb: stub}
	if err := b.Set([]byte("k"), []byte("v")); err != nil {
		t.Fatalf("Set: %v", err)
	}
	if stub.setViewCalls != 1 || stub.setCalls != 0 {
		t.Fatalf("set calls=%d setView calls=%d want 0/1", stub.setCalls, stub.setViewCalls)
	}
}

func TestCoreBatchSetFallsBackWithoutUnderlyingView(t *testing.T) {
	stub := &stubKVBatchNoView{}
	b := &coreBatch{kb: stub}
	if err := b.Set([]byte("k"), []byte("v")); err != nil {
		t.Fatalf("Set: %v", err)
	}
	if stub.setCalls != 1 {
		t.Fatalf("set calls=%d want 1", stub.setCalls)
	}
}

func TestCoreBatchSetValidationBeforeViewDispatch(t *testing.T) {
	stub := &stubKVBatchWithView{}
	b := &coreBatch{kb: stub}
	if err := b.Set(nil, []byte("v")); !errors.Is(err, errKeyEmpty) {
		t.Fatalf("Set nil key err=%v want %v", err, errKeyEmpty)
	}
	if err := b.Set([]byte("k"), nil); !errors.Is(err, errValueNil) {
		t.Fatalf("Set nil value err=%v want %v", err, errValueNil)
	}
	if stub.setViewCalls != 0 || stub.setCalls != 0 {
		t.Fatalf("set calls=%d setView calls=%d want 0/0", stub.setCalls, stub.setViewCalls)
	}
}

func TestCoreBatchSetClosedBeforeViewDispatch(t *testing.T) {
	stub := &stubKVBatchWithView{}
	b := &coreBatch{kb: stub, done: true}
	if err := b.Set([]byte("k"), []byte("v")); !errors.Is(err, errBatchClosed) {
		t.Fatalf("Set closed err=%v want %v", err, errBatchClosed)
	}
	if stub.setViewCalls != 0 || stub.setCalls != 0 {
		t.Fatalf("set calls=%d setView calls=%d want 0/0", stub.setCalls, stub.setViewCalls)
	}
}

func TestCoreBatchSetViewUsesUnderlyingView(t *testing.T) {
	stub := &stubKVBatchWithView{}
	b := &coreBatch{kb: stub}
	if err := b.SetView([]byte("k"), []byte("v")); err != nil {
		t.Fatalf("SetView: %v", err)
	}
	if err := b.DeleteView([]byte("k")); err != nil {
		t.Fatalf("DeleteView: %v", err)
	}
	if stub.setViewCalls != 1 || stub.setCalls != 0 {
		t.Fatalf("set calls=%d setView calls=%d want 0/1", stub.setCalls, stub.setViewCalls)
	}
	if stub.deleteViewCalls != 1 || stub.deleteCalls != 0 {
		t.Fatalf("delete calls=%d deleteView calls=%d want 0/1", stub.deleteCalls, stub.deleteViewCalls)
	}
}

func TestCoreBatchSetViewFallsBackWithoutUnderlyingView(t *testing.T) {
	stub := &stubKVBatchNoView{}
	b := &coreBatch{kb: stub}
	if err := b.SetView([]byte("k"), []byte("v")); err != nil {
		t.Fatalf("SetView: %v", err)
	}
	if err := b.DeleteView([]byte("k")); err != nil {
		t.Fatalf("DeleteView: %v", err)
	}
	if stub.setCalls != 1 {
		t.Fatalf("set calls=%d want 1", stub.setCalls)
	}
	if stub.deleteCalls != 1 {
		t.Fatalf("delete calls=%d want 1", stub.deleteCalls)
	}
}

func TestPrefixBatchSetViewPropagates(t *testing.T) {
	stub := &stubPrefixBatchWithView{}
	pb := newPrefixBatch([]byte("p/"), stub)
	if err := pb.SetView([]byte("k"), []byte("v")); err != nil {
		t.Fatalf("SetView: %v", err)
	}
	if err := pb.DeleteView([]byte("k")); err != nil {
		t.Fatalf("DeleteView: %v", err)
	}
	if stub.setViewCalls != 1 || stub.setCalls != 0 {
		t.Fatalf("set calls=%d setView calls=%d want 0/1", stub.setCalls, stub.setViewCalls)
	}
	if stub.deleteViewCalls != 1 || stub.deleteCalls != 0 {
		t.Fatalf("delete calls=%d deleteView calls=%d want 0/1", stub.deleteCalls, stub.deleteViewCalls)
	}
}

type stubPrefixBatchWithView struct {
	setCalls        int
	setViewCalls    int
	deleteCalls     int
	deleteViewCalls int
	setKeys         [][]byte
	setViewKeys     [][]byte
}

func (s *stubPrefixBatchWithView) Set(key, _ []byte) error {
	s.setCalls++
	s.setKeys = append(s.setKeys, key)
	return nil
}
func (s *stubPrefixBatchWithView) Delete(_ []byte) error { s.deleteCalls++; return nil }
func (s *stubPrefixBatchWithView) Write() error          { return nil }
func (s *stubPrefixBatchWithView) WriteSync() error      { return nil }
func (s *stubPrefixBatchWithView) Close() error          { return nil }
func (s *stubPrefixBatchWithView) SetView(key, _ []byte) error {
	s.setViewCalls++
	s.setViewKeys = append(s.setViewKeys, key)
	return nil
}
func (s *stubPrefixBatchWithView) DeleteView(_ []byte) error { s.deleteViewCalls++; return nil }

var _ Batch = (*stubPrefixBatchWithView)(nil)

func TestPrefixBatchSetUsesUnderlyingViewWithStablePrefixedKey(t *testing.T) {
	stub := &stubPrefixBatchWithView{}
	prefix := []byte("p/")
	key := []byte("key")
	pb := newPrefixBatch(prefix, stub)
	if err := pb.Set(key, []byte("value")); err != nil {
		t.Fatalf("Set: %v", err)
	}
	prefix[0] = 'x'
	key[0] = 'K'
	if stub.setViewCalls != 1 || stub.setCalls != 0 {
		t.Fatalf("set calls=%d setView calls=%d want 0/1", stub.setCalls, stub.setViewCalls)
	}
	if got := string(stub.setViewKeys[0]); got != "p/key" {
		t.Fatalf("SetView key=%q want %q", got, "p/key")
	}
}

type stubPrefixBatchNoView struct {
	setCalls int
}

func (s *stubPrefixBatchNoView) Set(_, _ []byte) error { s.setCalls++; return nil }
func (s *stubPrefixBatchNoView) Delete(_ []byte) error { return nil }
func (s *stubPrefixBatchNoView) Write() error          { return nil }
func (s *stubPrefixBatchNoView) WriteSync() error      { return nil }
func (s *stubPrefixBatchNoView) Close() error          { return nil }

func TestPrefixBatchSetFallsBackWithoutUnderlyingView(t *testing.T) {
	stub := &stubPrefixBatchNoView{}
	pb := newPrefixBatch([]byte("p/"), stub)
	if err := pb.Set([]byte("k"), []byte("v")); err != nil {
		t.Fatalf("Set: %v", err)
	}
	if stub.setCalls != 1 {
		t.Fatalf("set calls=%d want 1", stub.setCalls)
	}
}

type stubPrefixBatchSetViewErr struct{}

func (s *stubPrefixBatchSetViewErr) Set(_, _ []byte) error     { return nil }
func (s *stubPrefixBatchSetViewErr) Delete(_ []byte) error     { return nil }
func (s *stubPrefixBatchSetViewErr) Write() error              { return nil }
func (s *stubPrefixBatchSetViewErr) WriteSync() error          { return nil }
func (s *stubPrefixBatchSetViewErr) Close() error              { return nil }
func (s *stubPrefixBatchSetViewErr) SetView(_, _ []byte) error { return errors.New("set-view-failed") }
func (s *stubPrefixBatchSetViewErr) DeleteView(_ []byte) error {
	return errors.New("delete-view-failed")
}

func TestPrefixBatchSetPropagatesViewErrors(t *testing.T) {
	pb := newPrefixBatch([]byte("p/"), &stubPrefixBatchSetViewErr{})
	if err := pb.Set([]byte("k"), []byte("v")); err == nil || err.Error() != "set-view-failed" {
		t.Fatalf("Set err=%v want set-view-failed", err)
	}
}

func TestPrefixBatchSetViewPropagatesErrors(t *testing.T) {
	pb := newPrefixBatch([]byte("p/"), &stubPrefixBatchSetViewErr{})
	if err := pb.SetView([]byte("k"), []byte("v")); err == nil || err.Error() != "set-view-failed" {
		t.Fatalf("SetView err=%v want set-view-failed", err)
	}
	if err := pb.DeleteView([]byte("k")); err == nil || err.Error() != "delete-view-failed" {
		t.Fatalf("DeleteView err=%v want delete-view-failed", err)
	}
}

func TestCoreBatchWriteKeepsBatchOpenOnCommitError(t *testing.T) {
	stub := &stubKVBatchWithView{commitErr: errors.New("commit failed")}
	b := &coreBatch{kb: stub}
	if err := b.Write(); err == nil || err.Error() != "commit failed" {
		t.Fatalf("Write err=%v want commit failed", err)
	}
	if b.done {
		t.Fatalf("batch marked done after commit error")
	}
	if err := b.Set([]byte("k"), []byte("v")); err != nil {
		t.Fatalf("Set after failed Write: %v", err)
	}
}

func TestCoreBatchWriteSyncKeepsBatchOpenOnCommitError(t *testing.T) {
	stub := &stubKVBatchWithView{commitSyncErr: errors.New("commit-sync failed")}
	b := &coreBatch{kb: stub}
	if err := b.WriteSync(); err == nil || err.Error() != "commit-sync failed" {
		t.Fatalf("WriteSync err=%v want commit-sync failed", err)
	}
	if b.done {
		t.Fatalf("batch marked done after commit-sync error")
	}
	if err := b.Delete([]byte("k")); err != nil {
		t.Fatalf("Delete after failed WriteSync: %v", err)
	}
}

func TestCoreBatchCloseReturnsUnderlyingErrorAfterWrite(t *testing.T) {
	stub := &stubKVBatchWithView{closeErr: errors.New("close failed")}
	b := &coreBatch{kb: stub}
	if err := b.Write(); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if err := b.Close(); err == nil || err.Error() != "close failed" {
		t.Fatalf("Close err=%v want close failed", err)
	}
}

func TestCoreBatchPropagatesBatchInitError(t *testing.T) {
	initErr := errors.New("new batch failed")
	b := &coreBatch{err: initErr}
	if err := b.Set([]byte("k"), []byte("v")); !errors.Is(err, initErr) {
		t.Fatalf("Set err=%v want %v", err, initErr)
	}
	if err := b.Delete([]byte("k")); !errors.Is(err, initErr) {
		t.Fatalf("Delete err=%v want %v", err, initErr)
	}
	if err := b.Write(); !errors.Is(err, initErr) {
		t.Fatalf("Write err=%v want %v", err, initErr)
	}
	if err := b.WriteSync(); !errors.Is(err, initErr) {
		t.Fatalf("WriteSync err=%v want %v", err, initErr)
	}
	if err := b.Close(); !errors.Is(err, initErr) {
		t.Fatalf("Close err=%v want %v", err, initErr)
	}
}
