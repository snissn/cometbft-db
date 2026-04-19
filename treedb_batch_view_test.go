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
}

func (s *stubKVBatchWithView) Set(_, _ []byte) error     { s.setCalls++; return nil }
func (s *stubKVBatchWithView) Delete(_ []byte) error     { s.deleteCalls++; return nil }
func (s *stubKVBatchWithView) Commit() error             { return nil }
func (s *stubKVBatchWithView) CommitSync() error         { return nil }
func (s *stubKVBatchWithView) Close() error              { return nil }
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
}

func (s *stubPrefixBatchWithView) Set(_, _ []byte) error     { s.setCalls++; return nil }
func (s *stubPrefixBatchWithView) Delete(_ []byte) error     { s.deleteCalls++; return nil }
func (s *stubPrefixBatchWithView) Write() error              { return nil }
func (s *stubPrefixBatchWithView) WriteSync() error          { return nil }
func (s *stubPrefixBatchWithView) Close() error              { return nil }
func (s *stubPrefixBatchWithView) SetView(_, _ []byte) error { s.setViewCalls++; return nil }
func (s *stubPrefixBatchWithView) DeleteView(_ []byte) error { s.deleteViewCalls++; return nil }

var _ Batch = (*stubPrefixBatchWithView)(nil)

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

func TestPrefixBatchSetViewPropagatesErrors(t *testing.T) {
	pb := newPrefixBatch([]byte("p/"), &stubPrefixBatchSetViewErr{})
	if err := pb.SetView([]byte("k"), []byte("v")); err == nil || err.Error() != "set-view-failed" {
		t.Fatalf("SetView err=%v want set-view-failed", err)
	}
	if err := pb.DeleteView([]byte("k")); err == nil || err.Error() != "delete-view-failed" {
		t.Fatalf("DeleteView err=%v want delete-view-failed", err)
	}
}
