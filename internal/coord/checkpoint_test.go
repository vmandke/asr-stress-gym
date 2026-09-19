package coord

import (
	"testing"

	"asr-stress-gym/internal/backend"
)

func TestCanRestoreAcceptsMatchingKeyAndValidChecksum(t *testing.T) {
	cp := newCheckpoint("s1", "K1", backend.CheckpointResp{CheckpointBlob: []byte("state"), Generation: 3, LastSeqApplied: 100})
	if !CanRestore(cp, "K1") {
		t.Fatal("expected CanRestore to accept a matching key with a valid checksum")
	}
}

func TestCanRestoreRejectsWrongKey(t *testing.T) {
	cp := newCheckpoint("s1", "K1", backend.CheckpointResp{CheckpointBlob: []byte("state")})
	if CanRestore(cp, "K2") {
		t.Fatal("expected CanRestore to reject a compatibility key mismatch")
	}
}

func TestCanRestoreRejectsCorruptedChecksum(t *testing.T) {
	cp := newCheckpoint("s1", "K1", backend.CheckpointResp{CheckpointBlob: []byte("state")})
	cp.Checksum = "tampered"
	if CanRestore(cp, "K1") {
		t.Fatal("expected CanRestore to reject a checksum that doesn't match the blob")
	}
}

func TestCheckpointStoreRoundTrip(t *testing.T) {
	s := NewCheckpointStore()
	s.Store("s1", "K1", backend.CheckpointResp{CheckpointBlob: []byte("blob"), Generation: 2, LastSeqApplied: 50})

	cp, ok := s.Latest("s1")
	if !ok {
		t.Fatal("expected a stored checkpoint")
	}
	if cp.Generation != 2 || cp.Seq != 50 || string(cp.StateBlob) != "blob" {
		t.Fatalf("got %+v", cp)
	}
	if !CanRestore(cp, "K1") {
		t.Fatal("a freshly stored checkpoint must validate")
	}
}

func TestCheckpointStoreLatestOverwritesPrevious(t *testing.T) {
	s := NewCheckpointStore()
	s.Store("s1", "K1", backend.CheckpointResp{CheckpointBlob: []byte("old"), LastSeqApplied: 10})
	s.Store("s1", "K1", backend.CheckpointResp{CheckpointBlob: []byte("new"), LastSeqApplied: 20})

	cp, _ := s.Latest("s1")
	if string(cp.StateBlob) != "new" || cp.Seq != 20 {
		t.Fatalf("got %+v, want the second (newer) checkpoint", cp)
	}
}

func TestCheckpointStoreMissingSessionReturnsFalse(t *testing.T) {
	s := NewCheckpointStore()
	if _, ok := s.Latest("never-stored"); ok {
		t.Fatal("expected ok=false for a session with no checkpoint")
	}
}

// The gateway-side half of chaos scenario 5.
func TestCheckpointStoreCorruptMakesCanRestoreFail(t *testing.T) {
	s := NewCheckpointStore()
	s.Store("s1", "K1", backend.CheckpointResp{CheckpointBlob: []byte("blob")})

	if !s.Corrupt("s1") {
		t.Fatal("expected Corrupt to report success for an existing session")
	}
	cp, ok := s.Latest("s1")
	if !ok {
		t.Fatal("Corrupt must not delete the checkpoint, only invalidate it")
	}
	if CanRestore(cp, "K1") {
		t.Fatal("a corrupted checkpoint must fail CanRestore even with a matching key")
	}
}

func TestCheckpointStoreCorruptOnMissingSessionReturnsFalse(t *testing.T) {
	s := NewCheckpointStore()
	if s.Corrupt("never-stored") {
		t.Fatal("expected Corrupt to report false when there is nothing to corrupt")
	}
}

// A nil *CheckpointStore ("no checkpointing configured") must behave
// exactly like an empty one, not panic — found via
// TestRecoverSameModelDegradesWithNoCheckpoint crashing, not by
// inspection. Invariant 13: checkpoint failure must never take down
// healthy inference, and "there is no store at all" is the most basic
// case of that.
func TestNilCheckpointStoreIsSafe(t *testing.T) {
	var s *CheckpointStore
	if _, ok := s.Latest("s1"); ok {
		t.Fatal("expected ok=false from a nil store")
	}
	if s.Corrupt("s1") {
		t.Fatal("expected false from Corrupt on a nil store")
	}
	s.Store("s1", "K1", backend.CheckpointResp{CheckpointBlob: []byte("x")}) // must not panic
	s.Delete("s1")                                                          // must not panic
}

func TestCheckpointStoreDelete(t *testing.T) {
	s := NewCheckpointStore()
	s.Store("s1", "K1", backend.CheckpointResp{CheckpointBlob: []byte("blob")})
	s.Delete("s1")
	if _, ok := s.Latest("s1"); ok {
		t.Fatal("expected the checkpoint to be gone after Delete")
	}
}
