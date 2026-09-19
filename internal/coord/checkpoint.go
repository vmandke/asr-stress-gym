// Package coord is the Session Coordinator: drives both failover recovery
// algorithms (same-model checkpoint+tail-replay, cross-model fresh-state+
// full-replay) and the bounded failure-handling loop. This is the thesis
// of the project — see docs/implementation-plan.md and docs/build-plan.md
// "Failover".
package coord

import (
	"crypto/sha256"
	"encoding/hex"
	"sync"

	"asr-stress-gym/internal/backend"
	"asr-stress-gym/internal/session"
)

// CacheCheckpoint mirrors build-plan.md's "Checkpoint format", held
// GATEWAY-side (not worker-side): see internal/backend.CheckpointResp's
// doc comment for why — a checkpoint has to survive the death of the
// worker it's about, same as the audio journal, so neither can live only
// in that worker's own memory.
type CacheCheckpoint struct {
	SessionID        string
	CompatibilityKey session.CacheCompatibilityKey
	Generation       uint64
	Seq              uint64 // LastSeqApplied at the moment this was taken
	StateBlob        []byte
	Checksum         string
}

func checksum(blob []byte) string {
	sum := sha256.Sum256(blob)
	return hex.EncodeToString(sum[:])
}

func newCheckpoint(sessionID string, key session.CacheCompatibilityKey, resp backend.CheckpointResp) CacheCheckpoint {
	return CacheCheckpoint{
		SessionID:        sessionID,
		CompatibilityKey: key,
		Generation:       resp.Generation,
		Seq:              resp.LastSeqApplied,
		StateBlob:        resp.CheckpointBlob,
		Checksum:         checksum(resp.CheckpointBlob),
	}
}

// CanRestore — build-plan.md: "Validation failure means audio replay.
// Never partial-restore, never coerce." Checked BEFORE ever calling a
// worker's Restore, so a corrupted or incompatible checkpoint degrades
// gracefully without a wasted round trip.
func CanRestore(cp CacheCheckpoint, targetKey session.CacheCompatibilityKey) bool {
	return cp.CompatibilityKey == targetKey && cp.Checksum == checksum(cp.StateBlob)
}

// CheckpointStore holds the latest checkpoint per session, gateway-side.
// One instance per gateway process (like router.Router), shared across
// every connection — but each session only ever touches its own entry,
// so contention is a single map-wide lock over a cheap operation, not a
// bottleneck at this project's scale.
type CheckpointStore struct {
	mu     sync.Mutex
	latest map[string]CacheCheckpoint
}

func NewCheckpointStore() *CheckpointStore {
	return &CheckpointStore{latest: make(map[string]CacheCheckpoint)}
}

// Store records resp as sessionID's latest checkpoint, computing its
// checksum now rather than trusting one from the wire. A nil receiver is
// a safe no-op (see Latest's doc comment for why that matters).
func (s *CheckpointStore) Store(sessionID string, key session.CacheCompatibilityKey, resp backend.CheckpointResp) {
	if s == nil {
		return
	}
	cp := newCheckpoint(sessionID, key, resp)
	s.mu.Lock()
	defer s.mu.Unlock()
	s.latest[sessionID] = cp
}

// Latest is deliberately safe to call on a nil *CheckpointStore, reporting
// ok=false — "no checkpointing configured" and "no checkpoint found for
// this session" are the same thing to RecoverSameModel, which already
// degrades to RecoverCrossModel on ok=false (build-plan.md: "Validation
// failure means audio replay... session still succeeds", invariant 13:
// checkpoint failure must never take down healthy inference). Requiring
// every construction path to remember a non-nil store, on pain of a
// panic mid-failover, would be the wrong place to enforce that.
func (s *CheckpointStore) Latest(sessionID string) (CacheCheckpoint, bool) {
	if s == nil {
		return CacheCheckpoint{}, false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	cp, ok := s.latest[sessionID]
	return cp, ok
}

// Corrupt deliberately damages sessionID's stored checkpoint — the
// gateway-side half of chaos scenario 5 (build-plan.md demo 5: "corrupt
// checkpoint... validation fails, falls back to audio replay, session
// still succeeds"). Flips the recorded checksum, not the blob: either
// would make CanRestore fail, but corrupting the checksum is what a
// storage-layer bit-flip would actually look like in practice, and
// doesn't require the blob to still be well-formed to exercise the same
// path. Returns false if there is nothing stored yet for this session.
func (s *CheckpointStore) Corrupt(sessionID string) bool {
	if s == nil {
		return false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	cp, ok := s.latest[sessionID]
	if !ok {
		return false
	}
	cp.Checksum = "corrupted-by-chaos-scenario"
	s.latest[sessionID] = cp
	return true
}

// Delete removes sessionID's checkpoint — called on session close so this
// map doesn't grow unboundedly across the gateway process's lifetime.
func (s *CheckpointStore) Delete(sessionID string) {
	if s == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.latest, sessionID)
}
