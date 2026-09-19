package session

import "testing"

// Invariant 10: revision strictly increases within an utterance.
func TestPartialRevisionStrictlyIncreases(t *testing.T) {
	st := NewInferenceState("s1", ModeOnline)
	e := NewEmitter(st)

	var last uint64
	for i, text := range []string{"send", "send five", "send five thousand"} {
		ev := e.Partial(text)
		if i > 0 && ev.Revision <= last {
			t.Fatalf("revision did not strictly increase: %d -> %d", last, ev.Revision)
		}
		last = ev.Revision
		if ev.Text != text {
			t.Fatalf("got text %q, want %q", ev.Text, text)
		}
	}
}

// build-plan.md's own example: revision 17 [partial], 18 [partial.reset],
// 19 [partial] — reset is part of the SAME strictly-increasing sequence,
// not a restart of it.
func TestPartialResetContinuesTheRevisionSequence(t *testing.T) {
	st := NewInferenceState("s1", ModeOnline)
	e := NewEmitter(st)

	p1 := e.Partial("transfer five thousand to ram")
	reset := e.PartialReset()
	p2 := e.Partial("transfer five thousand to ramya")

	if !(p1.Revision < reset.Revision && reset.Revision < p2.Revision) {
		t.Fatalf("revisions not strictly increasing across reset: %d, %d, %d", p1.Revision, reset.Revision, p2.Revision)
	}
	if reset.Type != "partial.reset" || reset.UtteranceID != "u1" {
		t.Fatalf("got %+v", reset)
	}
}

func TestPartialResetAfterFinalPanics(t *testing.T) {
	st := NewInferenceState("s1", ModeOnline)
	e := NewEmitter(st)
	e.Final("done", 0, 10)

	defer func() {
		if r := recover(); r == nil {
			t.Fatal("expected panic: partial.reset emitted after final")
		}
	}()
	e.PartialReset()
}

// "duplicate final is a no-op, so delivery can be at-least-once."
func TestFinalIsIdempotent(t *testing.T) {
	st := NewInferenceState("s1", ModeOnline)
	e := NewEmitter(st)

	first, isNew := e.Final("send five thousand", 0, 100)
	if !isNew {
		t.Fatal("first Final call reported isNew=false")
	}
	second, isNew := e.Final("send five thousand DIFFERENT", 0, 999) // even with different args
	if isNew {
		t.Fatal("second Final call on an already-finalized utterance reported isNew=true")
	}
	if second != first {
		t.Fatalf("re-delivered final differs from the original: %+v vs %+v", second, first)
	}
}

// Invariant 9: no event follows a final for the same utterance_id.
func TestPartialAfterFinalPanics(t *testing.T) {
	st := NewInferenceState("s1", ModeOnline)
	e := NewEmitter(st)
	e.Final("done", 0, 10)

	defer func() {
		if r := recover(); r == nil {
			t.Fatal("expected panic: partial emitted after final")
		}
	}()
	e.Partial("this should never be emitted")
}

// Ack and Discontinuity are session/seq-scoped, not utterance-scoped, and
// must remain legal after the utterance they were about has finalized.
func TestAckAndDiscontinuityExemptFromFinalGuard(t *testing.T) {
	st := NewInferenceState("s1", ModeOnline)
	e := NewEmitter(st)
	e.Final("done", 0, 10)

	if ev := e.Ack(11); ev.HighestContiguousSeq != 11 {
		t.Fatalf("Ack after final: got %+v", ev)
	}
	if ev := e.Discontinuity(DiscontinuityRange{SeqStart: 12, SeqEnd: 15}); ev.SeqStart != 12 {
		t.Fatalf("Discontinuity after final: got %+v", ev)
	}
}

func TestUtteranceLabelFormat(t *testing.T) {
	st := NewInferenceState("s1", ModeOnline)
	if got := st.UtteranceLabel(); got != "u1" {
		t.Fatalf("got %q, want %q", got, "u1")
	}
}

// A latent bug caught while building M2: Finalized/lastFinal being
// session-wide (rather than scoped to the current utterance) would have
// permanently blocked every later utterance's partials the moment ANY
// utterance finalized. NewUtterance's reset is the fix; this proves each
// utterance's lifecycle is independent, not just that the first one works.
func TestUtterancesAreIndependentAfterNewUtterance(t *testing.T) {
	st := NewInferenceState("s1", ModeOnline)
	e := NewEmitter(st)

	e.Partial("first utterance partial")
	first, isNew := e.Final("first utterance final", 0, 10)
	if !isNew || first.UtteranceID != "u1" {
		t.Fatalf("got %+v isNew=%v, want u1's fresh final", first, isNew)
	}

	st.NewUtterance()
	if st.UtteranceID != 2 || st.Finalized {
		t.Fatalf("NewUtterance: UtteranceID=%d Finalized=%v, want 2/false", st.UtteranceID, st.Finalized)
	}

	// Without the fix, this would panic: mustNotBeFinalized would still
	// see the OLD utterance's Finalized=true.
	p := e.Partial("second utterance partial")
	if p.UtteranceID != "u2" {
		t.Fatalf("got utterance_id %q, want u2", p.UtteranceID)
	}
	if p.Revision != 1 {
		t.Fatalf("revision = %d after NewUtterance, want 1 (reset, not accumulated from u1)", p.Revision)
	}

	second, isNew := e.Final("second utterance final", 11, 20)
	if !isNew || second.UtteranceID != "u2" {
		t.Fatalf("got %+v isNew=%v, want u2's fresh final", second, isNew)
	}
	if second == first {
		t.Fatal("u2's final is identical to u1's — dedupe leaked across the utterance transition")
	}

	// A further Final() call now must dedupe against u2 (the CURRENT
	// utterance), not resurrect or re-check against u1 — the text/seq
	// arguments here are irrelevant and ignored precisely because this is
	// the idempotent-replay path.
	replay, isNew := e.Final("ignored: state.Finalized is already true", 0, 0)
	if isNew {
		t.Fatal("Final() after a NewUtterance transition reported isNew=true for what should be u2's already-finalized state")
	}
	if replay != second {
		t.Fatalf("re-delivery returned %+v, want the CURRENT (u2) final %+v", replay, second)
	}
}

// Invariant 11: the three counters never collapse into one. Sanity-checks
// the struct shape; real epoch-bumping logic (on reconnect / failover)
// lands at M1-reconnect and M3 respectively.
func TestEpochCountersAreIndependent(t *testing.T) {
	st := NewInferenceState("s1", ModeOnline)
	st.StreamEpoch = 3
	if st.FailoverEpoch != 0 || st.Generation != 0 {
		t.Fatalf("bumping StreamEpoch affected other counters: %+v", st)
	}
	st.FailoverEpoch = 7
	if st.StreamEpoch != 3 || st.Generation != 0 {
		t.Fatalf("bumping FailoverEpoch affected other counters: %+v", st)
	}
}
