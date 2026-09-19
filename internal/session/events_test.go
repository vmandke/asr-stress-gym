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
