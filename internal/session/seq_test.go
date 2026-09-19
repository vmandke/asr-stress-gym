package session

import "testing"

func TestSeqValidatorExpectedRun(t *testing.T) {
	v := NewSeqValidator()
	for seq := uint64(0); seq < 5; seq++ {
		outcome, _ := v.Validate(seq)
		if outcome != SeqExpected {
			t.Fatalf("seq %d: got %v, want SeqExpected", seq, outcome)
		}
	}
}

func TestSeqValidatorDuplicate(t *testing.T) {
	v := NewSeqValidator()
	v.Validate(0)
	v.Validate(1)
	outcome, _ := v.Validate(0) // a replayed frame after reconnect
	if outcome != SeqDuplicate {
		t.Fatalf("got %v, want SeqDuplicate", outcome)
	}
}

func TestSeqValidatorGapProducesDiscontinuityAndAdvances(t *testing.T) {
	v := NewSeqValidator()
	v.Validate(0)
	outcome, gap := v.Validate(5) // frames 1-4 lost
	if outcome != SeqGap {
		t.Fatalf("got %v, want SeqGap", outcome)
	}
	if gap.SeqStart != 1 || gap.SeqEnd != 5 {
		t.Fatalf("gap = [%d,%d], want [1,5]", gap.SeqStart, gap.SeqEnd)
	}
	// The validator must have advanced past the gap, matching
	// build-plan.md: "s.expectedSeq = f.Seq + 1".
	outcome, _ = v.Validate(6)
	if outcome != SeqExpected {
		t.Fatalf("after gap, seq 6: got %v, want SeqExpected", outcome)
	}
}

func TestSeqValidatorFirstFrameZero(t *testing.T) {
	v := NewSeqValidator()
	outcome, _ := v.Validate(0)
	if outcome != SeqExpected {
		t.Fatalf("first frame (seq 0): got %v, want SeqExpected", outcome)
	}
}
