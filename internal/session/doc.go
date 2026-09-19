// Package session owns SessionInferenceState: the three counters
// (StreamEpoch, FailoverEpoch, Generation), the transcript emission
// contract (revision monotonicity, final immutability, finals dedupe),
// and utterance lifecycle. Filled in at M1-M2.
package session
