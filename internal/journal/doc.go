// Package journal is the bounded, model-independent audio recovery store
// (Record) plus the dispatch log of chunk spans (Span) that recovery replays
// through audio.Recut. It stores opaque payloads only. Filled in at M2.
package journal
