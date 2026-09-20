package main

import (
	"bytes"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestPutGetRoundTripPreservesBytesAndCompatKey(t *testing.T) {
	tr := NewTier(1<<20, time.Minute)
	blob := []byte{0x00, 0xFF, 0x42, 0x00} // NULs: the store must be binary-safe
	tr.Put("s1:0", blob, "sha256:abc")

	e, ok := tr.Get("s1:0")
	if !ok {
		t.Fatal("expected a hit")
	}
	if !bytes.Equal(e.blob, blob) {
		t.Errorf("blob = %v, want %v", e.blob, blob)
	}
	if e.compatKey != "sha256:abc" {
		t.Errorf("compatKey = %q, want sha256:abc", e.compatKey)
	}
}

func TestGetMissIsOrdinary(t *testing.T) {
	tr := NewTier(1<<20, time.Minute)
	if _, ok := tr.Get("nope"); ok {
		t.Fatal("expected a miss")
	}
	if got := tr.Stats().Misses; got != 1 {
		t.Errorf("misses = %d, want 1", got)
	}
}

// The property the whole retry story rests on: writing version N must not
// disturb version N-1. If a worker dies after reading N-1 and before
// writing N, the retry on a peer has to find N-1 intact. Overwriting in
// place would turn a recoverable fault into a corrupted session.
func TestWritingANewVersionLeavesThePredecessorIntact(t *testing.T) {
	tr := NewTier(1<<20, time.Minute)
	tr.Put("s1:41", []byte("state-at-41"), "k")
	tr.Put("s1:42", []byte("state-at-42"), "k")

	prev, ok := tr.Get("s1:41")
	if !ok {
		t.Fatal("predecessor was lost when its successor was written")
	}
	if string(prev.blob) != "state-at-41" {
		t.Errorf("predecessor mutated: %q", prev.blob)
	}
}

func TestTTLExpiryCountsAsAMiss(t *testing.T) {
	tr := NewTier(1<<20, 10*time.Millisecond)
	tr.Put("s1:0", []byte("x"), "k")
	time.Sleep(25 * time.Millisecond)

	if _, ok := tr.Get("s1:0"); ok {
		t.Fatal("expected the entry to have expired")
	}
	if got := tr.Stats().Expiries; got != 1 {
		t.Errorf("expiries = %d, want 1", got)
	}
	// Expiry must also reclaim the bytes, or the ceiling drifts upward
	// until nothing can be stored at all.
	if got := tr.Stats().Bytes; got != 0 {
		t.Errorf("bytes = %d after expiry, want 0", got)
	}
}

func TestEvictionHonoursTheByteCeilingAndDropsTheColdest(t *testing.T) {
	tr := NewTier(30, time.Minute) // room for three 10-byte blobs
	ten := make([]byte, 10)
	tr.Put("a", ten, "k")
	tr.Put("b", ten, "k")
	tr.Put("c", ten, "k")

	// Touch "a" so "b" becomes the coldest, then overflow.
	if _, ok := tr.Get("a"); !ok {
		t.Fatal("a should still be resident")
	}
	tr.Put("d", ten, "k")

	if got := tr.Stats().Bytes; got > 30 {
		t.Errorf("bytes = %d, exceeds the 30-byte ceiling", got)
	}
	if _, ok := tr.Get("b"); ok {
		t.Error("b was the least recently used and should have been evicted")
	}
	if _, ok := tr.Get("a"); !ok {
		t.Error("a was touched most recently and should have survived")
	}
}

func TestOverwritingAKeyDoesNotDoubleCountBytes(t *testing.T) {
	tr := NewTier(1<<20, time.Minute)
	tr.Put("k", make([]byte, 100), "k")
	tr.Put("k", make([]byte, 100), "k")
	if got := tr.Stats().Bytes; got != 100 {
		t.Errorf("bytes = %d, want 100 — the replaced blob was not reclaimed", got)
	}
}

func TestDeletePrefixReclaimsOneSessionAndLeavesOthers(t *testing.T) {
	tr := NewTier(1<<20, time.Minute)
	for i := range 4 {
		tr.Put(fmt.Sprintf("sessA:%d", i), make([]byte, 10), "k")
	}
	tr.Put("sessB:0", make([]byte, 10), "k")

	if n := tr.DeletePrefix("sessA:"); n != 4 {
		t.Errorf("deleted %d, want 4", n)
	}
	if _, ok := tr.Get("sessB:0"); !ok {
		t.Error("a different session's state was collected")
	}
	if got := tr.Stats().Bytes; got != 10 {
		t.Errorf("bytes = %d, want 10", got)
	}
}

// A state reference is a prefix of its own successors, so an exact delete
// must not be implemented as a prefix delete: retiring "s1:10" would also
// take "s1:100" and "s1:101". That is silent state loss for any session
// long enough to reach three-digit versions, surfacing as a 424 much later
// and nowhere near its cause.
func TestDeletingOneVersionDoesNotTakeItsSuccessors(t *testing.T) {
	tr := NewTier(1<<20, time.Minute)
	for _, k := range []string{"s1:10", "s1:100", "s1:101"} {
		tr.Put(k, []byte("state"), "k")
	}
	if !tr.Delete("s1:10") {
		t.Fatal("expected s1:10 to be present")
	}
	for _, k := range []string{"s1:100", "s1:101"} {
		if _, ok := tr.Get(k); !ok {
			t.Errorf("%s was collected by an exact delete of s1:10", k)
		}
	}
	if got := tr.Stats().Bytes; got != 10 {
		t.Errorf("bytes = %d, want 10 (two 5-byte survivors)", got)
	}
}

func TestDeleteOfAnAbsentKeyIsHarmless(t *testing.T) {
	tr := NewTier(1<<20, time.Minute)
	if tr.Delete("never-existed") {
		t.Error("Delete reported removing something that was not there")
	}
}

func TestHTTPMissIs404AndHitCarriesTheCompatHeader(t *testing.T) {
	tr := NewTier(1<<20, time.Minute)
	srv := httptest.NewServer(http.HandlerFunc(tr.handleKV))
	defer srv.Close()

	blob := []byte("tensors")
	req, _ := http.NewRequest(http.MethodPut, srv.URL+"/kv/s1:0", bytes.NewReader(blob))
	req.Header.Set(compatHeader, "sha256:fam1")
	if resp, err := http.DefaultClient.Do(req); err != nil {
		t.Fatal(err)
	} else {
		resp.Body.Close()
	}

	resp, err := http.Get(srv.URL + "/kv/s1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	if got := resp.Header.Get(compatHeader); got != "sha256:fam1" {
		t.Errorf("%s = %q, want sha256:fam1 — a reader must be able to check\n"+
			"family compatibility BEFORE deserializing", compatHeader, got)
	}

	miss, err := http.Get(srv.URL + "/kv/absent")
	if err != nil {
		t.Fatal(err)
	}
	defer miss.Body.Close()
	if miss.StatusCode != http.StatusNotFound {
		t.Errorf("miss status = %d, want 404", miss.StatusCode)
	}
}

// Create-only is what makes "immutable version" true rather than merely
// intended. Two attempts at the same chunk — the original and a Bifrost
// retry — name the same state_sink, so without this the slower one
// silently overwrites the faster one's result.
func TestAVersionCannotBeRewritten(t *testing.T) {
	tr := NewTier(1<<20, time.Minute)
	if !tr.PutIfAbsent("s1:100", []byte("first"), "k") {
		t.Fatal("the first write should have been accepted")
	}
	if tr.PutIfAbsent("s1:100", []byte("second"), "k") {
		t.Fatal("the second write was accepted; the version is not immutable")
	}
	e, _ := tr.Get("s1:100")
	if string(e.blob) != "first" {
		t.Errorf("blob = %q, want the original %q", e.blob, "first")
	}
	if got := tr.Stats().Conflicts; got != 1 {
		t.Errorf("conflicts = %d, want 1", got)
	}
}

// A refused write must not be reported as a server failure: the caller
// only needs the version to EXIST, and it does.
func TestRewritingAVersionOverHTTPIs409(t *testing.T) {
	tr := NewTier(1<<20, time.Minute)
	srv := httptest.NewServer(http.HandlerFunc(tr.handleKV))
	defer srv.Close()

	put := func(body string) int {
		req, _ := http.NewRequest(http.MethodPut, srv.URL+"/kv/s1:100", strings.NewReader(body))
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		return resp.StatusCode
	}
	if got := put("first"); got != http.StatusOK {
		t.Fatalf("first put = %d, want 200", got)
	}
	if got := put("second"); got != http.StatusConflict {
		t.Errorf("second put = %d, want 409", got)
	}
}

// PutIfAbsent must test-and-set under one lock acquisition. Checking under
// one and writing under another would let a second writer slip in between
// and defeat the whole point.
func TestConcurrentWritesOfOneVersionElectExactlyOneWinner(t *testing.T) {
	tr := NewTier(1<<20, time.Minute)
	const racers = 32
	var wins atomic.Int64
	var wg sync.WaitGroup
	start := make(chan struct{})
	for i := range racers {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			if tr.PutIfAbsent("s1:100", []byte{byte(i)}, "k") {
				wins.Add(1)
			}
		}(i)
	}
	close(start)
	wg.Wait()

	if got := wins.Load(); got != 1 {
		t.Errorf("%d writers believed they created the version, want exactly 1", got)
	}
	if got := tr.Stats().Conflicts; got != racers-1 {
		t.Errorf("conflicts = %d, want %d", got, racers-1)
	}
}
