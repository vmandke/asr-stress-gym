// Command kvtier is the shared KV-cache tier: the piece that makes
// stateless routing through Bifrost possible.
//
// **The problem it solves.** A streaming ASR session builds inference
// state (worker/kvcache: 35 attention tensors for zipformer, 1.09 MB).
// While that state lives only inside one worker process, every chunk of
// the session must return to that worker — the session is PINNED, and a
// load balancer in front of the fleet can do nothing but honour the pin.
// That is why Bifrost carries only offline finals today.
//
// The obvious fix — send the state with every request — was measured and
// rejected: at a 160 ms chunk policy the round trip is 427x the audio it
// carries (fp32), 107x at int8. See docs/BIFROST-KVCACHE.md.
//
// **What production does instead**, and what this implements: move the
// state OUT of the worker into a shared tier, and put a *reference* in the
// request. Mooncake pools cluster DRAM/SSD and moves KV by RDMA; LMCache
// and NVIDIA Dynamo/NIXL have the same shape. The invariant everywhere is
// the same one this service exists to enforce:
//
//	the control request goes through the load balancer;
//	the state bytes never do.
//
// Through Bifrost: the audio plus ~64 bytes of key. Between worker and
// tier: the 1.09 MB, on a direct hop Bifrost never sees.
//
// **Immutable versions, not mutation in place.** A chunk reads key N-1 and
// writes key N; it never overwrites its own input. This is what makes
// retry safe. If a worker dies after reading N-1 and before writing N,
// Bifrost retries the chunk on a peer, which reads the *same, intact* N-1
// and succeeds. Overwriting in place would destroy the input the retry
// depends on, turning a recoverable fault into a corrupted session — the
// exact failure mode this repository exists to prevent.
//
// Old versions are reclaimed by TTL and by an explicit prefix delete when
// a session ends; the byte ceiling is enforced by LRU so the tier's memory
// is a hard bound rather than a hope.
//
// **The tier never parses a blob.** It stores opaque bytes and echoes back
// whatever compatibility key the writer advertised. That keeps rule 2 of
// docs/build-plan.md true one layer further out: the coordinator never
// knows a backend is a model, and neither does the cache tier. Validation
// that a blob may be loaded at all is the worker's job, in
// worker/kvcache/serde.py, which refuses a foreign key with a 422.
package main

import (
	"container/list"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type entry struct {
	key       string
	blob      []byte
	compatKey string // advertised by the writer; never interpreted here
	stored    time.Time
	el        *list.Element
}

// Tier is an in-memory, byte-bounded, TTL'd blob store with LRU eviction.
//
// In-memory on purpose. The production analogue is a distributed store
// over RDMA, and the mechanism this repo is demonstrating — reference in
// the request, bytes on a side channel — is identical either way. Saying
// so plainly is better than a Redis dependency that would make the demo
// look more production-shaped than it is.
type Tier struct {
	mu       sync.Mutex
	m        map[string]*entry
	lru      *list.List // front = most recently used
	bytes    int64
	maxBytes int64
	ttl      time.Duration

	puts, gets, hits, misses     atomic.Int64
	evictions, expiries, deletes atomic.Int64
	conflicts                    atomic.Int64 // create-only writes refused; see PutIfAbsent
}

func NewTier(maxBytes int64, ttl time.Duration) *Tier {
	return &Tier{
		m:        map[string]*entry{},
		lru:      list.New(),
		maxBytes: maxBytes,
		ttl:      ttl,
	}
}

func (t *Tier) Put(key string, blob []byte, compatKey string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.putLocked(key, blob, compatKey)
}

// putLocked is the store itself; callers hold t.mu. Split out so
// PutIfAbsent can test-and-set atomically rather than checking under one
// acquisition and writing under another, which would let a second writer
// slip in between and defeat the whole point.
func (t *Tier) putLocked(key string, blob []byte, compatKey string) {
	t.puts.Add(1)

	if old, ok := t.m[key]; ok {
		t.bytes -= int64(len(old.blob))
		t.lru.Remove(old.el)
		delete(t.m, key)
	}
	e := &entry{key: key, blob: blob, compatKey: compatKey, stored: time.Now()}
	e.el = t.lru.PushFront(e)
	t.m[key] = e
	t.bytes += int64(len(blob))

	// Evict from the cold end until we are back under the ceiling. A
	// session whose state is evicted mid-utterance does not corrupt: the
	// next read misses, and the caller's recovery path rebuilds by
	// replaying audio. Losing a cache costs recomputation, never
	// correctness — that property is what makes this safe to bound.
	for t.bytes > t.maxBytes && t.lru.Len() > 0 {
		back := t.lru.Back()
		ev := back.Value.(*entry)
		t.lru.Remove(back)
		delete(t.m, ev.key)
		t.bytes -= int64(len(ev.blob))
		t.evictions.Add(1)
	}
}

// PutIfAbsent stores a version only if that version does not already
// exist, reporting whether it was stored.
//
// This is what makes "immutable version" true rather than merely intended.
// Two attempts at the same chunk — the original and a Bifrost retry — name
// the same state_sink, so without create-only semantics the slower one
// silently overwrites the faster one's result. When both computed from the
// same predecessor that is harmless, and when they did not it is exactly
// the corruption the version scheme exists to prevent. Refusing the second
// write costs nothing: the caller only needs the version to EXIST, and it
// does.
func (t *Tier) PutIfAbsent(key string, blob []byte, compatKey string) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	if _, exists := t.m[key]; exists {
		t.conflicts.Add(1)
		return false
	}
	t.putLocked(key, blob, compatKey)
	return true
}

func (t *Tier) Get(key string) (*entry, bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.gets.Add(1)
	e, ok := t.m[key]
	if !ok {
		t.misses.Add(1)
		return nil, false
	}
	if t.ttl > 0 && time.Since(e.stored) > t.ttl {
		t.lru.Remove(e.el)
		delete(t.m, key)
		t.bytes -= int64(len(e.blob))
		t.expiries.Add(1)
		t.misses.Add(1)
		return nil, false
	}
	t.lru.MoveToFront(e.el)
	t.hits.Add(1)
	return e, true
}

// Delete removes exactly one version.
//
// Separate from DeletePrefix rather than a special case of it, because a
// state reference is a prefix of its own successors: deleting "s1:10" as a
// prefix would also take "s1:100" and "s1:101". That is silent state loss
// for a session that has merely run long enough to reach three-digit
// versions, and it surfaces as a 424 much later, nowhere near the cause.
func (t *Tier) Delete(key string) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	e, ok := t.m[key]
	if !ok {
		return false
	}
	t.lru.Remove(e.el)
	delete(t.m, key)
	t.bytes -= int64(len(e.blob))
	t.deletes.Add(1)
	return true
}

// DeletePrefix reclaims every version a session produced. Called when a
// session ends; TTL is the backstop for sessions that end by dying.
func (t *Tier) DeletePrefix(prefix string) int {
	t.mu.Lock()
	defer t.mu.Unlock()
	n := 0
	for k, e := range t.m {
		if strings.HasPrefix(k, prefix) {
			t.lru.Remove(e.el)
			delete(t.m, k)
			t.bytes -= int64(len(e.blob))
			n++
		}
	}
	t.deletes.Add(int64(n))
	return n
}

func (t *Tier) sweep() {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.ttl <= 0 {
		return
	}
	for k, e := range t.m {
		if time.Since(e.stored) > t.ttl {
			t.lru.Remove(e.el)
			delete(t.m, k)
			t.bytes -= int64(len(e.blob))
			t.expiries.Add(1)
		}
	}
}

type stats struct {
	Keys      int   `json:"keys"`
	Bytes     int64 `json:"bytes"`
	MaxBytes  int64 `json:"max_bytes"`
	Puts      int64 `json:"puts"`
	Gets      int64 `json:"gets"`
	Hits      int64 `json:"hits"`
	Misses    int64 `json:"misses"`
	Evictions int64 `json:"evictions"`
	Expiries  int64 `json:"expiries"`
	Deletes   int64 `json:"deletes"`
	Conflicts int64 `json:"conflicts"`
}

// keyInfo describes one resident version without exposing its bytes. The
// tier never parses a blob and this does not start: it reports size and
// age, which is what "show me what is cached" actually needs.
type keyInfo struct {
	Key       string `json:"key"`
	Bytes     int    `json:"bytes"`
	AgeMs     int64  `json:"age_ms"`
	CompatKey string `json:"compat_key"`
}

// Keys lists resident versions, optionally filtered by prefix (a session's
// own state is one prefix). Bounded: a tier under load holds thousands and
// a UI needs the newest handful, not all of them.
func (t *Tier) Keys(prefix string, limit int) []keyInfo {
	t.mu.Lock()
	defer t.mu.Unlock()
	out := make([]keyInfo, 0, len(t.m))
	now := time.Now()
	for k, e := range t.m {
		if prefix != "" && !strings.HasPrefix(k, prefix) {
			continue
		}
		out = append(out, keyInfo{
			Key: k, Bytes: len(e.blob),
			AgeMs:     now.Sub(e.stored).Milliseconds(),
			CompatKey: e.compatKey,
		})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].AgeMs < out[j].AgeMs })
	if limit > 0 && len(out) > limit {
		out = out[:limit]
	}
	return out
}

func (t *Tier) Stats() stats {
	t.mu.Lock()
	keys, b := len(t.m), t.bytes
	t.mu.Unlock()
	return stats{
		Keys: keys, Bytes: b, MaxBytes: t.maxBytes,
		Puts: t.puts.Load(), Gets: t.gets.Load(),
		Hits: t.hits.Load(), Misses: t.misses.Load(),
		Evictions: t.evictions.Load(), Expiries: t.expiries.Load(),
		Deletes: t.deletes.Load(), Conflicts: t.conflicts.Load(),
	}
}

const compatHeader = "X-Compat-Key"

func (t *Tier) handleKV(w http.ResponseWriter, r *http.Request) {
	key := strings.TrimPrefix(r.URL.Path, "/kv/")
	if key == "" && r.Method != http.MethodDelete {
		http.Error(w, "missing key", http.StatusBadRequest)
		return
	}

	switch r.Method {
	case http.MethodPut, http.MethodPost:
		blob, err := io.ReadAll(io.LimitReader(r.Body, 64<<20))
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		// Create-only: a version is written once and never rewritten. A
		// retry of the same chunk names the same key, and the caller only
		// needs it to EXIST — so 409 is information, not a failure, and
		// the worker treats it as success. See PutIfAbsent.
		if !t.PutIfAbsent(key, blob, r.Header.Get(compatHeader)) {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusConflict)
			json.NewEncoder(w).Encode(map[string]any{"key": key, "exists": true})
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{"key": key, "bytes": len(blob)})

	case http.MethodGet:
		e, ok := t.Get(key)
		if !ok {
			// A miss is ordinary, not exceptional: TTL, eviction, or a
			// session whose first chunk has no predecessor. The caller
			// decides what to do (start fresh, or replay).
			http.Error(w, "miss", http.StatusNotFound)
			return
		}
		w.Header().Set("Content-Type", "application/octet-stream")
		w.Header().Set(compatHeader, e.compatKey)
		w.Write(e.blob)

	case http.MethodDelete:
		if p := r.URL.Query().Get("prefix"); p != "" {
			n := t.DeletePrefix(p)
			json.NewEncoder(w).Encode(map[string]any{"deleted": n})
			return
		}
		t.Delete(key) // exactly this version — see Delete
		w.WriteHeader(http.StatusNoContent)

	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

func main() {
	addr := flag.String("addr", ":9500", "listen address")
	maxMB := flag.Int64("max-mb", 512, "byte ceiling before LRU eviction")
	ttl := flag.Duration("ttl", 5*time.Minute, "max age of a stored version")
	flag.Parse()

	t := NewTier(*maxMB<<20, *ttl)

	go func() {
		tick := time.NewTicker(30 * time.Second)
		defer tick.Stop()
		for range tick.C {
			t.sweep()
		}
	}()

	mux := http.NewServeMux()
	mux.HandleFunc("/kv/", t.handleKV)
	mux.HandleFunc("/kv", t.handleKV)
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintln(w, "ok")
	})
	mux.HandleFunc("/stats", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(t.Stats())
	})
	mux.HandleFunc("/keys", func(w http.ResponseWriter, r *http.Request) {
		limit := 50
		if n, err := strconv.Atoi(r.URL.Query().Get("limit")); err == nil && n > 0 {
			limit = n
		}
		keys := t.Keys(r.URL.Query().Get("prefix"), limit)
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{"keys": keys, "count": len(keys)})
	})

	log.Printf("kvtier: listening on %s (max %d MB, ttl %s)", *addr, *maxMB, *ttl)
	srv := &http.Server{
		Addr:    *addr,
		Handler: mux,
		// Generous: a 5.51 MB whisper blob over a container network is
		// still fast, but a loaded host can stall.
		ReadTimeout:  30 * time.Second,
		WriteTimeout: 30 * time.Second,
	}
	log.Fatal(srv.ListenAndServe())
}
