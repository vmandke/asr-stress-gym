// Command chaostest drives docs/build-plan.md's chaos scenarios 2, 3 and
// 5 against the REAL docker-compose stack — the M3 done-when bar
// (docs/implementation-plan.md): "chaos scenarios 2, 3 and 5 pass
// headlessly with non-zero exit on breach, duplicate_finals_total == 0
// in all three." internal/coord's and cmd/gateway's own tests already
// prove the recovery algorithms and the WS wiring correct against fakes;
// this is what proves the same thing true of the actual containers,
// the actual Python workers, and the actual supervisor process split —
// exactly the layer where M0-M2 found real bugs unit tests couldn't.
//
// Each scenario:
//   - opens a session and streams enough corpus audio to get pinned to a
//     worker and let one async checkpoint land,
//   - determines which worker actually got pinned by polling /health's
//     active_sessions (Pick's tie-breaking among equally-scored
//     candidates isn't deterministic — see cmd/gateway's own tests for
//     the same problem solved differently, via a push counter, which
//     isn't available to an external black-box tool like this one),
//   - injects the scenario's specific fault via each worker's admin port
//     (docs/PROTOCOL.md "Fault injection") or the gateway's debug
//     checkpoint-corruption hook,
//   - streams more audio and asserts recovery: partial.reset arrives,
//     the session keeps serving, the final is non-empty, and the
//     GATEWAY's own metrics snapshot confirms which recovery MODE fired
//     and that duplicate_finals_total stayed at zero.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"time"

	"github.com/coder/websocket"

	"asr-stress-gym/internal/corpus"
	"asr-stress-gym/internal/wire"
)

type workerRef struct {
	id        string
	healthURL string // host-mapped port 9000 — GET /health (active_sessions)
	adminURL  string // host-mapped port 9001 — POST /admin/die|restore
}

func defaultFleet() map[string]workerRef {
	return map[string]workerRef{
		"worker-a": {id: "worker-a", healthURL: envOr("WORKER_A_URL", "http://localhost:18001"), adminURL: envOr("WORKER_A_ADMIN_URL", "http://localhost:19001")},
		"worker-b": {id: "worker-b", healthURL: envOr("WORKER_B_URL", "http://localhost:18002"), adminURL: envOr("WORKER_B_ADMIN_URL", "http://localhost:19002")},
		"worker-c": {id: "worker-c", healthURL: envOr("WORKER_C_URL", "http://localhost:18003"), adminURL: envOr("WORKER_C_ADMIN_URL", "http://localhost:19003")},
		// The KV pair (M11), behind the `kv` compose profile. Listed
		// unconditionally: scenarios 12/13 probe whether they answer and
		// SKIP if they do not, which is cheaper and clearer than making
		// the fleet map itself conditional on a profile.
		"worker-f": {id: "worker-f", healthURL: envOr("WORKER_F_URL", "http://localhost:18007"), adminURL: envOr("WORKER_F_ADMIN_URL", "http://localhost:19007")},
		"worker-g": {id: "worker-g", healthURL: envOr("WORKER_G_URL", "http://localhost:18008"), adminURL: envOr("WORKER_G_ADMIN_URL", "http://localhost:19008")},
		"worker-h": {id: "worker-h", healthURL: envOr("WORKER_H_URL", "http://localhost:18009"), adminURL: envOr("WORKER_H_ADMIN_URL", "http://localhost:19009")},
	}
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func main() {
	scenario := flag.Int("scenario", 0, "chaos scenario to run: 2, 3, 4, 5, 6, 7, 10 or 11 (9 is scripts/scenarios/09_overload.sh)")
	wsURL := flag.String("ws-url", envOr("GATEWAY_WS_URL", "ws://localhost:7070/ws"), "gateway WebSocket URL")
	gatewayURL := flag.String("gateway-url", envOr("GATEWAY_DEBUG_URL", "http://localhost:7000"), "gateway dashboard/debug HTTP URL")
	clipPath := flag.String("clip", "", "corpus WAV clip to stream (default: the first clip in "+corpus.Dir+")")
	flag.Parse()

	// Default to the large corpus (docs/BENCH.md): 10-20s utterances, not
	// the 1-3s committed clips. A scenario that fails over mid-utterance
	// needs an utterance long enough to still be in progress when the
	// fault lands.
	if *clipPath == "" {
		c, err := corpus.AnyClip()
		if err != nil {
			log.Fatalf("chaostest: %v", err)
		}
		*clipPath = c
	}

	fleet := defaultFleet()
	if *scenario == -1 {
		// --scenario -1: diagnostic, not a chaos scenario. Opens one
		// session and prints every worker's active_sessions, so a router
		// or fleet-config problem shows up directly rather than as an
		// opaque "did not pin to any candidate" failure three layers up.
		// This is exactly how the tie-breaking bug below was found.
		ctx := context.Background()
		s, err := openSession(ctx, *wsURL)
		if err != nil {
			log.Fatalf("openSession: %v", err)
		}
		log.Printf("session_id=%s", s.sessionID)
		for _, id := range []string{"worker-zip-1", "worker-zip-2", "worker-ctc-1", "worker-ctc-2", "worker-whisper-1", "worker-whisper-2"} {
			n, err := activeSessions(fleet[id].healthURL)
			log.Printf("%s: active_sessions=%d err=%v", id, n, err)
		}
		s.close()
		return
	}

	var err error
	switch *scenario {
	case 2:
		err = scenario2SameModelCrash(*wsURL, *gatewayURL, *clipPath, fleet)
	case 3:
		err = scenario3CrossModelCrash(*wsURL, *gatewayURL, *clipPath, fleet)
	case 4:
		err = scenario4RateLimitStorm(*wsURL, *gatewayURL, *clipPath, fleet)
	case 5:
		err = scenario5CorruptCheckpoint(*wsURL, *gatewayURL, *clipPath, fleet)
	case 6:
		err = scenario6GrayFailure(*wsURL, *gatewayURL, *clipPath, fleet)
	case 7:
		err = scenario7Blackhole(*wsURL, *gatewayURL, *clipPath, fleet)
	case 10:
		err = scenario10LongSilence(*wsURL, *gatewayURL, fleet)
	case 11:
		err = scenario11AllBackendsDown(*wsURL, *gatewayURL, fleet)
	case 12:
		err = scenario12KVCheckpointRestore(*wsURL, *gatewayURL, *clipPath, fleet)
	case 13:
		err = scenario13CorruptKVCheckpointDegrades(*wsURL, *gatewayURL, *clipPath, fleet)
	default:
		log.Fatalf("--scenario must be one of 2, 3, 4, 5, 6, 7, 10, 11, 12, 13 (got %d). Scenario 9 (overload) is scripts/scenarios/09_overload.sh — it needs the gateway restarted with a lowered capacity envelope, which is a shell concern", *scenario)
	}
	if isSkip(err) {
		// A precondition was absent (e.g. the kv profile is not running).
		// Not a failure: a fleet that was never asked to start those
		// workers is not a broken fleet, and exiting non-zero here would
		// make `make chaos` fail for the default profile.
		log.Printf("SKIP scenario %d: %v", *scenario, err)
		return
	}
	if err != nil {
		log.Printf("FAIL scenario %d: %v", *scenario, err)
		os.Exit(1)
	}
	log.Printf("PASS scenario %d", *scenario)
}

// --- metrics: the gateway's debug snapshot, before/after comparison ---

type metricsSnap struct {
	DuplicateFinalsTotal       int64 `json:"duplicate_finals_total"`
	StaleGenerationWritesTotal int64 `json:"stale_generation_writes_total"`
	FailoverTotal              int64 `json:"failover_total"`
	FailoverSameModelTotal     int64 `json:"failover_same_model_total"`
	FailoverCrossModelTotal    int64 `json:"failover_cross_model_total"`
	FailoverExhaustedTotal     int64 `json:"failover_exhausted_total"`
	CheckpointRestoresTotal    int64 `json:"checkpoint_restores_total"`
	CheckpointDegradedTotal    int64 `json:"checkpoint_degraded_total"`
	AdmissionRejectedTotal     int64 `json:"admission_rejected_total"`
	Backend429Total            int64 `json:"backend_429_total"`
	BackendPushesTotal         int64 `json:"backend_pushes_total"`
}

func fetchMetrics(gatewayURL string) (metricsSnap, error) {
	resp, err := http.Get(gatewayURL + "/api/debug/metrics")
	if err != nil {
		return metricsSnap{}, err
	}
	defer resp.Body.Close()
	var m metricsSnap
	if err := json.NewDecoder(resp.Body).Decode(&m); err != nil {
		return metricsSnap{}, err
	}
	return m, nil
}

// --- worker health: detect which worker a session pinned to ---

type workerHealth struct {
	ActiveSessions int `json:"active_sessions"`
}

func activeSessions(healthURL string) (int, error) {
	resp, err := http.Get(healthURL + "/health")
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	var h workerHealth
	if err := json.NewDecoder(resp.Body).Decode(&h); err != nil {
		return 0, err
	}
	return h.ActiveSessions, nil
}

// findPinnedWorker polls candidates' active_sessions and returns whichever
// one shows >0 — correct as long as this tool is the only session
// touching the fleet at the time, which is true for a scripted,
// sequential chaos run.
// sessionCounts snapshots active_sessions across candidates. Workers that
// cannot be reached are omitted rather than recorded as 0, so a briefly
// unreachable worker cannot later look like it GAINED the session.
func sessionCounts(candidates ...workerRef) map[string]int {
	out := make(map[string]int, len(candidates))
	for _, w := range candidates {
		if n, err := activeSessions(w.healthURL); err == nil {
			out[w.id] = n
		}
	}
	return out
}

// findPinnedWorker identifies which candidate a just-opened session landed
// on, by finding the one whose active_sessions count went UP relative to
// `before`.
//
// The obvious implementation — "return the first candidate reporting
// active_sessions > 0" — is wrong, and wrong in a way that produces
// confident false positives rather than errors. A worker holds a session
// until the gateway calls Close on it, so any session that outlived its
// gateway (a crashed or force-recreated gateway never runs its teardown)
// is counted forever. A later scenario then "finds" that stale worker,
// injects its fault there, and asserts against a worker the session under
// test was never on. That is exactly how scenario 7 first failed: it
// blackholed worker-a, which was holding a leaked session from an earlier
// run, while the session it had just opened was quite happily streaming
// through worker-b.
//
// A delta is immune to that, needs no server-side session registry, and
// costs one extra round of /health.
func findPinnedWorker(before map[string]int, candidates ...workerRef) (workerRef, error) {
	for _, w := range candidates {
		prior, known := before[w.id]
		if !known {
			continue // unreachable when the baseline was taken; cannot reason about its delta
		}
		n, err := activeSessions(w.healthURL)
		if err != nil {
			continue
		}
		if n > prior {
			return w, nil
		}
	}
	return workerRef{}, fmt.Errorf("none of %d candidates gained a session", len(candidates))
}

func killWorker(w workerRef) error {
	resp, err := http.Post(w.adminURL+"/admin/die", "application/json", nil)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("admin/die on %s: unexpected status %d", w.id, resp.StatusCode)
	}
	return nil
}

func corruptCheckpoint(gatewayURL, sessionID string) (bool, error) {
	resp, err := http.Post(fmt.Sprintf("%s/api/debug/corrupt-checkpoint/%s", gatewayURL, sessionID), "application/json", nil)
	if err != nil {
		return false, err
	}
	defer resp.Body.Close()
	var body struct {
		Corrupted bool `json:"corrupted"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		return false, err
	}
	return body.Corrupted, nil
}

// --- WS client: dedicated reader goroutine on the connection's one
// long-lived context — see docs/STATUS.md's standing note on
// coder/websocket: canceling a Read's context (a short per-poll timeout)
// closes the whole connection, not just that call. cmd/smoketest found
// this the hard way; this tool inherits the fix, not the bug. ---

type wsSession struct {
	conn      *websocket.Conn
	events    chan map[string]any
	readErrs  chan error
	sessionID string
}

// dialSession opens the socket and starts the reader, but does NOT send
// session.start. Split out from openSession so a caller that expects to be
// REFUSED can inspect the gateway's first response itself.
//
// Scenario 11 is why. openSession treats anything that is not an `ack` as
// an error, which is right for the eight scenarios that need a working
// session — but it means a refusal is indistinguishable from a transport
// failure, and a scenario asserting "clean error, no hang" would pass on
// a dial that failed for entirely unrelated reasons.
func dialSession(ctx context.Context, wsURL string) (*wsSession, error) {
	conn, _, err := websocket.Dial(ctx, wsURL, nil)
	if err != nil {
		return nil, fmt.Errorf("dial: %w", err)
	}
	s := &wsSession{conn: conn, events: make(chan map[string]any, 256), readErrs: make(chan error, 1)}
	go s.readLoop(ctx)
	return s, nil
}

// startSession sends session.start and returns the gateway's FIRST event,
// whatever it is — ack, overloaded or error.
func (s *wsSession) startSession(ctx context.Context) (map[string]any, error) {
	sessionStart := []byte(`{"type":"session.start","mode":"online","sample_rate_hz":16000,"encoding":"pcm_s16le","channels":1,"nominal_frame_ms":20}`)
	if err := s.writeFrame(ctx, wire.Frame{Type: wire.MsgControl, Seq: 0, Payload: sessionStart}); err != nil {
		return nil, fmt.Errorf("send session.start: %w", err)
	}
	ev, err := s.next(ctx, 10*time.Second)
	if err != nil {
		return nil, fmt.Errorf("read first event after session.start: %w", err)
	}
	return ev, nil
}

func (s *wsSession) readLoop(ctx context.Context) {
	defer close(s.events)
	for {
		typ, data, err := s.conn.Read(ctx)
		if err != nil {
			s.readErrs <- err
			return
		}
		if typ != websocket.MessageText {
			continue
		}
		var ev map[string]any
		if err := json.Unmarshal(data, &ev); err != nil {
			s.readErrs <- fmt.Errorf("unmarshal event: %w", err)
			return
		}
		s.events <- ev
	}
}

// openSessionMode opens a session in an explicit mode. `openSession` is
// the online shorthand; offline sessions are the only class routed
// through Bifrost, so scenario 16 needs to ask for one directly.
func openSessionMode(ctx context.Context, wsURL, mode string) (*wsSession, error) {
	conn, _, err := websocket.Dial(ctx, wsURL, nil)
	if err != nil {
		return nil, fmt.Errorf("dial: %w", err)
	}
	s := &wsSession{conn: conn, events: make(chan map[string]any, 256), readErrs: make(chan error, 1)}
	go s.readLoop(ctx)

	start := fmt.Sprintf(`{"type":"session.start","mode":%q,"sample_rate_hz":16000,"encoding":"pcm_s16le","channels":1,"nominal_frame_ms":20}`, mode)
	if err := s.writeFrame(ctx, wire.Frame{Type: wire.MsgControl, Seq: 0, Payload: []byte(start)}); err != nil {
		return nil, fmt.Errorf("send session.start: %w", err)
	}
	ack, err := s.nextOfType(ctx, 15*time.Second, "ack", "overloaded", "error")
	if err != nil {
		return nil, err
	}
	if ack["type"] != "ack" {
		return nil, fmt.Errorf("got %v after session.start, want ack", ack)
	}
	s.sessionID, _ = ack["session_id"].(string)
	if s.sessionID == "" {
		return nil, fmt.Errorf("ack carried no session_id: %v", ack)
	}
	return s, nil
}

// restoreWorker respawns a killed worker child and waits for it to SERVE.
// /admin/restore returns when the process is spawned, not when it is
// listening — a real adapter then loads weights — so polling /health is
// the difference between a reliable scenario and an intermittent one.
func restoreWorker(w workerRef) error {
	resp, err := http.Post(w.adminURL+"/admin/restore", "application/json", nil)
	if err != nil {
		return err
	}
	resp.Body.Close()
	deadline := time.Now().Add(60 * time.Second)
	for time.Now().Before(deadline) {
		if _, err := activeSessions(w.healthURL); err == nil {
			return nil
		}
		time.Sleep(500 * time.Millisecond)
	}
	return fmt.Errorf("%s did not start serving within 60s", w.id)
}

func openSession(ctx context.Context, wsURL string) (*wsSession, error) {
	conn, _, err := websocket.Dial(ctx, wsURL, nil)
	if err != nil {
		return nil, fmt.Errorf("dial: %w", err)
	}
	s := &wsSession{conn: conn, events: make(chan map[string]any, 256), readErrs: make(chan error, 1)}
	go s.readLoop(ctx)

	ack, err := s.startSession(ctx)
	if err != nil {
		return nil, err
	}
	if ack["type"] != "ack" {
		return nil, fmt.Errorf("got %v after session.start, want ack", ack)
	}
	s.sessionID, _ = ack["session_id"].(string)
	if s.sessionID == "" {
		return nil, fmt.Errorf("ack carried no session_id: %v", ack)
	}
	return s, nil
}

func (s *wsSession) close() {
	_ = s.conn.Close(websocket.StatusNormalClosure, "")
}

// openSessionPinnedToOneOf retries session open/close until Pick lands on
// one of candidates, or gives up after maxAttempts. A brand-new session
// has no compatibility-key preference (there is no prior key to prefer),
// so Pick's score ties across ALL healthy workers and the actual pin is
// effectively a coin flip among them — there is no server-side way to
// request "pin to one of this specific subset" for an initial open, and
// pre-killing the OTHER candidates to force it is fragile (it can starve
// openWithRetry's own bounded budget if too many candidates are dead at
// once). Retrying the open/close cycle client-side is simpler and doesn't
// touch server state at all.
//
// active_sessions is checkable immediately after Open succeeds (server.py
// increments it there, not on the first push), so no audio needs to be
// streamed before checking — each attempt is just an open, a check, and
// on a miss, a close.
func openSessionPinnedToOneOf(ctx context.Context, wsURL string, maxAttempts int, candidates ...workerRef) (*wsSession, workerRef, error) {
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		// Baseline BEFORE opening, so the session is identified by the
		// count it adds rather than by any count already there — see
		// findPinnedWorker.
		before := sessionCounts(candidates...)
		s, err := openSession(ctx, wsURL)
		if err != nil {
			return nil, workerRef{}, fmt.Errorf("attempt %d: %w", attempt, err)
		}
		pinned, err := findPinnedWorker(before, candidates...)
		if err == nil {
			return s, pinned, nil
		}
		s.close()
		time.Sleep(100 * time.Millisecond) // let server-side cleanup (Close, UnbindSession) land before the next attempt
	}
	return nil, workerRef{}, fmt.Errorf("did not pin to any of %d candidates after %d attempts", len(candidates), maxAttempts)
}

func (s *wsSession) writeFrame(ctx context.Context, f wire.Frame) error {
	if err := s.conn.Write(ctx, websocket.MessageBinary, wire.Encode(f)); err != nil {
		// A write failure almost always means the connection already
		// closed for a REASON the reader goroutine saw first (the
		// gateway sent a terminal `error` event, or failover exhausted
		// and it just hung up) — "use of closed network connection" on
		// its own says nothing about why. Surface whatever's already
		// sitting in the event/error channels instead of the raw
		// transport error, without blocking to wait for one.
		if diag := s.diagnoseClosure(); diag != "" {
			return fmt.Errorf("write failed (%w); %s", err, diag)
		}
		return err
	}
	return nil
}

// diagnoseClosure drains every currently-buffered event non-blockingly
// (a single read would only surface the OLDEST queued event — some
// harmless partial/ack this caller just hadn't gotten around to reading
// yet — not the revealing one) and reports the most useful thing found:
// an `error` event if any was queued, else the last event seen, else
// whatever the reader goroutine's own Read error was. Returns "" if
// nothing is available yet (a genuine race the caller's timeout, not
// this, should handle).
func (s *wsSession) diagnoseClosure() string {
	var last map[string]any
	var sawError map[string]any
drain:
	for {
		select {
		case ev, ok := <-s.events:
			if !ok {
				break drain
			}
			last = ev
			if t, _ := ev["type"].(string); t == "error" {
				sawError = ev
			}
		default:
			break drain
		}
	}
	if sawError != nil {
		return fmt.Sprintf("gateway sent error: %v", sawError)
	}
	if last != nil {
		return fmt.Sprintf("last event before closure: %v", last)
	}
	select {
	case err := <-s.readErrs:
		return fmt.Sprintf("reader goroutine's error: %v", err)
	default:
	}
	return ""
}

func (s *wsSession) next(ctx context.Context, timeout time.Duration) (map[string]any, error) {
	tctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	select {
	case ev, ok := <-s.events:
		if !ok {
			select {
			case err := <-s.readErrs:
				return nil, err
			default:
				return nil, fmt.Errorf("event stream closed with no error")
			}
		}
		return ev, nil
	case <-tctx.Done():
		return nil, fmt.Errorf("timed out waiting for an event")
	}
}

// nextOfType drains events until one of the given types arrives, erroring
// immediately on an `error` event (unless error itself is being sought).
func (s *wsSession) nextOfType(ctx context.Context, timeout time.Duration, types ...string) (map[string]any, error) {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		ev, err := s.next(ctx, timeout)
		if err != nil {
			return nil, err
		}
		t, _ := ev["type"].(string)
		for _, want := range types {
			if t == want {
				return ev, nil
			}
		}
		if t == "error" {
			return nil, fmt.Errorf("gateway reported error while waiting for %v: %v", types, ev)
		}
	}
	return nil, fmt.Errorf("timed out waiting for one of %v", types)
}

// streamClip sends every frame of pcm at 20ms nominal framing, starting
// at seq. Returns the next seq to use. Takes a raw byte slice, not a
// corpus.Clip, so callers can split one clip's PCM into two halves and
// inject a fault between streamClip calls.
func streamClip(ctx context.Context, s *wsSession, pcm []byte, seq uint64) (uint64, error) {
	const bytesPerSample = 2
	const nominalFrameMs = 20
	bytesPerFrame := 16000 * nominalFrameMs / 1000 * bytesPerSample
	ticker := time.NewTicker(nominalFrameMs * time.Millisecond)
	defer ticker.Stop()

	for off := 0; off < len(pcm); off += bytesPerFrame {
		end := off + bytesPerFrame
		if end > len(pcm) {
			end = len(pcm)
		}
		payload := pcm[off:end]
		numSamples := uint32(len(payload) / bytesPerSample)
		<-ticker.C
		f := wire.Frame{Type: wire.MsgAudio, Seq: seq, NumSamples: numSamples, Payload: payload}
		if err := s.writeFrame(ctx, f); err != nil {
			return seq, fmt.Errorf("send audio frame seq=%d: %w", seq, err)
		}
		seq++
	}
	return seq, nil
}

func (s *wsSession) end(ctx context.Context, seq uint64) (map[string]any, error) {
	if err := s.writeFrame(ctx, wire.Frame{Type: wire.MsgControl, Seq: seq, Payload: []byte(`{"type":"session.end"}`)}); err != nil {
		return nil, fmt.Errorf("send session.end: %w", err)
	}
	return s.nextOfType(ctx, 15*time.Second, "final")
}

// evenMidpoint rounds len(pcm)/2 down to an even byte offset. s16le is 2
// bytes/sample, so splitting a clip at an arbitrary byte offset can leave
// one half with an odd length — its last frame's payload byte count then
// silently disagrees (by one, via integer division) with the num_samples
// the frame declares, which wire.ValidateAudioFrame correctly rejects as
// a protocol violation. Found by that exact rejection firing mid-scenario
// (a real invariant-1 check doing its job), not by inspection of this
// function.
func evenMidpoint(pcm []byte) int {
	return (len(pcm) / 2) &^ 1
}

func loadClip(path string) (corpus.Clip, error) {
	clip, err := corpus.LoadWAV(path)
	if err != nil {
		return corpus.Clip{}, err
	}
	if clip.SampleRateHz != 16000 || clip.Channels != 1 || clip.BitsPerSample != 16 {
		return corpus.Clip{}, fmt.Errorf("%s is %dHz/%dch/%dbit, want 16000/1/16", path, clip.SampleRateHz, clip.Channels, clip.BitsPerSample)
	}
	return clip, nil
}
