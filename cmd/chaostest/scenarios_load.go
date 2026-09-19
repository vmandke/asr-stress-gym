package main

import (
	"context"
	"fmt"
	"time"

	"asr-stress-gym/internal/wire"
)

// nominalFrameSamples is 20ms at 16kHz — the same framing streamClip
// uses, named here because scenario 10 builds frames directly rather than
// from a clip.
const nominalFrameSamples = 320

// M7's chaos scenarios: the ones about load, rate limits and degraded
// backends rather than about dead ones. Scenario 9 (overload) is
// deliberately NOT here — it needs the gateway restarted with a lowered
// capacity envelope, which is a shell concern, so it lives in
// scripts/scenarios/09_overload.sh.

// scenario4RateLimitStorm: build-plan.md demo 4 — "worker A returns 429 at
// 50% -> breaker opens, fallback, no retry storm, zero client errors."
//
// The assertion that carries this one is **zero client errors**. A 429 is
// the backend saying "not me, not now"; the client should never learn
// that happened. build-plan.md is explicit that the online path must not
// sleep on it — a 500ms backoff against a 200ms budget is catastrophic —
// so the gateway must move rather than wait, and the transcript must come
// out whole either way.
func scenario4RateLimitStorm(wsURL, gatewayURL, clipPath string, fleet map[string]workerRef) error {
	ctx := context.Background()
	clip, err := loadClip(clipPath)
	if err != nil {
		return err
	}
	defer resetAllFaults(fleet)

	s, pinned, err := openSessionPinnedToOneOf(ctx, wsURL, 10, fleet["worker-a"], fleet["worker-b"])
	if err != nil {
		return fmt.Errorf("could not pin a session to worker-a or worker-b: %w", err)
	}
	defer s.close()

	mid := evenMidpoint(clip.PCM)
	seq, err := streamClip(ctx, s, clip.PCM[:mid], 1)
	if err != nil {
		return fmt.Errorf("first half: %w", err)
	}

	before, err := fetchMetrics(gatewayURL)
	if err != nil {
		return fmt.Errorf("fetch baseline metrics: %w", err)
	}

	// 50%, per the demo. Not 100%: a worker that refuses everything is
	// just a dead worker wearing a different status code, and scenario 2
	// already covers dead workers. Half is what makes this a *storm*
	// rather than an outage — the gateway keeps being offered a backend
	// that keeps intermittently refusing.
	if err := inject429(pinned, 0.5); err != nil {
		return fmt.Errorf("inject 429 on %s: %w", pinned.id, err)
	}

	seq, err = streamClip(ctx, s, clip.PCM[mid:], seq)
	if err != nil {
		return fmt.Errorf("second half (under 429 storm): %w", err)
	}

	after, err := fetchMetrics(gatewayURL)
	if err != nil {
		return fmt.Errorf("fetch post-storm metrics: %w", err)
	}
	if after.Backend429Total <= before.Backend429Total {
		return fmt.Errorf("backend_429_total did not move (%d -> %d) — the fault never reached the gateway, so this scenario proved nothing",
			before.Backend429Total, after.Backend429Total)
	}
	if after.DuplicateFinalsTotal != before.DuplicateFinalsTotal {
		return fmt.Errorf("duplicate_finals_total changed (%d -> %d) — must stay zero", before.DuplicateFinalsTotal, after.DuplicateFinalsTotal)
	}

	// No retry storm: the gateway must move off the refusing worker, not
	// hammer it. Each rate-limited push costs at most one 429 before the
	// bucket is zeroed and the router stops choosing it, so the count
	// stays on the order of the failovers taken — not the order of frames
	// sent.
	if got := after.Backend429Total - before.Backend429Total; got > 50 {
		return fmt.Errorf("backend_429_total rose by %d — that is a retry storm, not a fallback", got)
	}

	// Zero client errors is the headline. `error` is terminal, so if one
	// had been emitted the stream above would already have failed; assert
	// the transcript is whole to prove the session genuinely survived
	// rather than merely not crashing.
	final, err := s.end(ctx, seq)
	if err != nil {
		return fmt.Errorf("session.end after a 429 storm: %w", err)
	}
	if text, _ := final["text"].(string); text == "" {
		return fmt.Errorf("final carried empty text — the client was affected by a fault it should never have seen")
	}
	return nil
}

// scenario6GrayFailure: build-plan.md demo 6 — "worker A +2s latency ->
// ejected within 15s on latency, not errors."
//
// The hard part is the second clause. A worker that is slow but correct
// returns no errors at all, so an error-rate-driven health check never
// touches it — it just quietly ruins every session routed to it. The
// router's latency arm compares against the CLUSTER p95 rather than an
// absolute threshold, precisely so "slow" means "slow compared to its
// peers right now" and does not go stale when the model or hardware
// changes.
func scenario6GrayFailure(wsURL, gatewayURL, clipPath string, fleet map[string]workerRef) error {
	ctx := context.Background()
	clip, err := loadClip(clipPath)
	if err != nil {
		return err
	}
	defer resetAllFaults(fleet)

	// Give the cluster a p95 to compare against first. With no healthy
	// traffic anywhere, every worker looks equally slow and there is no
	// "gray" to detect — the comparison is relative by design.
	warm, _, err := openSessionPinnedToOneOf(ctx, wsURL, 10, fleet["worker-a"], fleet["worker-b"], fleet["worker-c"])
	if err != nil {
		return fmt.Errorf("warm-up session: %w", err)
	}
	warmSeq, err := streamClip(ctx, warm, clip.PCM, 1)
	if err != nil {
		return fmt.Errorf("warm-up stream: %w", err)
	}
	_, _ = warm.end(ctx, warmSeq)
	warm.close()

	s, pinned, err := openSessionPinnedToOneOf(ctx, wsURL, 10, fleet["worker-a"], fleet["worker-b"])
	if err != nil {
		return fmt.Errorf("could not pin a session to worker-a or worker-b: %w", err)
	}
	defer s.close()

	if err := injectSlow(pinned, 2*time.Second); err != nil {
		return fmt.Errorf("inject +2s latency on %s: %w", pinned.id, err)
	}

	// Drive traffic through the now-slow worker so the router accumulates
	// latency samples. Health here is REACTIVE (internal/router): there is
	// no background prober, so a worker nobody calls is never re-evaluated.
	if _, err := streamClip(ctx, s, clip.PCM, 1); err != nil {
		// A slow worker may time the session out entirely; that is a
		// different failure mode from the one under test, but it still
		// means the fault was injected, so keep going to the assertion.
		_ = err
	}

	status, ok := awaitWorkerStatus(gatewayURL, pinned.id, 15*time.Second, "ejected", "degraded")
	if !ok {
		return fmt.Errorf("%s was still %q after 15s of +2s latency — a gray failure must be caught on latency, not only on errors",
			pinned.id, status)
	}
	return nil
}

// scenario7Blackhole: build-plan.md demo 7 — "accepts, never responds ->
// timeout fires, failover occurs."
//
// Distinct from a crash in the one way that matters: the TCP connection
// succeeds, so nothing at the transport layer reports a problem. Only the
// client-side timeout (internal/backend's http.Client) turns this into a
// detectable failure, and a system without one hangs here forever.
func scenario7Blackhole(wsURL, gatewayURL, clipPath string, fleet map[string]workerRef) error {
	ctx := context.Background()
	clip, err := loadClip(clipPath)
	if err != nil {
		return err
	}
	defer resetAllFaults(fleet)

	s, pinned, err := openSessionPinnedToOneOf(ctx, wsURL, 10, fleet["worker-a"], fleet["worker-b"])
	if err != nil {
		return fmt.Errorf("could not pin a session to worker-a or worker-b: %w", err)
	}
	defer s.close()

	mid := evenMidpoint(clip.PCM)
	seq, err := streamClip(ctx, s, clip.PCM[:mid], 1)
	if err != nil {
		return fmt.Errorf("first half: %w", err)
	}

	before, err := fetchMetrics(gatewayURL)
	if err != nil {
		return fmt.Errorf("fetch baseline metrics: %w", err)
	}
	if err := injectBlackhole(pinned, true); err != nil {
		return fmt.Errorf("blackhole %s: %w", pinned.id, err)
	}

	if _, err := streamClip(ctx, s, clip.PCM[mid:], seq); err != nil {
		return fmt.Errorf("second half (into the blackhole): %w", err)
	}

	// Generous: the failover cannot begin until the backend client's own
	// timeout fires, and only then does recovery run.
	if _, err := s.nextOfType(ctx, 30*time.Second, "partial.reset"); err != nil {
		return fmt.Errorf("no partial.reset after blackholing %s — the timeout never fired, or failover never ran: %w", pinned.id, err)
	}

	after, err := fetchMetrics(gatewayURL)
	if err != nil {
		return fmt.Errorf("fetch post-failover metrics: %w", err)
	}
	if after.FailoverTotal <= before.FailoverTotal {
		return fmt.Errorf("failover_total did not increase (%d -> %d)", before.FailoverTotal, after.FailoverTotal)
	}
	if after.DuplicateFinalsTotal != before.DuplicateFinalsTotal {
		return fmt.Errorf("duplicate_finals_total changed (%d -> %d) — must stay zero", before.DuplicateFinalsTotal, after.DuplicateFinalsTotal)
	}
	return nil
}

// scenario10LongSilence: build-plan.md demo 10 — "60s silence -> backend
// calls ≈ 0."
//
// The economic argument, asserted rather than described. Sending silence
// is NOT the same as sending nothing: every frame is framed, sequenced,
// acked and journaled exactly as speech would be, so the audio still
// arrives and the session stays live. What must not happen is any of it
// reaching a model.
//
// Frames are sent unpaced. The VAD accumulates by DECLARED duration, never
// by arrival time (invariant 1), so 60s of audio is 60s of audio however
// fast it is delivered — and a test that actually waited 60s would be one
// nobody runs.
func scenario10LongSilence(wsURL, gatewayURL string, fleet map[string]workerRef) error {
	ctx := context.Background()
	defer resetAllFaults(fleet)

	s, err := openSession(ctx, wsURL)
	if err != nil {
		return err
	}
	defer s.close()

	before, err := fetchMetrics(gatewayURL)
	if err != nil {
		return fmt.Errorf("fetch baseline metrics: %w", err)
	}

	const (
		silenceSeconds = 60
		frameMs        = 20
		frames         = silenceSeconds * 1000 / frameMs
	)
	silence := make([]byte, nominalFrameSamples*wire.BytesPerSample)
	seq := uint64(1)
	for i := 0; i < frames; i++ {
		f := wire.Frame{
			Type:       wire.MsgAudio,
			Seq:        seq,
			NumSamples: nominalFrameSamples,
			Payload:    silence,
		}
		if err := s.writeFrame(ctx, f); err != nil {
			return fmt.Errorf("silence frame seq=%d: %w", seq, err)
		}
		seq++
	}

	// The tail has to settle before the count means anything: a chunk cut
	// right at the end may still be in flight.
	time.Sleep(2 * time.Second)

	after, err := fetchMetrics(gatewayURL)
	if err != nil {
		return fmt.Errorf("fetch post-silence metrics: %w", err)
	}
	pushes := after.BackendPushesTotal - before.BackendPushesTotal

	// "≈ 0", not "== 0", and the slack is principled rather than
	// convenient: a VAD tuned to never dispatch silence would also clip
	// the first syllable of every utterance, which is a far worse trade
	// (docs/build-plan.md). Ungated, 60s at a 160ms chunk policy would be
	// ~375 pushes, so anything in single digits is the argument holding.
	if pushes > 10 {
		return fmt.Errorf("60s of silence produced %d backend pushes, want ~0 (ungated would be ~375) — silence is not being gated", pushes)
	}
	return nil
}

// scenario11AllBackendsDown: build-plan.md demo 11 — "kill everything ->
// clean error, no hang, no panic."
//
// build-plan.md notes this "matters more than it looks — plenty of systems
// handle one failure and deadlock on total failure", and the failure mode
// it guards against is specific: with no candidate to fail over to, a
// bounded retry loop that was written as recursion, or a wait that was
// written without a deadline, hangs instead of returning. So the real
// assertion is that an answer arrives AT ALL, promptly, and that the
// gateway is still alive afterwards to say so.
func scenario11AllBackendsDown(wsURL, gatewayURL string, fleet map[string]workerRef) error {
	ctx := context.Background()

	for _, w := range fleet {
		if err := killWorker(w); err != nil {
			return fmt.Errorf("kill %s: %w", w.id, err)
		}
	}

	// A session opened against an empty fleet must be refused, not parked.
	// Either answer is correct and they mean different things: `overloaded`
	// is the router finding no capable worker (retryable), `error` is the
	// session failing outright (terminal). What is NOT acceptable is
	// silence.
	//
	// dialSession + startSession rather than openSession, so the refusal
	// is INSPECTED rather than inferred: openSession treats anything that
	// is not an ack as an error, which would let this scenario "pass" on a
	// dial that failed for reasons having nothing to do with the fleet.
	s, err := dialSession(ctx, wsURL)
	if err != nil {
		return fmt.Errorf("could not even open a socket to the gateway after killing the fleet — it did not survive: %w", err)
	}
	defer s.close()

	start := time.Now()
	ev, err := s.startSession(ctx)
	if err != nil {
		return fmt.Errorf("no response to session.start within the timeout — the gateway hung on a dead fleet: %w", err)
	}
	elapsed := time.Since(start)

	t, _ := ev["type"].(string)
	if t != "overloaded" && t != "error" {
		return fmt.Errorf("got %q (%v), want overloaded or error — a dead fleet must refuse, not admit", t, ev)
	}
	// "No hang" deserves a real bound rather than just "it eventually
	// answered". Refusal is a routing decision with nothing to wait for,
	// so it should be immediate; anything near the backend timeout means
	// the gateway was blocking on a dead worker instead of consulting the
	// router.
	if elapsed > 10*time.Second {
		return fmt.Errorf("refusal took %s — that is a timeout unwinding, not an admission decision", elapsed)
	}

	// No panic: the gateway process must still be serving. A crashed
	// gateway would fail this rather than merely returning odd data, which
	// is exactly the distinction this scenario exists to draw.
	if _, err := fetchMetrics(gatewayURL); err != nil {
		return fmt.Errorf("gateway stopped answering after every worker died — it did not survive total failure: %w", err)
	}
	return nil
}
