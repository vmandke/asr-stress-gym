// Command loadgen is the paced synthetic client from docs/build-plan.md,
// "The load generator". Its command-line surface and CSV output are a
// frozen contract — see docs/BENCH.md, which is the file to change first
// if either needs to move, because the benchmarks (M8) are built against
// it.
//
// It imports internal/wire and internal/corpus directly rather than
// re-implementing the codec or the WAV reader, so the load generator and
// the gateway can never drift onto two different notions of a frame.
//
// Go, not Python, for one reason: this drives hundreds of streams each
// paced on its own ticker. A generator that becomes the bottleneck is
// measuring itself.
//
// **loadgen never asserts.** A run that gets `overloaded` back, or sees
// streams error out, still exits 0 — those outcomes are the data. Pass
// and fail live in cmd/chaostest.
package main

import (
	"context"
	"encoding/csv"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"os"
	"os/signal"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"asr-stress-gym/internal/corpus"
)

// nominalFrameMs is the frame cadence every stream paces itself on: 20ms
// at 16kHz is 320 samples, the size docs/PROTOCOL.md uses throughout.
// Pacing is by wall clock, never by "send as fast as possible" — an
// unpaced generator measures the gateway's queue depth rather than its
// latency.
const (
	nominalFrameMs   = 20
	sampleRateHz     = 16000
	bytesPerSample   = 2
	frameSamples     = sampleRateHz * nominalFrameMs / 1000 // 320
	frameBytes       = frameSamples * bytesPerSample        // 640
	eventChanBuffer  = 4096
	finalWaitTimeout = 20 * time.Second
)

type config struct {
	streams        int
	ramp           time.Duration
	duration       time.Duration
	speechRatio    float64
	jitterMs       int
	dropPct        float64
	reconnectEvery time.Duration
	mode           string
	corpusDir      string
	wsURL          string
	out            string
	seed           int64
	controlAddr    string // M9: serve the control API instead of running one batch
}

// event is one row of the CSV in docs/BENCH.md. Streams produce these on
// a channel; exactly one goroutine writes them, so ordering within the
// file is by arrival and no stream ever blocks on disk.
type event struct {
	tsMs          int64
	streamID      int
	kind          string
	seq           int64
	latencyMs     float64
	hasLatency    bool
	streamsActive int64
	utteranceID   string
	revision      int64
	failoverEpoch int64
}

// counters is the whole run's tally, kept as atomics because every stream
// updates it concurrently. The summary in docs/BENCH.md is rendered from
// exactly these.
type counters struct {
	streamsOpened    atomic.Int64
	streamsRefused   atomic.Int64
	streamsActive    atomic.Int64
	framesSent       atomic.Int64
	framesDropped    atomic.Int64
	audioMsSent      atomic.Int64
	finals           atomic.Int64
	duplicateFinals  atomic.Int64
	discontinuities  atomic.Int64
	errors           atomic.Int64
	overloaded       atomic.Int64
	partialLatencies latencySet
	finalLatencies   latencySet
}

// latencySet collects samples for a percentile. Unbounded on purpose: a
// benchmark run is bounded by --duration, and reservoir-sampling here
// would put an approximation into the one number the run exists to
// report.
type latencySet struct {
	mu      sync.Mutex
	samples []float64
}

func (l *latencySet) add(ms float64) {
	l.mu.Lock()
	l.samples = append(l.samples, ms)
	l.mu.Unlock()
}

func (l *latencySet) percentile(p float64) float64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	if len(l.samples) == 0 {
		return 0
	}
	s := append([]float64(nil), l.samples...)
	sort.Float64s(s)
	idx := int(float64(len(s)) * p)
	if idx >= len(s) {
		idx = len(s) - 1
	}
	return s[idx]
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func main() {
	var cfg config
	flag.IntVar(&cfg.streams, "streams", 1, "concurrent sessions to hold open")
	flag.DurationVar(&cfg.ramp, "ramp", 0, "spread stream starts evenly over this window")
	flag.DurationVar(&cfg.duration, "duration", 30*time.Second, "how long to keep generating after the ramp completes")
	flag.Float64Var(&cfg.speechRatio, "speech-ratio", 1.0, "fraction of each stream's audio that is speech; the rest is silence")
	flag.IntVar(&cfg.jitterMs, "jitter-ms", 0, "uniform random 0..N ms delay before each frame write")
	flag.Float64Var(&cfg.dropPct, "drop-pct", 0, "fraction of audio frames not sent, while still consuming their seq")
	flag.DurationVar(&cfg.reconnectEvery, "reconnect-every", 0, "each stream reconnects on this period, resuming from its last ack (0 = never)")
	flag.StringVar(&cfg.mode, "mode", "online", "session mode: online | offline")
	flag.StringVar(&cfg.corpusDir, "corpus", "", "directory of WAV clips (default: "+corpus.Dir+")")
	flag.StringVar(&cfg.wsURL, "ws-url", envOr("GATEWAY_WS_URL", "ws://localhost:7070/ws"), "gateway WebSocket URL")
	flag.StringVar(&cfg.out, "out", "", "write the event CSV here (omit for summary only)")
	flag.Int64Var(&cfg.seed, "seed", 1, "seeds clip choice, jitter and drops")
	// M9. Additive and off by default: every flag above keeps exactly the
	// meaning docs/BENCH.md froze, and a run without this flag behaves
	// identically to before. With it, loadgen starts IDLE and serves a
	// small control API instead of running one batch and exiting — which
	// is what lets the dashboard's "Start load" button exist without the
	// gateway growing its own duplicate of the streaming client.
	flag.StringVar(&cfg.controlAddr, "control-addr", envOr("LOADGEN_CONTROL_ADDR", ""),
		"serve the load-control API on this address and start idle (e.g. :8090)")
	flag.Parse()

	if cfg.controlAddr != "" {
		if err := serveControl(cfg); err != nil {
			log.Fatalf("loadgen: %v", err)
		}
		return
	}

	if err := run(cfg); err != nil {
		// Reaching here means loadgen could not run at all — bad flags, an
		// unreadable corpus, a gateway that was never up. An `overloaded`
		// response or a stream that errored is NOT this; see the package
		// comment.
		log.Fatalf("loadgen: %v", err)
	}
}

// run is the CLI entry point: one batch, ended by --duration or Ctrl-C.
func run(cfg config) error {
	// Ctrl-C ends the run cleanly — streams close their sessions and the
	// CSV is flushed — rather than leaving a truncated file and a fleet
	// full of half-open sessions.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	return runCtx(ctx, cfg)
}

// runCtx is the same batch under a caller-supplied context, so the M9
// control API can stop a run on demand. Cancelling it is the same code
// path as Ctrl-C: streams close their sessions and the summary still
// prints, rather than the process being torn down mid-stream and leaving
// the fleet holding handles nobody will ever Close.
func runCtx(ctx context.Context, cfg config) error {
	if cfg.streams < 1 {
		return fmt.Errorf("--streams must be at least 1, got %d", cfg.streams)
	}
	if cfg.mode != "online" && cfg.mode != "offline" {
		return fmt.Errorf("--mode must be online or offline, got %q", cfg.mode)
	}
	if cfg.speechRatio <= 0 || cfg.speechRatio > 1 {
		return fmt.Errorf("--speech-ratio must be in (0, 1], got %v", cfg.speechRatio)
	}

	if cfg.corpusDir == "" {
		dir, err := corpus.ResolveDir()
		if err != nil {
			return err
		}
		cfg.corpusDir = dir
	}
	clips, err := loadClips(cfg.corpusDir, cfg.streams, cfg.seed)
	if err != nil {
		return err
	}

	var cnt counters
	events := make(chan event, eventChanBuffer)
	writerDone := make(chan struct{})
	go func() {
		defer close(writerDone)
		if err := writeCSV(cfg.out, events); err != nil {
			log.Printf("loadgen: CSV writer: %v", err)
		}
	}()

	start := time.Now()
	var wg sync.WaitGroup
	for i := 0; i < cfg.streams; i++ {
		// Ramp by start offset rather than by sleeping in the loop: every
		// stream is launched now and decides for itself when to dial, so a
		// long ramp cannot be skewed by the launcher falling behind.
		var delay time.Duration
		if cfg.ramp > 0 && cfg.streams > 1 {
			delay = time.Duration(int64(cfg.ramp) * int64(i) / int64(cfg.streams))
		}
		wg.Add(1)
		go func(id int, delay time.Duration) {
			defer wg.Done()
			s := &stream{
				id:     id,
				cfg:    cfg,
				clip:   clips[id%len(clips)],
				rng:    rand.New(rand.NewSource(cfg.seed + int64(id))),
				cnt:    &cnt,
				events: events,
				start:  start,
			}
			s.runWithDelay(ctx, delay)
		}(i, delay)
	}

	wg.Wait()
	close(events)
	<-writerDone

	printSummary(cfg, &cnt, time.Since(start))
	return nil
}

// maxLoadedClips caps how much of the corpus is held in memory at once.
//
// The default corpus is ~500 clips averaging 15s, which is ~240MB of PCM.
// Loading all of it to run four streams would make the generator's own
// resident size larger than the gateway it is measuring, and on a ramp to
// 200 streams that memory is competing with the very containers under
// test. Streams only ever read one clip each, so loading more distinct
// clips than there are streams buys nothing.
const maxLoadedClips = 64

// loadClips reads a bounded, seeded sample of the corpus once, up front.
// Streams share the decoded PCM read-only; re-reading per stream would
// make the generator's own disk I/O part of what it measures.
//
// The sample is seeded and taken from a sorted path list, so a given
// --seed and --streams always select the same clips — a run is
// reproducible without pinning the whole corpus.
func loadClips(dir string, want int, seed int64) ([]corpus.Clip, error) {
	// Recursive: the corpus is one subdirectory per kind (monologue/,
	// dialogue/, long_form/, ...). A flat glob finds nothing and reports
	// an empty corpus on a full one.
	paths, err := corpus.WalkClips(dir)
	if err != nil {
		return nil, err
	}
	if len(paths) == 0 {
		return nil, fmt.Errorf("no *.wav found under %s — run %s", dir, corpus.GenerateHint)
	}

	if want < 1 {
		want = 1
	}
	if want > maxLoadedClips {
		want = maxLoadedClips
	}
	if want < len(paths) {
		rng := rand.New(rand.NewSource(seed))
		rng.Shuffle(len(paths), func(i, j int) { paths[i], paths[j] = paths[j], paths[i] })
		paths = paths[:want]
		sort.Strings(paths)
	}

	clips := make([]corpus.Clip, 0, len(paths))
	for _, p := range paths {
		c, err := corpus.LoadWAV(p)
		if err != nil {
			return nil, fmt.Errorf("%s: %w", p, err)
		}
		if c.SampleRateHz != sampleRateHz || c.Channels != 1 || c.BitsPerSample != 16 {
			return nil, fmt.Errorf("%s: expected mono 16kHz s16le (docs/FAQ.md), got %dHz %dch %dbit",
				p, c.SampleRateHz, c.Channels, c.BitsPerSample)
		}
		clips = append(clips, c)
	}
	return clips, nil
}

func writeCSV(path string, events <-chan event) error {
	if path == "" {
		for range events { // still drain, or every stream blocks on a full channel
		}
		return nil
	}
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()

	w := csv.NewWriter(f)
	defer w.Flush()
	header := []string{"ts_ms", "stream_id", "event", "seq", "latency_ms", "streams_active", "utterance_id", "revision", "failover_epoch"}
	if err := w.Write(header); err != nil {
		return err
	}
	for e := range events {
		latency := ""
		if e.hasLatency {
			latency = strconv.FormatFloat(e.latencyMs, 'f', 3, 64)
		}
		row := []string{
			strconv.FormatInt(e.tsMs, 10),
			strconv.Itoa(e.streamID),
			e.kind,
			strconv.FormatInt(e.seq, 10),
			latency,
			strconv.FormatInt(e.streamsActive, 10),
			e.utteranceID,
			strconv.FormatInt(e.revision, 10),
			strconv.FormatInt(e.failoverEpoch, 10),
		}
		if err := w.Write(row); err != nil {
			return err
		}
	}
	return nil
}

func printSummary(cfg config, c *counters, elapsed time.Duration) {
	audioSec := float64(c.audioMsSent.Load()) / 1000
	fmt.Printf("streams_requested=%d streams_opened=%d streams_refused=%d\n",
		cfg.streams, c.streamsOpened.Load(), c.streamsRefused.Load())
	fmt.Printf("elapsed_s=%.1f audio_seconds_sent=%.1f frames_sent=%d frames_dropped=%d\n",
		elapsed.Seconds(), audioSec, c.framesSent.Load(), c.framesDropped.Load())
	fmt.Printf("partial_p50_ms=%.1f partial_p95_ms=%.1f\n",
		c.partialLatencies.percentile(0.50), c.partialLatencies.percentile(0.95))
	fmt.Printf("final_p50_ms=%.1f final_p95_ms=%.1f\n",
		c.finalLatencies.percentile(0.50), c.finalLatencies.percentile(0.95))
	fmt.Printf("finals=%d duplicate_finals=%d discontinuities=%d errors=%d overloaded=%d\n",
		c.finals.Load(), c.duplicateFinals.Load(), c.discontinuities.Load(),
		c.errors.Load(), c.overloaded.Load())
}
