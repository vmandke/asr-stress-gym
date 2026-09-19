package audio

import (
	"encoding/binary"
	"fmt"

	webrtcvad "github.com/rolandhe/go-vad"

	"asr-stress-gym/internal/wire"
)

// VAD is the small, swappable decision boundary around voice activity
// detection. Implementations receive one signed-PCM window and report whether
// it contains speech; the surrounding pipeline owns reframing, hysteresis and
// pre-roll. Keeping the interface here lets tests use a deterministic fake and
// lets a future model-backed detector replace WebRTC VAD without changing any
// caller outside internal/audio.
type VAD interface {
	Speech(samples []int16) (bool, error)
}

// VADConfig is intentionally expressed in duration, not frame counts, so the
// policy remains meaningful if the detector window changes. Defaults are
// reported in the README rather than claimed to be universally correct.
type VADConfig struct {
	WindowMs      int
	StartSpeechMs int
	EndSilenceMs  int
	PreRollMs     int
}

var DefaultVADConfig = VADConfig{
	WindowMs:      20,
	StartSpeechMs: 40,
	EndSilenceMs:  600,
	PreRollMs:     160,
}

func (c VADConfig) validate(sampleRateHz uint32) error {
	if sampleRateHz != 8000 && sampleRateHz != 16000 && sampleRateHz != 32000 && sampleRateHz != 48000 {
		return fmt.Errorf("audio: VAD sample rate %d unsupported", sampleRateHz)
	}
	if c.WindowMs != 10 && c.WindowMs != 20 && c.WindowMs != 30 {
		return fmt.Errorf("audio: VAD window must be 10, 20, or 30ms (got %d)", c.WindowMs)
	}
	if c.StartSpeechMs < c.WindowMs || c.EndSilenceMs < c.WindowMs || c.PreRollMs < 0 {
		return fmt.Errorf("audio: invalid VAD hysteresis/pre-roll config")
	}
	return nil
}

// NewVADPipeline is M4's production pipeline. The dependency is a pure-Go,
// bit-exact port of libfvad/WebRTC VAD, so the gateway image remains CGO-free.
func NewVADPipeline(sampleRateHz uint32, chunkMs float64, cfg VADConfig) (Pipeline, error) {
	if err := cfg.validate(sampleRateHz); err != nil {
		return nil, err
	}
	detector, err := newWebRTCVAD(sampleRateHz)
	if err != nil {
		return nil, err
	}
	return newVADPipeline(sampleRateHz, chunkMs, cfg, detector), nil
}

type webRTCVAD struct{ native *webrtcvad.VAD }

func newWebRTCVAD(sampleRateHz uint32) (*webRTCVAD, error) {
	v := webrtcvad.New()
	if err := v.SetSampleRate(webrtcvad.SampleRate(sampleRateHz)); err != nil {
		return nil, fmt.Errorf("audio: configure WebRTC VAD sample rate: %w", err)
	}
	if err := v.SetMode(webrtcvad.ModeAggressive); err != nil {
		return nil, fmt.Errorf("audio: configure WebRTC VAD mode: %w", err)
	}
	return &webRTCVAD{native: v}, nil
}

func (v *webRTCVAD) Speech(samples []int16) (bool, error) {
	result, err := v.native.Process(samples)
	if err != nil {
		return false, err
	}
	return result == webrtcvad.ResultVoice, nil
}

type vadPipeline struct {
	sampleRateHz uint32
	config       VADConfig
	detector     VAD

	// Reframing is sample-based because transport frames may be batched or
	// split arbitrarily; VAD itself only accepts its fixed 10/20/30ms windows.
	reframe []int16

	live       accumulator
	ready      []Chunk
	boundaries []Event

	active    bool
	voiceMs   int
	silenceMs int
	preRoll   []pendingRecord
	preRollMs float64
}

type pendingRecord struct {
	seq        uint64
	durationMs float64
	payload    []byte
}

func newVADPipeline(sampleRateHz uint32, chunkMs float64, cfg VADConfig, detector VAD) *vadPipeline {
	return &vadPipeline{
		sampleRateHz: sampleRateHz,
		config:       cfg,
		detector:     detector,
		live:         accumulator{chunkMs: chunkMs},
	}
}

func decodePCM16LE(payload []byte) ([]int16, error) {
	if len(payload)%wire.BytesPerSample != 0 {
		return nil, fmt.Errorf("audio: odd PCM payload length %d", len(payload))
	}
	samples := make([]int16, len(payload)/wire.BytesPerSample)
	for i := range samples {
		samples[i] = int16(binary.LittleEndian.Uint16(payload[i*2:]))
	}
	return samples, nil
}

func (p *vadPipeline) Ingest(f wire.Frame) (Ref, error) {
	samples, err := decodePCM16LE(f.Payload)
	if err != nil {
		return Ref{}, err
	}
	p.reframe = append(p.reframe, samples...)

	windowSamples := int(p.sampleRateHz) * p.config.WindowMs / 1000
	rawVoice := false
	for len(p.reframe) >= windowSamples {
		voice, err := p.detector.Speech(p.reframe[:windowSamples])
		if err != nil {
			return Ref{}, fmt.Errorf("audio: VAD decision: %w", err)
		}
		rawVoice = rawVoice || voice
		p.reframe = p.reframe[windowSamples:]
	}

	durationMs := f.DurationMs(p.sampleRateHz)
	p.applyHysteresis(f.Seq, durationMs, f.Payload, rawVoice)
	return Ref{Seq: f.Seq, DurationMs: durationMs, Voiced: rawVoice}, nil
}

func (p *vadPipeline) applyHysteresis(seq uint64, durationMs float64, payload []byte, rawVoice bool) {
	if !p.active {
		p.keepPreRoll(seq, durationMs, payload)
		if rawVoice {
			p.voiceMs += int(durationMs)
		} else {
			p.voiceMs = 0
		}
		if p.voiceMs < p.config.StartSpeechMs {
			return
		}

		p.active = true
		p.voiceMs = 0
		p.silenceMs = 0
		p.boundaries = append(p.boundaries, Event{Type: EventSpeechStart, Seq: seq})
		for _, r := range p.preRoll {
			p.ready = append(p.ready, p.live.feed(r.seq, r.durationMs, true, r.payload)...)
		}
		p.preRoll = nil
		p.preRollMs = 0
		return
	}

	if rawVoice {
		p.silenceMs = 0
		// A brief unvoiced gap within an active utterance is not pre-roll for
		// a new utterance. It is deliberately gated out of inference, and a
		// renewed voiced frame keeps this utterance alive.
		p.preRoll = nil
		p.preRollMs = 0
		p.ready = append(p.ready, p.live.feed(seq, durationMs, true, payload)...)
		return
	}

	// Keep a rolling copy while waiting for endpointing. If this gap grows
	// long enough to end the utterance, it is already the next utterance's
	// pre-roll; if speech resumes first the voice branch above discards it.
	p.keepPreRoll(seq, durationMs, payload)
	p.silenceMs += int(durationMs)
	if p.silenceMs < p.config.EndSilenceMs {
		return
	}
	p.active = false
	p.silenceMs = 0
	p.boundaries = append(p.boundaries, Event{Type: EventEndpoint, Seq: seq})
	p.keepPreRoll(seq, durationMs, payload)
}

func (p *vadPipeline) keepPreRoll(seq uint64, durationMs float64, payload []byte) {
	p.preRoll = append(p.preRoll, pendingRecord{seq: seq, durationMs: durationMs, payload: append([]byte(nil), payload...)})
	p.preRollMs += durationMs
	for p.preRollMs > float64(p.config.PreRollMs) && len(p.preRoll) > 0 {
		p.preRollMs -= p.preRoll[0].durationMs
		p.preRoll = p.preRoll[1:]
	}
}

func (p *vadPipeline) Ready() []Chunk {
	out := p.ready
	p.ready = nil
	return out
}

func (p *vadPipeline) Boundary() (Event, bool) {
	if len(p.boundaries) == 0 {
		return Event{}, false
	}
	ev := p.boundaries[0]
	p.boundaries = p.boundaries[1:]
	return ev, true
}

func (p *vadPipeline) RetentionStart() (uint64, bool) {
	if len(p.preRoll) == 0 {
		return 0, false
	}
	return p.preRoll[0].seq, true
}

func (p *vadPipeline) Flush() []Chunk {
	return p.live.forceCut()
}

// Recut preserves the recorded raw VAD decision and replays the same
// hysteresis/pre-roll state machine. VAD itself is deliberately not rerun:
// journal records are the source of truth for the original decision, while
// replaying the state machine is what retains the silent pre-roll that was
// intentionally sent with the live onset.
func (p *vadPipeline) Recut(records []Record) ([]Chunk, error) {
	replay := newVADPipeline(p.sampleRateHz, p.live.chunkMs, p.config, nil)
	var out []Chunk
	for _, r := range records {
		replay.applyHysteresis(r.Seq, r.DurationMs, r.Payload, r.Voiced)
		out = append(out, replay.Ready()...)
	}
	return out, nil
}
