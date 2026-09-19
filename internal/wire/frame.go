// Package wire implements the client<->gateway binary frame codec defined
// in docs/PROTOCOL.md.
//
// cmd/loadgen imports this package directly so the load generator and the
// gateway can never drift onto two different codecs.
package wire

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
)

// MsgType is the frame's byte-0 discriminant.
type MsgType uint8

const (
	MsgAudio   MsgType = 1
	MsgControl MsgType = 2
)

// HeaderSize is the fixed prefix before payload: type(1) + seq(8) +
// capture_ms(4) + num_samples(4).
const HeaderSize = 1 + 8 + 4 + 4

// BytesPerSample is fixed by the locked audio format decision (mono
// s16le) — see docs/DECISIONS.md and docs/FAQ.md for why. Not a general
// PCM parameter: internal/wire only ever speaks this one format, on
// purpose, so there is no format-negotiation logic anywhere on this path.
const BytesPerSample = 2

var (
	ErrFrameTooShort    = errors.New("wire: frame shorter than header")
	ErrDurationMismatch = errors.New("wire: declared num_samples disagrees with payload length")
	ErrUnknownType      = errors.New("wire: unknown frame type")
)

// Frame is one decoded client->gateway message. NumSamples is always
// authoritative for duration — see invariant 1 in build-plan.md: it is
// never inferred from arrival timing or message size.
type Frame struct {
	Type       MsgType
	Seq        uint64
	CaptureMs  uint32
	NumSamples uint32
	Payload    []byte // PCM s16le for MsgAudio; UTF-8 JSON for MsgControl
}

// DurationMs derives duration from the frame's own declared sample count
// and the session's negotiated sample rate — the only place this
// computation happens.
func (f Frame) DurationMs(sampleRateHz uint32) float64 {
	return float64(f.NumSamples) * 1000 / float64(sampleRateHz)
}

// Encode serializes a Frame to the wire format in docs/PROTOCOL.md.
func Encode(f Frame) []byte {
	buf := make([]byte, HeaderSize+len(f.Payload))
	buf[0] = byte(f.Type)
	binary.BigEndian.PutUint64(buf[1:9], f.Seq)
	binary.BigEndian.PutUint32(buf[9:13], f.CaptureMs)
	binary.BigEndian.PutUint32(buf[13:17], f.NumSamples)
	copy(buf[HeaderSize:], f.Payload)
	return buf
}

// Decode parses a wire message into a Frame. It does NOT validate
// audio-frame duration against payload length — call ValidateAudioFrame
// for that, since the check needs the session's sample rate, which is
// negotiated in session.start and not present on every frame.
func Decode(buf []byte) (Frame, error) {
	if len(buf) < HeaderSize {
		return Frame{}, ErrFrameTooShort
	}
	t := MsgType(buf[0])
	if t != MsgAudio && t != MsgControl {
		return Frame{}, fmt.Errorf("%w: %d", ErrUnknownType, buf[0])
	}
	f := Frame{
		Type:       t,
		Seq:        binary.BigEndian.Uint64(buf[1:9]),
		CaptureMs:  binary.BigEndian.Uint32(buf[9:13]),
		NumSamples: binary.BigEndian.Uint32(buf[13:17]),
	}
	if len(buf) > HeaderSize {
		f.Payload = buf[HeaderSize:]
	}
	return f, nil
}

// ValidateAudioFrame checks the invariant that makes chunking correct:
// the payload's actual byte length must match what NumSamples declares,
// for the session's negotiated (mono, s16le) format. A mismatch is a
// protocol violation, not a transient loss — see docs/PROTOCOL.md: unlike
// a sequence gap, this is not something the session continues past.
func ValidateAudioFrame(f Frame) error {
	want := int(f.NumSamples) * BytesPerSample
	if len(f.Payload) != want {
		return fmt.Errorf("%w: num_samples=%d implies %d bytes, got %d",
			ErrDurationMismatch, f.NumSamples, want, len(f.Payload))
	}
	return nil
}

// --- CONTROL message JSON bodies (docs/PROTOCOL.md) ---

type SessionStart struct {
	Type           string `json:"type"` // "session.start"
	Mode           string `json:"mode"` // "online" | "offline"
	SampleRateHz   int    `json:"sample_rate_hz"`
	Encoding       string `json:"encoding"` // "pcm_s16le"
	Channels       int    `json:"channels"`
	NominalFrameMs int    `json:"nominal_frame_ms"`
}

type SessionEnd struct {
	Type string `json:"type"` // "session.end"
}

// The locked audio format decision (docs/DECISIONS.md, docs/FAQ.md).
// session.start must declare exactly this; the server rejects anything
// else rather than transcoding or guessing.
const (
	RequiredSampleRateHz = 16000
	RequiredEncoding     = "pcm_s16le"
	RequiredChannels     = 1
)

var ErrUnsupportedFormat = errors.New("wire: session.start declares an unsupported audio format")

// ValidateSessionStart enforces the locked format and a known mode. A
// mismatch here means the session is never created — see docs/PROTOCOL.md.
func ValidateSessionStart(ss SessionStart) error {
	if ss.Mode != "online" && ss.Mode != "offline" {
		return fmt.Errorf("wire: unknown mode %q", ss.Mode)
	}
	if ss.SampleRateHz != RequiredSampleRateHz || ss.Encoding != RequiredEncoding || ss.Channels != RequiredChannels {
		return fmt.Errorf("%w: got sample_rate_hz=%d encoding=%q channels=%d, want %d/%q/%d",
			ErrUnsupportedFormat, ss.SampleRateHz, ss.Encoding, ss.Channels,
			RequiredSampleRateHz, RequiredEncoding, RequiredChannels)
	}
	return nil
}

// controlEnvelope is used only to sniff the "type" field before deciding
// which concrete struct to unmarshal into.
type controlEnvelope struct {
	Type string `json:"type"`
}

// DecodeControl inspects a CONTROL frame's JSON payload and returns the
// concrete message type: *SessionStart or *SessionEnd. Unknown "type"
// values are returned as an error rather than silently ignored.
func DecodeControl(payload []byte) (any, error) {
	var env controlEnvelope
	if err := json.Unmarshal(payload, &env); err != nil {
		return nil, fmt.Errorf("wire: control payload is not valid JSON: %w", err)
	}
	switch env.Type {
	case "session.start":
		var m SessionStart
		if err := json.Unmarshal(payload, &m); err != nil {
			return nil, fmt.Errorf("wire: malformed session.start: %w", err)
		}
		return &m, nil
	case "session.end":
		var m SessionEnd
		if err := json.Unmarshal(payload, &m); err != nil {
			return nil, fmt.Errorf("wire: malformed session.end: %w", err)
		}
		return &m, nil
	default:
		return nil, fmt.Errorf("wire: unknown control type %q", env.Type)
	}
}
