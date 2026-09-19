package wire

import (
	"bytes"
	"errors"
	"testing"
)

func TestEncodeDecodeRoundTrip(t *testing.T) {
	orig := Frame{
		Type:       MsgAudio,
		Seq:        424242,
		CaptureMs:  1234,
		NumSamples: 320, // 20ms @ 16kHz
		Payload:    bytes.Repeat([]byte{0xAB, 0xCD}, 320),
	}
	got, err := Decode(Encode(orig))
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if got.Type != orig.Type || got.Seq != orig.Seq || got.CaptureMs != orig.CaptureMs || got.NumSamples != orig.NumSamples {
		t.Fatalf("round trip mismatch: got %+v, want %+v", got, orig)
	}
	if !bytes.Equal(got.Payload, orig.Payload) {
		t.Fatalf("payload mismatch")
	}
}

func TestDecodeTooShort(t *testing.T) {
	_, err := Decode([]byte{1, 2, 3})
	if !errors.Is(err, ErrFrameTooShort) {
		t.Fatalf("got %v, want ErrFrameTooShort", err)
	}
}

func TestDecodeUnknownType(t *testing.T) {
	buf := Encode(Frame{Type: MsgAudio, Seq: 1})
	buf[0] = 99
	_, err := Decode(buf)
	if !errors.Is(err, ErrUnknownType) {
		t.Fatalf("got %v, want ErrUnknownType", err)
	}
}

// This is invariant 1, at the codec layer: num_samples is authoritative,
// and a client is free to batch several nominal 20ms frames into one
// message. Duration must never be inferred from message size or count —
// only NumSamples decides, and it can be any value.
func TestDurationNeverInferredFromMessageSize(t *testing.T) {
	cases := []struct {
		name       string
		numSamples uint32
	}{
		{"single nominal 20ms frame", 320},
		{"batched 3x20ms frame", 960},
		{"tiny 1-sample frame", 1},
		{"unusually large batched frame", 32000}, // 2 seconds
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := Frame{
				Type:       MsgAudio,
				Seq:        1,
				NumSamples: tc.numSamples,
				Payload:    make([]byte, int(tc.numSamples)*BytesPerSample),
			}
			if err := ValidateAudioFrame(f); err != nil {
				t.Fatalf("expected valid, got %v", err)
			}
			gotMs := f.DurationMs(16000)
			wantMs := float64(tc.numSamples) * 1000 / 16000
			if gotMs != wantMs {
				t.Fatalf("DurationMs = %v, want %v", gotMs, wantMs)
			}
		})
	}
}

func TestValidateAudioFrameRejectsMismatch(t *testing.T) {
	f := Frame{
		Type:       MsgAudio,
		NumSamples: 320,
		Payload:    make([]byte, 100), // wrong: should be 320*2=640
	}
	err := ValidateAudioFrame(f)
	if !errors.Is(err, ErrDurationMismatch) {
		t.Fatalf("got %v, want ErrDurationMismatch", err)
	}
}

func TestValidateAudioFrameAcceptsExactMatch(t *testing.T) {
	f := Frame{NumSamples: 480, Payload: make([]byte, 960)}
	if err := ValidateAudioFrame(f); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestDecodeControlSessionStart(t *testing.T) {
	payload := []byte(`{"type":"session.start","mode":"online","sample_rate_hz":16000,"encoding":"pcm_s16le","channels":1,"nominal_frame_ms":20}`)
	msg, err := DecodeControl(payload)
	if err != nil {
		t.Fatalf("DecodeControl: %v", err)
	}
	ss, ok := msg.(*SessionStart)
	if !ok {
		t.Fatalf("got %T, want *SessionStart", msg)
	}
	if ss.Mode != "online" || ss.SampleRateHz != 16000 || ss.Encoding != "pcm_s16le" || ss.Channels != 1 {
		t.Fatalf("unexpected SessionStart: %+v", ss)
	}
}

func TestDecodeControlSessionEnd(t *testing.T) {
	msg, err := DecodeControl([]byte(`{"type":"session.end"}`))
	if err != nil {
		t.Fatalf("DecodeControl: %v", err)
	}
	if _, ok := msg.(*SessionEnd); !ok {
		t.Fatalf("got %T, want *SessionEnd", msg)
	}
}

func TestDecodeControlUnknownType(t *testing.T) {
	_, err := DecodeControl([]byte(`{"type":"something.else"}`))
	if err == nil {
		t.Fatal("expected error for unknown control type")
	}
}

func TestDecodeControlMalformedJSON(t *testing.T) {
	_, err := DecodeControl([]byte(`not json`))
	if err == nil {
		t.Fatal("expected error for malformed JSON")
	}
}

func TestValidateSessionStartAcceptsLockedFormat(t *testing.T) {
	ss := SessionStart{Type: "session.start", Mode: "online", SampleRateHz: 16000, Encoding: "pcm_s16le", Channels: 1, NominalFrameMs: 20}
	if err := ValidateSessionStart(ss); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestValidateSessionStartRejectsWrongFormat(t *testing.T) {
	cases := []SessionStart{
		{Mode: "online", SampleRateHz: 8000, Encoding: "pcm_s16le", Channels: 1},  // wrong rate
		{Mode: "online", SampleRateHz: 16000, Encoding: "pcm_f32le", Channels: 1}, // wrong encoding
		{Mode: "online", SampleRateHz: 16000, Encoding: "pcm_s16le", Channels: 2}, // wrong channel count
	}
	for _, ss := range cases {
		if err := ValidateSessionStart(ss); !errors.Is(err, ErrUnsupportedFormat) {
			t.Fatalf("case %+v: got %v, want ErrUnsupportedFormat", ss, err)
		}
	}
}

func TestValidateSessionStartRejectsUnknownMode(t *testing.T) {
	ss := SessionStart{Mode: "turbo", SampleRateHz: 16000, Encoding: "pcm_s16le", Channels: 1}
	if err := ValidateSessionStart(ss); err == nil {
		t.Fatal("expected error for unknown mode")
	}
}
