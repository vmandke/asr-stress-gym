package audio

import (
	"bytes"
	"encoding/binary"
)

// WAVFromRecords wraps journaled records as a canonical RIFF/WAVE file.
//
// This lives HERE, not in cmd/gateway or internal/bifrost, for the reason
// the whole package exists: building a WAV header means knowing the sample
// rate, the sample width and the byte order, and no code above this line is
// allowed to know any of them. The caller passes opaque Records in and gets
// opaque bytes out; it never learns that 16-bit little-endian was involved.
//
// Needed because the OpenAI-shaped transcription endpoint that Bifrost
// routes to takes a CONTAINER, not bare samples — the worker can accept
// either (worker/server.py's _to_pcm), but a gateway that shipped raw PCM
// to an endpoint documented as taking audio files would be relying on a
// courtesy rather than a contract.
//
// Records are concatenated in the order given. Ordering is the caller's
// business; the journal already returns them in sequence order.
func WAVFromRecords(records []Record, sampleRateHz uint32) []byte {
	var pcm []byte
	for _, r := range records {
		pcm = append(pcm, r.Payload...)
	}
	return wrapWAV(pcm, sampleRateHz)
}

const (
	wavHeaderSize = 44
	wavPCMFormat  = 1 // uncompressed PCM
	wavChannels   = 1 // mono — the locked format (docs/DECISIONS.md)
	wavBitDepth   = 16
)

func wrapWAV(pcm []byte, sampleRateHz uint32) []byte {
	byteRate := sampleRateHz * wavChannels * wavBitDepth / 8
	blockAlign := uint16(wavChannels * wavBitDepth / 8)

	buf := bytes.NewBuffer(make([]byte, 0, wavHeaderSize+len(pcm)))
	w := func(v any) { _ = binary.Write(buf, binary.LittleEndian, v) }

	buf.WriteString("RIFF")
	// Everything after this field: the 4-byte "WAVE" tag, the 24-byte fmt
	// chunk (8 header + 16 body), the 8-byte data header, then the samples.
	w(uint32(4 + 24 + 8 + len(pcm)))
	buf.WriteString("WAVE")

	buf.WriteString("fmt ")
	w(uint32(16)) // PCM fmt chunk body size
	w(uint16(wavPCMFormat))
	w(uint16(wavChannels))
	w(sampleRateHz)
	w(byteRate)
	w(blockAlign)
	w(uint16(wavBitDepth))

	buf.WriteString("data")
	w(uint32(len(pcm)))
	buf.Write(pcm)
	return buf.Bytes()
}
