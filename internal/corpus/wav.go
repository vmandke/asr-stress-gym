// Package corpus reads the committed corpus/*.wav clips (see
// corpus/README.md) into raw PCM. Used by cmd/smoketest now and by
// cmd/loadgen from M7 — written once, here, so the two don't grow two
// independent WAV readers.
package corpus

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"
)

// Clip is one loaded WAV file's format plus its raw PCM payload.
type Clip struct {
	SampleRateHz int
	Channels     int
	BitsPerSample int
	PCM          []byte // raw samples, exactly as stored in the file's data chunk
}

// LoadWAV reads a canonical PCM WAV file (RIFF/WAVE, walking chunks
// rather than assuming a fixed 44-byte header — macOS `say -o
// --file-format=WAVE` produces this shape, but nothing here assumes a
// specific chunk order beyond fmt appearing before data).
func LoadWAV(path string) (Clip, error) {
	f, err := os.Open(path)
	if err != nil {
		return Clip{}, err
	}
	defer f.Close()

	var riffHeader [12]byte
	if _, err := io.ReadFull(f, riffHeader[:]); err != nil {
		return Clip{}, fmt.Errorf("corpus: %s: read RIFF header: %w", path, err)
	}
	if string(riffHeader[0:4]) != "RIFF" || string(riffHeader[8:12]) != "WAVE" {
		return Clip{}, fmt.Errorf("corpus: %s: not a RIFF/WAVE file", path)
	}

	var clip Clip
	var haveFmt, haveData bool

	for {
		var chunkHeader [8]byte
		_, err := io.ReadFull(f, chunkHeader[:])
		if err == io.EOF {
			break
		}
		if err != nil {
			return Clip{}, fmt.Errorf("corpus: %s: read chunk header: %w", path, err)
		}
		chunkID := string(chunkHeader[0:4])
		chunkSize := binary.LittleEndian.Uint32(chunkHeader[4:8])

		switch chunkID {
		case "fmt ":
			body := make([]byte, chunkSize)
			if _, err := io.ReadFull(f, body); err != nil {
				return Clip{}, fmt.Errorf("corpus: %s: read fmt chunk: %w", path, err)
			}
			if len(body) < 16 {
				return Clip{}, fmt.Errorf("corpus: %s: fmt chunk too short", path)
			}
			audioFormat := binary.LittleEndian.Uint16(body[0:2])
			if audioFormat != 1 { // 1 == PCM
				return Clip{}, fmt.Errorf("corpus: %s: audio format %d is not PCM", path, audioFormat)
			}
			clip.Channels = int(binary.LittleEndian.Uint16(body[2:4]))
			clip.SampleRateHz = int(binary.LittleEndian.Uint32(body[4:8]))
			clip.BitsPerSample = int(binary.LittleEndian.Uint16(body[14:16]))
			haveFmt = true

		case "data":
			clip.PCM = make([]byte, chunkSize)
			if _, err := io.ReadFull(f, clip.PCM); err != nil {
				return Clip{}, fmt.Errorf("corpus: %s: read data chunk: %w", path, err)
			}
			haveData = true

		default:
			if _, err := f.Seek(int64(chunkSize), io.SeekCurrent); err != nil {
				return Clip{}, fmt.Errorf("corpus: %s: skip chunk %q: %w", path, chunkID, err)
			}
		}

		if chunkSize%2 == 1 { // RIFF chunks are word-aligned; skip the pad byte
			if _, err := f.Seek(1, io.SeekCurrent); err != nil {
				return Clip{}, err
			}
		}
	}

	if !haveFmt || !haveData {
		return Clip{}, fmt.Errorf("corpus: %s: missing fmt or data chunk", path)
	}
	return clip, nil
}
