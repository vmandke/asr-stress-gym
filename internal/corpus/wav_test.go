package corpus

import (
	"os"
	"path/filepath"
	"testing"
)

// findCorpusDir locates the repo-root corpus/ directory from wherever `go
// test` happens to run from. Walks up to the go.mod marking repo root,
// not just any directory literally named "corpus" — this package is
// ALSO named corpus (internal/corpus), so a naive walk matches itself one
// level up before ever reaching the real data directory.
func findCorpusDir(t *testing.T) string {
	t.Helper()
	dir, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 8; i++ {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			candidate := filepath.Join(dir, "corpus")
			if info, err := os.Stat(candidate); err == nil && info.IsDir() {
				return candidate
			}
			break
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			break
		}
		dir = parent
	}
	t.Skip("corpus/ directory not found; run scripts/gen_corpus.sh first")
	return ""
}

func TestLoadWAVMatchesLockedFormat(t *testing.T) {
	corpusDir := findCorpusDir(t)
	entries, err := os.ReadDir(corpusDir)
	if err != nil {
		t.Fatal(err)
	}
	var found bool
	for _, e := range entries {
		if filepath.Ext(e.Name()) != ".wav" {
			continue
		}
		found = true
		clip, err := LoadWAV(filepath.Join(corpusDir, e.Name()))
		if err != nil {
			t.Fatalf("%s: %v", e.Name(), err)
		}
		// docs/DECISIONS.md: mono 16kHz s16le — the corpus is generated
		// to match exactly so it needs no runtime conversion.
		if clip.SampleRateHz != 16000 {
			t.Errorf("%s: sample rate = %d, want 16000", e.Name(), clip.SampleRateHz)
		}
		if clip.Channels != 1 {
			t.Errorf("%s: channels = %d, want 1", e.Name(), clip.Channels)
		}
		if clip.BitsPerSample != 16 {
			t.Errorf("%s: bits per sample = %d, want 16", e.Name(), clip.BitsPerSample)
		}
		if len(clip.PCM) == 0 {
			t.Errorf("%s: empty PCM payload", e.Name())
		}
		if len(clip.PCM)%2 != 0 {
			t.Errorf("%s: PCM length %d is not a whole number of s16le samples", e.Name(), len(clip.PCM))
		}
	}
	if !found {
		t.Skip("no .wav files in corpus/; run scripts/gen_corpus.sh first")
	}
}
