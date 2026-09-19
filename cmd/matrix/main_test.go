package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"asr-stress-gym/internal/backend"
)

func TestRunBuildsPolicyFromLiveAdvertisements(t *testing.T) {
	server := func(advert backend.WorkerAdvert) *httptest.Server {
		t.Helper()
		return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path != "/health" {
				t.Fatalf("path = %q, want /health", r.URL.Path)
			}
			if err := json.NewEncoder(w).Encode(advert); err != nil {
				t.Fatal(err)
			}
		}))
	}

	caps := func(streaming, serializable bool) backend.Capabilities {
		modes := []string{"offline"}
		if streaming {
			modes = append(modes, "online")
		}
		return backend.Capabilities{Streaming: streaming, Serializable: serializable, Modes: modes}
	}
	mock := server(backend.WorkerAdvert{WorkerID: "worker-mock", Model: "mock-v1", CompatibilityKeyHash: "K0", Capabilities: caps(true, true)})
	defer mock.Close()
	a := server(backend.WorkerAdvert{WorkerID: "worker-a", Model: "zipformer", CompatibilityKeyHash: "K1", Capabilities: caps(true, false)})
	defer a.Close()
	b := server(backend.WorkerAdvert{WorkerID: "worker-b", Model: "zipformer", CompatibilityKeyHash: "K1", Capabilities: caps(true, false)})
	defer b.Close()
	d := server(backend.WorkerAdvert{WorkerID: "worker-d", Model: "whisper", CompatibilityKeyHash: "K3", Capabilities: caps(false, false)})
	defer d.Close()

	out := filepath.Join(t.TempDir(), "compat-matrix.md")
	spec := "worker-mock=" + mock.URL + ",worker-a=" + a.URL + ",worker-b=" + b.URL + ",worker-d=" + d.URL
	if err := run(context.Background(), spec, out, time.Second); err != nil {
		t.Fatal(err)
	}
	gotBytes, err := os.ReadFile(out)
	if err != nil {
		t.Fatal(err)
	}
	got := string(gotBytes)
	for _, want := range []string{
		"| worker-a | worker-b | `K1` | `K1` | fresh state + replay from last committed final | fresh state + replay from last committed final |",
		"| worker-a | worker-d | `K1` | `K3` | refused by router: target cannot serve online traffic | fresh state + replay from last committed final |",
		"| worker-d | worker-a | `K3` | `K1` | not applicable: source cannot serve online traffic | fresh state + replay from last committed final |",
		"| worker-mock | worker-a | `K0` | `K1` | fresh state + replay from last committed final | fresh state + replay from last committed final |",
	} {
		if !strings.Contains(got, want) {
			t.Errorf("matrix missing row:\n%s\n--- matrix ---\n%s", want, got)
		}
	}
}

func TestRecoveryPolicyAllowsCheckpointOnlyForSerializableSameKey(t *testing.T) {
	base := worker{advert: backend.WorkerAdvert{CompatibilityKeyHash: "same", Capabilities: backend.Capabilities{Streaming: true, Serializable: true, Modes: []string{"online"}}}}
	if got, want := recoveryPolicy(base, base, "online"), "checkpoint restore + tail replay"; got != want {
		t.Fatalf("recoveryPolicy = %q, want %q", got, want)
	}
	nonSerializable := base
	nonSerializable.advert.Capabilities.Serializable = false
	if got, want := recoveryPolicy(base, nonSerializable, "online"), "fresh state + replay from last committed final"; got != want {
		t.Fatalf("recoveryPolicy = %q, want %q", got, want)
	}
	offlineSource := worker{advert: backend.WorkerAdvert{CompatibilityKeyHash: "K3", Capabilities: backend.Capabilities{Modes: []string{"offline"}}}}
	offlineTarget := worker{advert: backend.WorkerAdvert{CompatibilityKeyHash: "K4", Capabilities: backend.Capabilities{Modes: []string{"offline"}}}}
	if got, want := recoveryPolicy(offlineSource, offlineTarget, "offline"), "fresh state + replay from last committed final"; got != want {
		t.Fatalf("offline recoveryPolicy = %q, want %q", got, want)
	}
}

func TestParseWorkersRejectsMalformedAndDuplicateEntries(t *testing.T) {
	for _, spec := range []string{"", "worker-a", "worker-a=http://a,worker-a=http://b"} {
		if _, err := parseWorkers(spec); err == nil {
			t.Errorf("parseWorkers(%q) succeeded, want error", spec)
		}
	}
}
