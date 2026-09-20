// Command fleetmanager is the deliberately small, demo-only lifecycle
// control plane. It is not part of request serving: its only authority is to
// create one of two allowlisted streaming worker families on the local Docker engine,
// wait for the worker's own health advertisement, and register that endpoint
// with Bifrost. The gateway separately verifies the advertisement before it
// puts the worker in its routing cohort.
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"strings"
	"sync/atomic"
	"time"
)

type familySpec struct {
	Model   string
	Adapter string
	TierURL string
	Timeout int
}

var families = map[string]familySpec{
	"zip": {Model: "zipformer-en-20M", Adapter: "zipformer_kv", TierURL: "http://kvtier-zip:9500", Timeout: 5},
	"ctc": {Model: "fastconformer-ctc", Adapter: "conformer_ctc_kv", TierURL: "http://kvtier-ctc:9500", Timeout: 5},
}

type createRequest struct {
	Family string `json:"family"`
}

type workerResponse struct {
	ID                   string `json:"id"`
	URL                  string `json:"url"`
	Family               string `json:"family"`
	CompatibilityKeyHash string `json:"compatibility_key_hash"`
}

type manager struct {
	docker     *http.Client
	bifrostURL string
	network    string
	image      string
	kvDType    string
	hc         *http.Client
	sequence   atomic.Uint64
}

func main() {
	addr := flag.String("addr", ":8091", "HTTP listen address")
	flag.Parse()
	m := &manager{
		docker:     dockerClient(envOr("DOCKER_SOCKET", "/var/run/docker.sock")),
		bifrostURL: strings.TrimRight(envOr("BIFROST_URL", "http://bifrost:8080"), "/"),
		network:    envOr("FLEET_NETWORK", "asr-stress-gym_default"),
		image:      envOr("FLEET_WORKER_IMAGE", "asr-stress-gym-worker:demo"),
		kvDType:    envOr("KV_DTYPE", "fp32"),
		hc:         &http.Client{Timeout: 2 * time.Second},
	}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", m.health)
	mux.HandleFunc("POST /v1/workers", m.createWorker)
	log.Printf("fleet-manager: listening on %s; network=%s image=%s", *addr, m.network, m.image)
	log.Fatal(http.ListenAndServe(*addr, mux))
}

func envOr(key, fallback string) string {
	if value := strings.TrimSpace(os.Getenv(key)); value != "" {
		return value
	}
	return fallback
}

// dockerClient uses Docker's Unix-socket HTTP API directly. Keeping the
// narrow request vocabulary here avoids giving the dashboard or gateway a
// Docker client, shell, compose binary, or arbitrary-image escape hatch.
func dockerClient(socket string) *http.Client {
	transport := &http.Transport{
		DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
			return (&net.Dialer{}).DialContext(ctx, "unix", socket)
		},
	}
	return &http.Client{Transport: transport, Timeout: 10 * time.Second}
}

func (m *manager) health(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{"status": "READY", "families": []string{"zip", "ctc"}})
}

func (m *manager) createWorker(w http.ResponseWriter, r *http.Request) {
	var req createRequest
	if err := json.NewDecoder(io.LimitReader(r.Body, 1024)).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": "invalid JSON"})
		return
	}
	req.Family = strings.ToLower(strings.TrimSpace(req.Family))
	spec, ok := families[req.Family]
	if !ok {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": "family must be zip or ctc"})
		return
	}

	id := fmt.Sprintf("worker-%s-dyn-%d", req.Family, m.sequence.Add(1))
	if err := m.createAndStart(r.Context(), id, spec); err != nil {
		writeJSON(w, http.StatusBadGateway, map[string]any{"error": err.Error(), "worker": id})
		return
	}
	// Do not register a merely-started container. The worker's advertisement
	// is the source of truth for its compatibility key and catches a bad image
	// or environment before Bifrost can send it traffic.
	advert, err := m.waitForHealth(r.Context(), id)
	if err != nil {
		_ = m.remove(context.Background(), id)
		writeJSON(w, http.StatusBadGateway, map[string]any{"error": err.Error(), "worker": id})
		return
	}
	if err := m.registerBifrost(r.Context(), id, spec); err != nil {
		_ = m.remove(context.Background(), id)
		writeJSON(w, http.StatusBadGateway, map[string]any{"error": err.Error(), "worker": id})
		return
	}
	writeJSON(w, http.StatusCreated, workerResponse{
		ID: id, URL: "http://" + id + ":9000", Family: req.Family,
		CompatibilityKeyHash: advert.CompatibilityKeyHash,
	})
}

func (m *manager) createAndStart(ctx context.Context, id string, spec familySpec) error {
	body := map[string]any{
		"Image": m.image,
		"Env": []string{
			"WORKER_ID=" + id, "MODEL=" + spec.Model, "ADAPTER=" + spec.Adapter,
			"KVTIER_URL=" + spec.TierURL, "KV_DTYPE=" + m.kvDType, "KV_HOT_MAX=32",
		},
		"Labels": map[string]string{"asr-stress-gym.managed": "true", "asr-stress-gym.family": id},
		"HostConfig": map[string]any{
			"NanoCPUs": int64(1500000000), "Memory": int64(1536 * 1024 * 1024),
			"RestartPolicy": map[string]string{"Name": "no"},
		},
		"NetworkingConfig": map[string]any{"EndpointsConfig": map[string]any{
			m.network: map[string]any{"Aliases": []string{id}},
		}},
	}
	if err := m.dockerJSON(ctx, http.MethodPost, "/v1.43/containers/create?name="+id, body, nil, http.StatusCreated); err != nil {
		return fmt.Errorf("create worker: %w", err)
	}
	if err := m.dockerJSON(ctx, http.MethodPost, "/v1.43/containers/"+id+"/start", nil, nil, http.StatusNoContent); err != nil {
		_ = m.remove(context.Background(), id)
		return fmt.Errorf("start worker: %w", err)
	}
	return nil
}

type advert struct {
	CompatibilityKeyHash string `json:"compatibility_key_hash"`
}

func (m *manager) waitForHealth(ctx context.Context, id string) (advert, error) {
	deadline := time.Now().Add(75 * time.Second)
	var last error
	for time.Now().Before(deadline) {
		req, _ := http.NewRequestWithContext(ctx, http.MethodGet, "http://"+id+":9000/health", nil)
		resp, err := m.hc.Do(req)
		if err == nil && resp.StatusCode == http.StatusOK {
			var result advert
			err = json.NewDecoder(io.LimitReader(resp.Body, 64<<10)).Decode(&result)
			resp.Body.Close()
			if err == nil && result.CompatibilityKeyHash != "" {
				return result, nil
			}
		}
		if resp != nil {
			resp.Body.Close()
		}
		last = err
		select {
		case <-ctx.Done():
			return advert{}, ctx.Err()
		case <-time.After(time.Second):
		}
	}
	return advert{}, fmt.Errorf("worker did not become healthy within 75s: %v", last)
}

func (m *manager) registerBifrost(ctx context.Context, id string, spec familySpec) error {
	provider := map[string]any{
		"provider": id,
		"network_config": map[string]any{
			"base_url": "http://" + id + ":9000", "allow_private_network": true,
			"default_request_timeout_in_seconds": spec.Timeout, "max_retries": 1,
			"retry_backoff_initial_seconds": 0.1, "retry_backoff_max_seconds": 1,
		},
		"custom_provider_config": map[string]string{"base_provider_type": "openai"},
	}
	if err := m.bifrostPost(ctx, "/api/providers", provider); err != nil {
		return fmt.Errorf("register provider: %w", err)
	}
	// Bifrost v1.5 manages provider keys separately. Including a key in the
	// provider payload succeeds but silently discards it, producing a provider
	// that returns 400 for every inference request.
	key := map[string]any{"name": id + "-local", "value": "dummy-local-no-auth", "models": []string{"*"}, "weight": 1.0}
	if err := m.bifrostPost(ctx, "/api/providers/"+id+"/keys", key); err != nil {
		return fmt.Errorf("register provider key: %w", err)
	}
	return nil
}

func (m *manager) bifrostPost(ctx context.Context, path string, value any) error {
	body, err := json.Marshal(value)
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, m.bifrostURL+path, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := m.hc.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 8<<10))
		return fmt.Errorf("Bifrost returned %s: %s", resp.Status, strings.TrimSpace(string(body)))
	}
	return nil
}

func (m *manager) remove(ctx context.Context, id string) error {
	return m.dockerJSON(ctx, http.MethodDelete, "/v1.43/containers/"+id+"?force=true", nil, nil, http.StatusNoContent)
}

func (m *manager) dockerJSON(ctx context.Context, method, path string, body any, out any, want int) error {
	var reader io.Reader
	if body != nil {
		encoded, err := json.Marshal(body)
		if err != nil {
			return err
		}
		reader = bytes.NewReader(encoded)
	}
	req, err := http.NewRequestWithContext(ctx, method, "http://docker"+path, reader)
	if err != nil {
		return err
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := m.docker.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != want {
		data, _ := io.ReadAll(io.LimitReader(resp.Body, 8<<10))
		return fmt.Errorf("Docker API %s %s returned %s: %s", method, path, resp.Status, strings.TrimSpace(string(data)))
	}
	if out != nil {
		return json.NewDecoder(resp.Body).Decode(out)
	}
	return nil
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}
