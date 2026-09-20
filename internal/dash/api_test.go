package dash

import (
	"bufio"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"asr-stress-gym/internal/session"
)

// recorder stands in for a worker and its supervisor, recording which
// port+path each action reached.
type recorder struct {
	mu   sync.Mutex
	hits []string
	body []string
}

func (r *recorder) handler() http.HandlerFunc {
	return func(w http.ResponseWriter, req *http.Request) {
		buf := make([]byte, 256)
		n, _ := req.Body.Read(buf)
		r.mu.Lock()
		r.hits = append(r.hits, req.URL.Path)
		r.body = append(r.body, string(buf[:n]))
		r.mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"ok":true}`))
	}
}

func (r *recorder) seen() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.hits...)
}

func newTestControl(t *testing.T) (*Control, *httptest.Server, *recorder, *recorder) {
	t.Helper()
	worker, supervisor := &recorder{}, &recorder{}
	ws := httptest.NewServer(worker.handler())
	ss := httptest.NewServer(supervisor.handler())
	t.Cleanup(ws.Close)
	t.Cleanup(ss.Close)

	drained := map[string]bool{}
	c := &Control{
		Hub:     NewHub(),
		Nodes:   NewNodeSampler(nil),
		Fleet:   func() FleetView { return FleetView{Workers: []WorkerView{{ID: "worker-a", Status: "healthy"}}} },
		Targets: map[string]Target{"worker-a": {WorkerURL: ws.URL, AdminURL: ss.URL}},
		Drain:   func(id string, on bool) error { drained[id] = on; return nil },
	}
	mux := http.NewServeMux()
	c.Mount(mux)
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return c, srv, worker, supervisor
}

// The two-port split is the thing most likely to be got wrong, and getting
// it wrong is not loud: a kill sent to the worker's own port would 404,
// and the dashboard would report a failure for an action that simply went
// to the wrong place. Worse, the inverse — request faults sent to the
// supervisor — would leave a killed worker permanently unreachable,
// because the port that could tell it to stop misbehaving died with it.
func TestProcessFaultsGoToTheSupervisorAndRequestFaultsToTheWorker(t *testing.T) {
	_, srv, worker, supervisor := newTestControl(t)

	for _, action := range []string{"kill", "restore"} {
		resp, err := http.Post(srv.URL+"/api/chaos/worker-a/"+action, "", nil)
		if err != nil {
			t.Fatalf("%s: %v", action, err)
		}
		resp.Body.Close()
	}
	for _, action := range []string{"slow?ms=800", "blackhole?on=true", "429?rate=0.5", "corrupt?on=true", "reset"} {
		resp, err := http.Post(srv.URL+"/api/chaos/worker-a/"+action, "", nil)
		if err != nil {
			t.Fatalf("%s: %v", action, err)
		}
		resp.Body.Close()
	}

	wantSupervisor := []string{"/admin/die", "/admin/restore"}
	if got := supervisor.seen(); !equal(got, wantSupervisor) {
		t.Errorf("supervisor saw %v, want %v", got, wantSupervisor)
	}
	wantWorker := []string{"/admin/slow", "/admin/blackhole", "/admin/429", "/admin/corrupt", "/admin/reset"}
	if got := worker.seen(); !equal(got, wantWorker) {
		t.Errorf("worker saw %v, want %v", got, wantWorker)
	}
}

func TestChaosParametersReachTheWorker(t *testing.T) {
	_, srv, worker, _ := newTestControl(t)
	resp, err := http.Post(srv.URL+"/api/chaos/worker-a/slow?ms=1234", "", nil)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()

	worker.mu.Lock()
	body := worker.body[0]
	worker.mu.Unlock()
	if !strings.Contains(body, "1234") {
		t.Errorf("worker received %q, want the ms parameter", body)
	}
}

// A parameter that does not parse must fall back to the documented
// default, not to the zero value: `?ms=` turning into a 0ms slow fault
// would report success for an action that did nothing at all.
func TestUnparseableParametersFallBackToDefaultsNotZero(t *testing.T) {
	_, srv, worker, _ := newTestControl(t)
	resp, err := http.Post(srv.URL+"/api/chaos/worker-a/slow?ms=notanumber", "", nil)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()

	worker.mu.Lock()
	body := worker.body[0]
	worker.mu.Unlock()
	if !strings.Contains(body, "500") {
		t.Errorf("worker received %q, want the 500ms default", body)
	}
}

func TestDrainIsHandledLocallyAndNeverReachesTheWorker(t *testing.T) {
	_, srv, worker, supervisor := newTestControl(t)
	resp, err := http.Post(srv.URL+"/api/chaos/worker-a/drain?on=true", "", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d", resp.StatusCode)
	}
	// Drain is a SELECTION decision. Asking the worker to start refusing
	// would surface as errors the router then has to unlearn.
	if len(worker.seen()) != 0 || len(supervisor.seen()) != 0 {
		t.Errorf("drain reached a worker endpoint: worker=%v supervisor=%v", worker.seen(), supervisor.seen())
	}
}

func TestUnknownWorkerAndActionAreRejected(t *testing.T) {
	_, srv, _, _ := newTestControl(t)
	resp, _ := http.Post(srv.URL+"/api/chaos/worker-zzz/kill", "", nil)
	if resp.StatusCode != http.StatusNotFound {
		t.Errorf("unknown worker: status = %d, want 404", resp.StatusCode)
	}
	resp.Body.Close()

	resp, _ = http.Post(srv.URL+"/api/chaos/worker-a/explode", "", nil)
	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("unknown action: status = %d, want 400", resp.StatusCode)
	}
	resp.Body.Close()
}

// Every chaos action must land in the event feed, because the latency
// chart draws its fault markers from it. A spike with no annotated cause
// is the exact reading error the markers exist to prevent.
func TestChaosActionsAppearInTheEventFeed(t *testing.T) {
	c, srv, _, _ := newTestControl(t)
	resp, _ := http.Post(srv.URL+"/api/chaos/worker-a/kill", "", nil)
	resp.Body.Close()

	events := c.Hub.Since(0)
	if len(events) != 1 || events[0].Kind != "chaos" || events[0].Worker != "worker-a" {
		t.Fatalf("event feed = %+v, want one chaos event for worker-a", events)
	}
}

func TestStreamsAndLogEndpoints(t *testing.T) {
	c, srv, _, _ := newTestControl(t)
	c.Hub.SessionStarted("s-1", "online", "worker-a", "k")
	c.Hub.Observe(session.PartialEvent{Type: "partial", SessionID: "s-1", Revision: 1, Text: "hello"})

	resp, err := http.Get(srv.URL + "/api/streams")
	if err != nil {
		t.Fatal(err)
	}
	var list struct {
		Streams []Stream `json:"streams"`
		Count   int      `json:"count"`
	}
	json.NewDecoder(resp.Body).Decode(&list)
	resp.Body.Close()
	if list.Count != 1 || list.Streams[0].ID != "s-1" {
		t.Fatalf("streams = %+v", list)
	}

	resp, err = http.Get(srv.URL + "/api/streams/s-1/log")
	if err != nil {
		t.Fatal(err)
	}
	var log struct {
		Events []Event `json:"events"`
	}
	json.NewDecoder(resp.Body).Decode(&log)
	resp.Body.Close()
	// The inspector is the ONE place partials are served, because they are
	// deliberately not broadcast.
	var sawPartial bool
	for _, ev := range log.Events {
		if ev.Kind == "partial" && ev.Text == "hello" {
			sawPartial = true
		}
	}
	if !sawPartial {
		t.Errorf("stream log = %+v, want the partial", log.Events)
	}

	resp, _ = http.Get(srv.URL + "/api/streams/s-nope/log")
	if resp.StatusCode != http.StatusNotFound {
		t.Errorf("unknown stream: status = %d, want 404", resp.StatusCode)
	}
	resp.Body.Close()
}

func TestDashboardIsServedFromTheEmbeddedFS(t *testing.T) {
	_, srv, _, _ := newTestControl(t)
	resp, err := http.Get(srv.URL + "/dashboard/")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d", resp.StatusCode)
	}
	buf := make([]byte, 512)
	n, _ := resp.Body.Read(buf)
	if !strings.Contains(string(buf[:n]), "<!DOCTYPE html>") {
		t.Errorf("body did not start with an HTML document: %q", buf[:n])
	}
}

// A reviewer is told to open localhost:7000. Landing on a 404 there looks
// exactly like a broken build.
func TestRootRedirectsToTheDashboard(t *testing.T) {
	_, srv, _, _ := newTestControl(t)
	client := &http.Client{CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	resp, err := client.Get(srv.URL + "/")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusFound || resp.Header.Get("Location") != "/dashboard/" {
		t.Errorf("status=%d location=%q, want 302 to /dashboard/", resp.StatusCode, resp.Header.Get("Location"))
	}
}

// The SSE contract, exercised over a real connection: correct media type,
// a snapshot before anything else, and discrete events carrying ids so a
// reconnect can resume.
func TestSSEStreamsSnapshotsThenEvents(t *testing.T) {
	c, srv, _, _ := newTestControl(t)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	req, _ := http.NewRequestWithContext(ctx, "GET", srv.URL+"/api/events", nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	if ct := resp.Header.Get("Content-Type"); ct != "text/event-stream" {
		t.Fatalf("content-type = %q", ct)
	}

	sc := bufio.NewScanner(resp.Body)
	var sawRetry, sawSnapshot, sawEvent, sawID bool

	// The first snapshot is written before the first tick, so a reader
	// sees state immediately rather than a blank quarter-second.
	deadline := time.Now().Add(4 * time.Second)
	fired := false
	for time.Now().Before(deadline) && sc.Scan() {
		line := sc.Text()
		switch {
		case strings.HasPrefix(line, "retry:"):
			sawRetry = true
		case line == "event: snapshot":
			sawSnapshot = true
			if !fired {
				fired = true
				// Produce a notable event now that the stream is live.
				go c.Hub.Chaos("worker-a", "kill", "SIGKILL")
			}
		case line == "event: event":
			sawEvent = true
		case strings.HasPrefix(line, "id: "):
			sawID = true
		}
		if sawRetry && sawSnapshot && sawEvent && sawID {
			break
		}
	}

	if !sawRetry {
		t.Error("no retry: hint — the browser would use the 3s default after a kill")
	}
	if !sawSnapshot {
		t.Error("no snapshot frame")
	}
	if !sawEvent || !sawID {
		t.Errorf("discrete event seen=%v with id=%v; both are required for Last-Event-ID resume", sawEvent, sawID)
	}
}

// Last-Event-ID is the whole reason this feed is SSE rather than a second
// WebSocket: the browser reconnects itself and the server closes the gap.
func TestLastEventIDReplaysTheGap(t *testing.T) {
	c, srv, _, _ := newTestControl(t)
	c.Hub.Chaos("worker-a", "kill", "first")
	c.Hub.Chaos("worker-a", "restore", "second")
	c.Hub.Chaos("worker-a", "slow", "third")

	all := c.Hub.Since(0)
	if len(all) != 3 {
		t.Fatalf("seeded %d events, want 3", len(all))
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	req, _ := http.NewRequestWithContext(ctx, "GET", srv.URL+"/api/events", nil)
	req.Header.Set("Last-Event-ID", strings.TrimSpace(itoa(all[0].ID)))
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	sc := bufio.NewScanner(resp.Body)
	var replayed []string
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) && sc.Scan() {
		line := sc.Text()
		if strings.HasPrefix(line, "data: ") && strings.Contains(line, `"kind":"chaos"`) {
			var ev Event
			if json.Unmarshal([]byte(strings.TrimPrefix(line, "data: ")), &ev) == nil {
				replayed = append(replayed, ev.Detail)
			}
		}
		if line == "event: snapshot" && len(replayed) > 0 {
			break // replay is written before the first snapshot
		}
	}

	want := []string{"kill first", "restore second", "slow third"}
	// The client already has the first event, so only the two after it
	// should come back.
	if !equal(replayed, want[1:]) {
		t.Errorf("replayed %v, want %v", replayed, want[1:])
	}
}

func itoa(v uint64) string {
	b, _ := json.Marshal(v)
	return string(b)
}

func equal(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
