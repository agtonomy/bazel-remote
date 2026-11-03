package multiproxy

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/buchgr/bazel-remote/v2/cache"
)

// fakeProxy is a cache.Proxy which serves blobs from a map, or returns err
// for every Get when it is set.
type fakeProxy struct {
	mu       sync.Mutex
	blobs    map[string][]byte
	err      error
	gets     int
	contains int
	puts     []string
}

func newFakeProxy(blobs ...string) *fakeProxy {
	p := &fakeProxy{blobs: make(map[string][]byte)}
	for _, b := range blobs {
		p.blobs[b] = []byte("data-" + b)
	}
	return p
}

func (p *fakeProxy) setErr(err error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.err = err
}

func (p *fakeProxy) Put(ctx context.Context, kind cache.EntryKind, hash string, logicalSize int64, sizeOnDisk int64, rc io.ReadCloser) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.puts = append(p.puts, hash)
	_ = rc.Close()
}

func (p *fakeProxy) Get(ctx context.Context, kind cache.EntryKind, hash string, size int64) (io.ReadCloser, int64, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.gets++
	if p.err != nil {
		return nil, -1, p.err
	}
	data, ok := p.blobs[hash]
	if !ok {
		return nil, -1, nil
	}
	return io.NopCloser(bytes.NewReader(data)), int64(len(data)), nil
}

func (p *fakeProxy) Contains(ctx context.Context, kind cache.EntryKind, hash string, size int64) (bool, int64) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.contains++
	if p.err != nil {
		return false, -1
	}
	data, ok := p.blobs[hash]
	if !ok {
		return false, -1
	}
	return true, int64(len(data))
}

type closeTracker struct {
	io.Reader
	closed bool
}

func (c *closeTracker) Close() error {
	c.closed = true
	return nil
}

type testLogger struct {
	mu   sync.Mutex
	logs []string
}

func (l *testLogger) Printf(format string, v ...interface{}) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.logs = append(l.logs, fmt.Sprintf(format, v...))
}

func (l *testLogger) contains(s string) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	for _, line := range l.logs {
		if strings.Contains(line, s) {
			return true
		}
	}
	return false
}

func newMulti(t *testing.T, backends ...Backend) (*multiProxy, *testLogger) {
	t.Helper()
	logger := &testLogger{}
	p, err := New(backends, logger)
	if err != nil {
		t.Fatal(err)
	}
	return p.(*multiProxy), logger
}

func mustGet(t *testing.T, p cache.Proxy, hash string) string {
	t.Helper()
	rc, _, err := p.Get(context.Background(), cache.CAS, hash, -1)
	if err != nil {
		t.Fatalf("unexpected error getting %s: %v", hash, err)
	}
	if rc == nil {
		return ""
	}
	defer func() { _ = rc.Close() }()
	data, err := io.ReadAll(rc)
	if err != nil {
		t.Fatal(err)
	}
	return string(data)
}

func TestGetInOrder(t *testing.T) {
	a := newFakeProxy("both", "a-only")
	b := newFakeProxy("both", "b-only")
	b.blobs["both"] = []byte("from-b")

	p, _ := newMulti(t,
		Backend{Name: "a", Proxy: a, ReadOnly: true},
		Backend{Name: "b", Proxy: b, ReadOnly: true})

	if got := mustGet(t, p, "both"); got != "data-both" {
		t.Errorf("expected the first backend's blob, got %q", got)
	}
	if b.gets != 0 {
		t.Errorf("expected the second backend not to be queried on a hit, got %d gets", b.gets)
	}

	if got := mustGet(t, p, "b-only"); got != "data-b-only" {
		t.Errorf("expected fallback to the second backend, got %q", got)
	}

	if got := mustGet(t, p, "missing"); got != "" {
		t.Errorf("expected a miss, got %q", got)
	}
}

func TestGetToleratesErrors(t *testing.T) {
	a := newFakeProxy("blob")
	a.setErr(errors.New("connection refused"))
	b := newFakeProxy("blob")

	p, _ := newMulti(t,
		Backend{Name: "a", Proxy: a, ReadOnly: true},
		Backend{Name: "b", Proxy: b, ReadOnly: true})

	if got := mustGet(t, p, "blob"); got != "data-blob" {
		t.Errorf("expected the blob from the second backend, got %q", got)
	}

	// An error from one backend and a miss from the other is a miss.
	if got := mustGet(t, p, "missing"); got != "" {
		t.Errorf("expected a miss, got %q", got)
	}
}

func TestGetAllBackendsFail(t *testing.T) {
	a := newFakeProxy()
	a.setErr(errors.New("a is down"))
	b := newFakeProxy()
	b.setErr(errors.New("b is down"))

	p, _ := newMulti(t,
		Backend{Name: "a", Proxy: a, ReadOnly: true},
		Backend{Name: "b", Proxy: b, ReadOnly: true})

	_, _, err := p.Get(context.Background(), cache.CAS, "blob", -1)
	if err == nil || err.Error() != "b is down" {
		t.Fatalf("expected the last backend's error, got %v", err)
	}
}

func TestUnavailableBackendIsSkipped(t *testing.T) {
	a := newFakeProxy("blob")
	a.setErr(errors.New("connection refused"))
	b := newFakeProxy("blob")

	p, logger := newMulti(t,
		Backend{Name: "a", Proxy: a, ReadOnly: true},
		Backend{Name: "b", Proxy: b, ReadOnly: true})

	now := time.Now()
	p.now = func() time.Time { return now }

	for range failureThreshold {
		mustGet(t, p, "blob")
	}
	if a.gets != failureThreshold {
		t.Fatalf("expected %d gets to the failing backend, got %d", failureThreshold, a.gets)
	}
	if !logger.contains("backend a is unavailable") {
		t.Errorf("expected a log message about the unavailable backend, got %q", logger.logs)
	}

	// The failing backend is now skipped, for both Get and Contains.
	if got := mustGet(t, p, "blob"); got != "data-blob" {
		t.Errorf("expected the blob from the second backend, got %q", got)
	}
	ok, _ := p.Contains(context.Background(), cache.CAS, "blob", -1)
	if !ok {
		t.Error("expected Contains to find the blob in the second backend")
	}
	if a.gets != failureThreshold || a.contains != 0 {
		t.Fatalf("expected the unavailable backend to be skipped, got %d gets and %d contains", a.gets, a.contains)
	}

	// A failed retry after the cooldown makes it unavailable again.
	now = now.Add(cooldown)
	mustGet(t, p, "blob")
	if a.gets != failureThreshold+1 {
		t.Fatalf("expected the backend to be retried after the cooldown, got %d gets", a.gets)
	}
	mustGet(t, p, "blob")
	if a.gets != failureThreshold+1 {
		t.Fatalf("expected the backend to be skipped after a failed retry, got %d gets", a.gets)
	}

	// After it recovers, it is used again.
	a.setErr(nil)
	now = now.Add(cooldown)
	mustGet(t, p, "blob")
	if !logger.contains("backend a is available again") {
		t.Errorf("expected a log message about the recovered backend, got %q", logger.logs)
	}
	before := a.gets
	mustGet(t, p, "blob")
	if a.gets != before+1 {
		t.Fatal("expected the recovered backend to be used")
	}
}

func TestFailuresMustBeConsecutive(t *testing.T) {
	a := newFakeProxy("blob")
	p, _ := newMulti(t, Backend{Name: "a", Proxy: a, ReadOnly: true})

	for range 3 * failureThreshold {
		a.setErr(errors.New("flaky"))
		_, _, _ = p.Get(context.Background(), cache.CAS, "blob", -1)
		a.setErr(nil)
		mustGet(t, p, "blob")
	}

	if !p.available(p.backends[0]) {
		t.Fatal("expected the backend to stay available with non-consecutive failures")
	}
}

func TestCancelledContextIsNotABackendFailure(t *testing.T) {
	a := newFakeProxy()
	a.setErr(context.Canceled)
	p, _ := newMulti(t, Backend{Name: "a", Proxy: a, ReadOnly: true})

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	for range 2 * failureThreshold {
		_, _, err := p.Get(ctx, cache.CAS, "blob", -1)
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("expected context.Canceled, got %v", err)
		}
	}

	if !p.available(p.backends[0]) {
		t.Fatal("expected the backend to stay available")
	}
}

func TestContainsInOrder(t *testing.T) {
	a := newFakeProxy("a-only")
	b := newFakeProxy("b-only")

	p, _ := newMulti(t,
		Backend{Name: "a", Proxy: a, ReadOnly: true},
		Backend{Name: "b", Proxy: b, ReadOnly: true})

	for _, hash := range []string{"a-only", "b-only"} {
		ok, size := p.Contains(context.Background(), cache.CAS, hash, -1)
		if !ok || size != int64(len("data-"+hash)) {
			t.Errorf("expected to find %s, got %v %d", hash, ok, size)
		}
	}
	if b.contains != 1 {
		t.Errorf("expected the second backend to be queried once, got %d", b.contains)
	}

	ok, _ := p.Contains(context.Background(), cache.CAS, "missing", -1)
	if ok {
		t.Error("expected a miss")
	}
}

func TestPutReadOnly(t *testing.T) {
	a := newFakeProxy()
	b := newFakeProxy()

	p, _ := newMulti(t,
		Backend{Name: "a", Proxy: a, ReadOnly: true},
		Backend{Name: "b", Proxy: b, ReadOnly: true})

	rc := &closeTracker{Reader: strings.NewReader("data")}
	p.Put(context.Background(), cache.CAS, "blob", 4, 4, rc)

	if len(a.puts) != 0 || len(b.puts) != 0 {
		t.Errorf("expected no uploads to read-only backends, got %q and %q", a.puts, b.puts)
	}
	if !rc.closed {
		t.Error("expected the reader to be closed")
	}
}

func TestPutWritable(t *testing.T) {
	a := newFakeProxy()
	b := newFakeProxy()

	p, _ := newMulti(t,
		Backend{Name: "a", Proxy: a, ReadOnly: true},
		Backend{Name: "b", Proxy: b})

	p.Put(context.Background(), cache.CAS, "blob", 4, 4, io.NopCloser(strings.NewReader("data")))

	if len(a.puts) != 0 {
		t.Errorf("expected no uploads to the read-only backend, got %q", a.puts)
	}
	if len(b.puts) != 1 || b.puts[0] != "blob" {
		t.Errorf("expected an upload to the writable backend, got %q", b.puts)
	}

	// Uploads to an unavailable writable backend are dropped.
	b.setErr(errors.New("down"))
	for range failureThreshold {
		_, _, _ = p.Get(context.Background(), cache.CAS, "x", -1)
	}
	rc := &closeTracker{Reader: strings.NewReader("data")}
	p.Put(context.Background(), cache.CAS, "blob2", 4, 4, rc)
	if len(b.puts) != 1 {
		t.Errorf("expected no uploads to the unavailable backend, got %q", b.puts)
	}
	if !rc.closed {
		t.Error("expected the reader to be closed")
	}
}

func TestNewErrors(t *testing.T) {
	_, err := New(nil, &testLogger{})
	if err == nil {
		t.Error("expected an error with no backends")
	}

	_, err = New([]Backend{
		{Name: "a", Proxy: newFakeProxy()},
		{Name: "b", Proxy: newFakeProxy()},
	}, &testLogger{})
	if err == nil || !strings.Contains(err.Error(), "at most one proxy backend may be writable") {
		t.Errorf("expected an error with two writable backends, got %v", err)
	}
}
