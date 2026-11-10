package credentialhelper

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

// When this environment variable is set, the test binary acts as a fake
// credential helper instead of running tests.
const fakeHelperModeEnv = "BAZEL_REMOTE_FAKE_CREDENTIAL_HELPER_MODE"

// Directory the fake helper writes invocation records to.
const fakeHelperDirEnv = "BAZEL_REMOTE_FAKE_CREDENTIAL_HELPER_DIR"

func TestMain(m *testing.M) {
	mode := os.Getenv(fakeHelperModeEnv)
	if mode == "" {
		os.Exit(m.Run())
	}

	os.Exit(fakeHelper(mode))
}

func fakeHelper(mode string) int {
	if len(os.Args) != 2 || os.Args[1] != "get" {
		fmt.Fprintf(os.Stderr, "unexpected args: %v", os.Args[1:])
		return 1
	}

	input, err := io.ReadAll(os.Stdin)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}

	var req getCredentialsRequest
	err = json.Unmarshal(input, &req)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}

	// Record the invocation, so tests can count them.
	f, err := os.CreateTemp(os.Getenv(fakeHelperDirEnv), "invocation-*")
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	_, _ = f.WriteString(req.URI)
	_ = f.Close()

	resp := getCredentialsResponse{
		Headers: map[string][]string{
			"Authorization": {"Bearer token-for-" + req.URI},
			"X-Extra":       {"a", "b"},
		},
	}

	switch mode {
	case "ok":
	case "expires":
		resp.Expires = time.Now().Add(time.Hour).Format(time.RFC3339)
	case "expired":
		resp.Expires = time.Now().Add(-time.Hour).Format(time.RFC3339)
	case "bad_expires":
		resp.Expires = "tomorrow"
	case "slow":
		time.Sleep(200 * time.Millisecond)
	case "hang":
		time.Sleep(time.Minute)
	case "fail":
		fmt.Fprint(os.Stderr, "not logged in")
		return 1
	case "bad_json":
		fmt.Print("not json")
		return 0
	default:
		fmt.Fprintf(os.Stderr, "unknown mode %q", mode)
		return 1
	}

	err = json.NewEncoder(os.Stdout).Encode(resp)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}

	return 0
}

// newFakeHelper returns a Helper which runs this test binary as the
// credential helper in the given mode, and a function which returns the
// URIs the helper has been invoked with.
func newFakeHelper(t *testing.T, mode string) (*Helper, func() []string) {
	t.Helper()

	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}

	dir := t.TempDir()
	t.Setenv(fakeHelperModeEnv, mode)
	t.Setenv(fakeHelperDirEnv, dir)

	invocations := func() []string {
		entries, err := os.ReadDir(dir)
		if err != nil {
			t.Fatal(err)
		}
		var uris []string
		for _, e := range entries {
			data, err := os.ReadFile(filepath.Join(dir, e.Name()))
			if err != nil {
				t.Fatal(err)
			}
			uris = append(uris, string(data))
		}
		return uris
	}

	h, err := New(exe)
	if err != nil {
		t.Fatal(err)
	}

	return h, invocations
}

func TestGet(t *testing.T) {
	h, invocations := newFakeHelper(t, "ok")

	headers, err := h.Get(context.Background(), "https://example.com/foo")
	if err != nil {
		t.Fatal(err)
	}

	if got, want := headers.Get("Authorization"), "Bearer token-for-https://example.com/foo"; got != want {
		t.Errorf("got Authorization %q, want %q", got, want)
	}
	if got := headers.Values("X-Extra"); len(got) != 2 || got[0] != "a" || got[1] != "b" {
		t.Errorf("got X-Extra %q, want [a b]", got)
	}

	// The second lookup should be served from the cache.
	_, err = h.Get(context.Background(), "https://example.com/foo")
	if err != nil {
		t.Fatal(err)
	}
	if n := len(invocations()); n != 1 {
		t.Errorf("expected 1 helper invocation, got %d", n)
	}

	// A different URI should invoke the helper again.
	headers, err = h.Get(context.Background(), "https://example.com/bar")
	if err != nil {
		t.Fatal(err)
	}
	if got, want := headers.Get("Authorization"), "Bearer token-for-https://example.com/bar"; got != want {
		t.Errorf("got Authorization %q, want %q", got, want)
	}
	if n := len(invocations()); n != 2 {
		t.Errorf("expected 2 helper invocations, got %d", n)
	}
}

func TestDefaultCacheDuration(t *testing.T) {
	h, invocations := newFakeHelper(t, "ok")

	now := time.Now()
	h.now = func() time.Time { return now }

	for range 2 {
		_, err := h.Get(context.Background(), "https://example.com")
		if err != nil {
			t.Fatal(err)
		}
	}
	if n := len(invocations()); n != 1 {
		t.Fatalf("expected 1 helper invocation, got %d", n)
	}

	now = now.Add(DefaultCacheDuration - time.Second)
	_, err := h.Get(context.Background(), "https://example.com")
	if err != nil {
		t.Fatal(err)
	}
	if n := len(invocations()); n != 1 {
		t.Fatalf("expected 1 helper invocation before expiry, got %d", n)
	}

	now = now.Add(2 * time.Second)
	_, err = h.Get(context.Background(), "https://example.com")
	if err != nil {
		t.Fatal(err)
	}
	if n := len(invocations()); n != 2 {
		t.Fatalf("expected 2 helper invocations after expiry, got %d", n)
	}
}

func TestExpires(t *testing.T) {
	h, invocations := newFakeHelper(t, "expires")

	now := time.Now()
	h.now = func() time.Time { return now }

	_, err := h.Get(context.Background(), "https://example.com")
	if err != nil {
		t.Fatal(err)
	}

	// Past the default cache duration, but before the helper's expiry time.
	now = now.Add(DefaultCacheDuration + time.Minute)
	_, err = h.Get(context.Background(), "https://example.com")
	if err != nil {
		t.Fatal(err)
	}
	if n := len(invocations()); n != 1 {
		t.Fatalf("expected 1 helper invocation before expiry, got %d", n)
	}

	now = now.Add(time.Hour)
	_, err = h.Get(context.Background(), "https://example.com")
	if err != nil {
		t.Fatal(err)
	}
	if n := len(invocations()); n != 2 {
		t.Fatalf("expected 2 helper invocations after expiry, got %d", n)
	}
}

func TestAlreadyExpiredIsNotCached(t *testing.T) {
	h, invocations := newFakeHelper(t, "expired")

	for range 2 {
		headers, err := h.Get(context.Background(), "https://example.com")
		if err != nil {
			t.Fatal(err)
		}
		if headers.Get("Authorization") == "" {
			t.Fatal("expected an Authorization header")
		}
	}

	if n := len(invocations()); n != 2 {
		t.Fatalf("expected 2 helper invocations, got %d", n)
	}
}

func TestConcurrentGetsAreCoalesced(t *testing.T) {
	h, invocations := newFakeHelper(t, "slow")

	var wg sync.WaitGroup
	errs := make(chan error, 10)
	for range 10 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := h.Get(context.Background(), "https://example.com")
			errs <- err
		}()
	}
	wg.Wait()
	close(errs)

	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}

	if n := len(invocations()); n != 1 {
		t.Fatalf("expected 1 helper invocation, got %d", n)
	}
}

func TestErrors(t *testing.T) {
	tcs := []struct {
		mode    string
		wantErr string
	}{
		{mode: "fail", wantErr: "not logged in"},
		{mode: "bad_json", wantErr: "failed to parse credentials"},
		{mode: "bad_expires", wantErr: "failed to parse expiry time"},
		{mode: "hang", wantErr: "timed out"},
	}

	for _, tc := range tcs {
		t.Run(tc.mode, func(t *testing.T) {
			h, _ := newFakeHelper(t, tc.mode)
			if tc.mode == "hang" {
				h.timeout = 2 * time.Second
			}

			_, err := h.Get(context.Background(), "https://example.com")
			if err == nil {
				t.Fatal("expected an error")
			}
			if !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("expected error containing %q, got %q", tc.wantErr, err)
			}
		})
	}
}

func TestMissingBinary(t *testing.T) {
	for _, path := range []string{
		filepath.Join(t.TempDir(), "does-not-exist"),
		"bazel-remote-credential-helper-which-does-not-exist",
	} {
		_, err := New(path)
		if err == nil {
			t.Fatalf("expected an error for %q", path)
		}
	}
}

func TestGetHonorsContext(t *testing.T) {
	h, _ := newFakeHelper(t, "hang")
	h.timeout = 2 * time.Second

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	start := time.Now()
	_, err := h.Get(ctx, "https://example.com")
	if err != context.DeadlineExceeded {
		t.Fatalf("expected context.DeadlineExceeded, got %v", err)
	}
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Fatalf("Get took %s, expected it to return when the context expired", elapsed)
	}
}

func TestTimeoutWithChildProcess(t *testing.T) {
	// The helper's child process inherits stdout and outlives the helper
	// when it is killed on timeout.
	helper := filepath.Join(t.TempDir(), "helper.sh")
	err := os.WriteFile(helper, []byte("#!/bin/sh\nsleep 60\n"), 0755)
	if err != nil {
		t.Fatal(err)
	}

	h, err := New(helper)
	if err != nil {
		t.Fatal(err)
	}
	h.timeout = 200 * time.Millisecond

	start := time.Now()
	_, err = h.Get(context.Background(), "https://example.com")
	if err == nil || !strings.Contains(err.Error(), "timed out") {
		t.Fatalf("expected a timeout error, got %v", err)
	}
	if elapsed := time.Since(start); elapsed > 5*time.Second {
		t.Fatalf("Get took %s, expected it to time out", elapsed)
	}
}

func TestHTTPTransport(t *testing.T) {
	h, invocations := newFakeHelper(t, "ok")

	var gotAuth []string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotAuth = r.Header.Values("Authorization")
	}))
	defer srv.Close()

	client := &http.Client{Transport: h.HTTPTransport("https://cache.example.com/prefix", nil)}

	req, err := http.NewRequest(http.MethodGet, srv.URL+"/prefix/cas/abc", nil)
	if err != nil {
		t.Fatal(err)
	}
	// Headers from the helper should replace any existing values.
	req.Header.Set("Authorization", "stale")

	resp, err := client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()

	if len(gotAuth) != 1 || gotAuth[0] != "Bearer token-for-https://cache.example.com/prefix" {
		t.Errorf("unexpected Authorization headers: %q", gotAuth)
	}
	if req.Header.Get("Authorization") != "stale" {
		t.Error("the original request was modified")
	}

	uris := invocations()
	if len(uris) != 1 || uris[0] != "https://cache.example.com/prefix" {
		t.Errorf("unexpected helper invocations: %q", uris)
	}
}

func TestPerRPCCredentials(t *testing.T) {
	h, _ := newFakeHelper(t, "ok")

	creds := h.PerRPCCredentials()
	md, err := creds.GetRequestMetadata(context.Background(),
		"https://cache.example.com/build.bazel.remote.execution.v2.ActionCache")
	if err != nil {
		t.Fatal(err)
	}

	want := map[string]string{
		"authorization": "Bearer token-for-https://cache.example.com/build.bazel.remote.execution.v2.ActionCache",
		"x-extra":       "a, b",
	}
	if len(md) != len(want) {
		t.Fatalf("got %v, want %v", md, want)
	}
	for k, v := range want {
		if md[k] != v {
			t.Errorf("got %s=%q, want %q", k, md[k], v)
		}
	}
}
