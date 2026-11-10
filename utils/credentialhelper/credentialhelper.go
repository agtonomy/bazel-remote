// Package credentialhelper implements the client side of the Bazel credential
// helper protocol, which allows an external binary to supply authentication
// headers for requests to a remote server.
//
// See https://github.com/EngFlow/credential-helper-spec for the protocol.
package credentialhelper

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os/exec"
	"strings"
	"sync"
	"time"

	"golang.org/x/sync/singleflight"
	"google.golang.org/grpc/credentials"
)

const (
	// DefaultTimeout matches Bazel's default --credential_helper_timeout.
	DefaultTimeout = 10 * time.Second

	// DefaultCacheDuration matches Bazel's default
	// --credential_helper_cache_duration, and is used for responses
	// that do not specify an expiry time.
	DefaultCacheDuration = 30 * time.Minute
)

type getCredentialsRequest struct {
	URI string `json:"uri"`
}

type getCredentialsResponse struct {
	Expires string              `json:"expires,omitempty"`
	Headers map[string][]string `json:"headers"`
}

type cacheEntry struct {
	headers http.Header
	expires time.Time
}

// Helper invokes an external credential helper binary and caches the
// returned headers per URI, until they expire.
type Helper struct {
	path                 string
	timeout              time.Duration
	defaultCacheDuration time.Duration
	now                  func() time.Time

	group singleflight.Group

	mu    sync.Mutex
	cache map[string]cacheEntry
}

// New returns a Helper which runs the credential helper binary at path.
// If path does not contain a path separator, it is looked up in $PATH.
// An error is returned if the binary cannot be found.
func New(path string) (*Helper, error) {
	_, err := exec.LookPath(path)
	if err != nil {
		return nil, fmt.Errorf("invalid credential helper %q: %w", path, err)
	}

	return &Helper{
		path:                 path,
		timeout:              DefaultTimeout,
		defaultCacheDuration: DefaultCacheDuration,
		now:                  time.Now,
		cache:                make(map[string]cacheEntry),
	}, nil
}

// Get returns the headers to attach to requests for uri, invoking the
// credential helper if there are no unexpired cached headers for uri.
// The returned header must not be modified.
//
// If ctx is done before the helper finishes, Get returns ctx.Err(), but
// the helper keeps running so that its result can be cached for other
// callers.
func (h *Helper) Get(ctx context.Context, uri string) (http.Header, error) {
	if headers, ok := h.cached(uri); ok {
		return headers, nil
	}

	// Coalesce concurrent lookups for the same URI into a single invocation
	// of the helper.
	ch := h.group.DoChan(uri, func() (interface{}, error) {
		if headers, ok := h.cached(uri); ok {
			return headers, nil
		}

		entry, err := h.invoke(uri)
		if err != nil {
			return nil, err
		}

		if entry.expires.After(h.now()) {
			h.mu.Lock()
			h.cache[uri] = entry
			h.mu.Unlock()
		}

		return entry.headers, nil
	})

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case res := <-ch:
		if res.Err != nil {
			return nil, res.Err
		}
		return res.Val.(http.Header), nil
	}
}

func (h *Helper) cached(uri string) (http.Header, bool) {
	h.mu.Lock()
	defer h.mu.Unlock()

	entry, ok := h.cache[uri]
	if !ok {
		return nil, false
	}

	if !entry.expires.After(h.now()) {
		delete(h.cache, uri)
		return nil, false
	}

	return entry.headers, true
}

func (h *Helper) invoke(uri string) (cacheEntry, error) {
	req, err := json.Marshal(getCredentialsRequest{URI: uri})
	if err != nil {
		return cacheEntry{}, err
	}

	// Don't tie the helper's lifetime to any single caller's context, since
	// the result is shared with all concurrent callers for this URI.
	ctx, cancel := context.WithTimeout(context.Background(), h.timeout)
	defer cancel()

	var stdout, stderr bytes.Buffer
	cmd := exec.CommandContext(ctx, h.path, "get")
	cmd.Stdin = bytes.NewReader(req)
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	// Only the helper process itself is killed when ctx expires. Don't
	// wait indefinitely for any of its child processes which still hold
	// stdout or stderr open.
	cmd.WaitDelay = time.Second

	err = cmd.Run()
	if ctx.Err() == context.DeadlineExceeded {
		return cacheEntry{}, fmt.Errorf("failed to get credentials for %q from helper %q: timed out after %s",
			uri, h.path, h.timeout)
	}
	if err != nil {
		return cacheEntry{}, fmt.Errorf("failed to get credentials for %q from helper %q: %w, stderr: %s",
			uri, h.path, err, strings.TrimSpace(stderr.String()))
	}

	var resp getCredentialsResponse
	err = json.Unmarshal(stdout.Bytes(), &resp)
	if err != nil {
		return cacheEntry{}, fmt.Errorf("failed to parse credentials for %q from helper %q: %w",
			uri, h.path, err)
	}

	headers := make(http.Header, len(resp.Headers))
	for name, values := range resp.Headers {
		for _, value := range values {
			headers.Add(name, value)
		}
	}

	entry := cacheEntry{
		headers: headers,
		expires: h.now().Add(h.defaultCacheDuration),
	}

	if resp.Expires != "" {
		entry.expires, err = time.Parse(time.RFC3339, resp.Expires)
		if err != nil {
			return cacheEntry{}, fmt.Errorf("failed to parse expiry time %q for %q from helper %q: %w",
				resp.Expires, uri, h.path, err)
		}
	}

	return entry, nil
}

// HTTPTransport returns an http.RoundTripper which adds the headers that
// the credential helper returns for uri to each request, before passing
// it to base. If base is nil, http.DefaultTransport is used.
func (h *Helper) HTTPTransport(uri string, base http.RoundTripper) http.RoundTripper {
	if base == nil {
		base = http.DefaultTransport
	}

	return &roundTripper{helper: h, uri: uri, base: base}
}

type roundTripper struct {
	helper *Helper
	uri    string
	base   http.RoundTripper
}

func (rt *roundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	headers, err := rt.helper.Get(req.Context(), rt.uri)
	if err != nil {
		if req.Body != nil {
			_ = req.Body.Close()
		}
		return nil, err
	}

	// RoundTrippers must not modify the request, so make a copy.
	req = req.Clone(req.Context())
	for name, values := range headers {
		req.Header.Del(name)
		for _, value := range values {
			req.Header.Add(name, value)
		}
	}

	return rt.base.RoundTrip(req)
}

// PerRPCCredentials returns gRPC credentials which add the headers returned
// by the credential helper to each RPC.
//
// Like Bazel, the URI passed to the credential helper is of the form
// "https://<authority>/<service name>", regardless of whether TLS is used.
func (h *Helper) PerRPCCredentials() credentials.PerRPCCredentials {
	return &perRPCCredentials{helper: h}
}

type perRPCCredentials struct {
	helper *Helper
}

func (c *perRPCCredentials) GetRequestMetadata(ctx context.Context, uri ...string) (map[string]string, error) {
	if len(uri) == 0 {
		return nil, fmt.Errorf("no URI provided for credential helper %q", c.helper.path)
	}

	headers, err := c.helper.Get(ctx, uri[0])
	if err != nil {
		return nil, err
	}

	md := make(map[string]string, len(headers))
	for name, values := range headers {
		md[strings.ToLower(name)] = strings.Join(values, ", ")
	}

	return md, nil
}

func (c *perRPCCredentials) RequireTransportSecurity() bool {
	// Allow insecure grpc:// backends, for consistency with the existing
	// basic auth support for the grpc proxy backend.
	return false
}
