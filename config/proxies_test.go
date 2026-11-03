package config

import (
	"bytes"
	"context"
	"io"
	"log"
	"net"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/buchgr/bazel-remote/v2/cache"
	pb "github.com/buchgr/bazel-remote/v2/genproto/build/bazel/remote/execution/v2"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

func TestProxiesConfig(t *testing.T) {
	yaml := `dir: /opt/cache-dir
max_size: 100
proxies:
  - read_only: true
    grpc_proxy:
      url: grpcs://cache-a.example.com:443
      credential_helper: /usr/bin/helper
  - http_proxy:
      url: https://cache-b.example.com/prefix
  - read_only: true
    gcs_proxy:
      bucket: my-bucket
`
	config, err := NewFromYaml([]byte(yaml))
	if err != nil {
		t.Fatal(err)
	}

	if len(config.Proxies) != 3 {
		t.Fatalf("expected 3 proxies, got %d", len(config.Proxies))
	}

	a, b, c := config.Proxies[0], config.Proxies[1], config.Proxies[2]
	if !a.ReadOnly || a.GRPCBackend == nil || a.GRPCBackend.BaseURL.Host != "cache-a.example.com:443" ||
		a.GRPCBackend.CredentialHelper != "/usr/bin/helper" {
		t.Errorf("unexpected first proxy: %+v", a)
	}
	if b.ReadOnly || b.HTTPBackend == nil || b.HTTPBackend.BaseURL.String() != "https://cache-b.example.com/prefix" {
		t.Errorf("unexpected second proxy: %+v", b)
	}
	if !c.ReadOnly || c.GoogleCloudStorage == nil || c.GoogleCloudStorage.Bucket != "my-bucket" {
		t.Errorf("unexpected third proxy: %+v", c)
	}
	if config.GRPCBackend != nil || config.HTTPBackend != nil || config.GoogleCloudStorage != nil {
		t.Error("expected no top-level proxy backends")
	}

	if got := a.name(); got != "grpc_proxy grpcs://cache-a.example.com:443" {
		t.Errorf("unexpected name %q", got)
	}
}

func TestProxiesConfigErrors(t *testing.T) {
	tcs := []struct {
		name    string
		yaml    string
		wantErr string
	}{
		{
			name: "combined with top-level backend",
			yaml: `
grpc_proxy:
  url: grpc://a:1
proxies:
  - grpc_proxy:
      url: grpc://b:1
`,
			wantErr: "'proxies' cannot be used together",
		},
		{
			name: "no backend in entry",
			yaml: `
proxies:
  - read_only: true
`,
			wantErr: "entry 0 has 0",
		},
		{
			name: "two backends in entry",
			yaml: `
proxies:
  - grpc_proxy:
      url: grpc://a:1
    http_proxy:
      url: http://b:1
`,
			wantErr: "entry 0 has 2",
		},
		{
			name: "two writable",
			yaml: `
proxies:
  - grpc_proxy:
      url: grpc://a:1
  - grpc_proxy:
      url: grpc://b:1
`,
			wantErr: "at most one entry in 'proxies' may be writable",
		},
		{
			name: "invalid backend",
			yaml: `
proxies:
  - read_only: true
    grpc_proxy:
      url: http://a:1
`,
			wantErr: "invalid entry 0 in 'proxies'",
		},
	}

	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			_, err := NewFromYaml([]byte("dir: /opt/cache-dir\nmax_size: 100\n" + tc.yaml))
			if err == nil {
				t.Fatal("expected an error")
			}
			if !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("expected an error containing %q, got %q", tc.wantErr, err)
			}
		})
	}
}

type actionCacheServer struct {
	pb.UnimplementedActionCacheServer

	results map[string]*pb.ActionResult
}

func (s *actionCacheServer) GetActionResult(ctx context.Context, req *pb.GetActionResultRequest) (*pb.ActionResult, error) {
	ar, ok := s.results[req.GetActionDigest().GetHash()]
	if !ok {
		return nil, status.Error(codes.NotFound, "not found")
	}
	return ar, nil
}

func TestProxiesToleratesUnavailableBackend(t *testing.T) {
	useTestRegistry(t)

	// An address with nothing listening on it.
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	offlineAddr := l.Addr().String()
	_ = l.Close()

	// A read-only backend which has one action result.
	hash := strings.Repeat("a", 64)
	ar := &pb.ActionResult{ExitCode: 42}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	srv := grpc.NewServer()
	pb.RegisterCapabilitiesServer(srv, &capabilitiesServer{readOnly: true})
	pb.RegisterActionCacheServer(srv, &actionCacheServer{results: map[string]*pb.ActionResult{hash: ar}})
	go func() { _ = srv.Serve(listener) }()
	defer srv.Stop()

	parse := func(s string) *url.URL {
		u, err := url.Parse(s)
		if err != nil {
			t.Fatal(err)
		}
		return u
	}

	var logs bytes.Buffer
	logger := log.New(&logs, "", 0)
	c := &Config{
		StorageMode:      "uncompressed",
		NumUploaders:     1,
		MaxQueuedUploads: 1,
		AccessLogger:     log.New(io.Discard, "", 0),
		ErrorLogger:      logger,
		Proxies: []ProxyConfig{
			{ReadOnly: true, GRPCBackend: &URLBackendConfig{BaseURL: parse("grpc://" + offlineAddr)}},
			{ReadOnly: true, GRPCBackend: &URLBackendConfig{BaseURL: parse("grpc://" + listener.Addr().String())}},
		},
	}

	err = c.validateProxies()
	if err != nil {
		t.Fatal(err)
	}

	// Startup succeeds even though the first backend is offline, and the
	// second does not allow action cache updates.
	err = c.setProxy()
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(logs.String(), "WARNING: failed to check the capabilities of grpc_proxy grpc://"+offlineAddr) {
		t.Errorf("expected a warning about the offline backend, got %q", logs.String())
	}

	rc, _, err := c.ProxyBackend.Get(context.Background(), cache.AC, hash, -1)
	if err != nil {
		t.Fatal(err)
	}
	if rc == nil {
		t.Fatal("expected a hit from the second backend")
	}
	data, err := io.ReadAll(rc)
	if err != nil {
		t.Fatal(err)
	}
	got := &pb.ActionResult{}
	err = proto.Unmarshal(data, got)
	if err != nil {
		t.Fatal(err)
	}
	if !proto.Equal(got, ar) {
		t.Fatalf("expected %v, got %v", ar, got)
	}

	rc, _, err = c.ProxyBackend.Get(context.Background(), cache.AC, strings.Repeat("b", 64), -1)
	if err != nil || rc != nil {
		t.Fatalf("expected a miss, got %v %v", rc, err)
	}
}

func TestProxiesStartupCheckTimeout(t *testing.T) {
	useTestRegistry(t)

	orig := capabilitiesCheckTimeout
	capabilitiesCheckTimeout = 300 * time.Millisecond
	t.Cleanup(func() { capabilitiesCheckTimeout = orig })

	// The kernel completes TCP handshakes for this listener, but since
	// nothing accepts the connections the grpc handshake never completes.
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = listener.Close() }()

	u, err := url.Parse("grpc://" + listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}

	var logs bytes.Buffer
	c := &Config{
		StorageMode:  "uncompressed",
		AccessLogger: log.New(io.Discard, "", 0),
		ErrorLogger:  log.New(&logs, "", 0),
		Proxies:      []ProxyConfig{{ReadOnly: true, GRPCBackend: &URLBackendConfig{BaseURL: u}}},
	}

	start := time.Now()
	err = c.setProxy()
	if err != nil {
		t.Fatal(err)
	}
	if elapsed := time.Since(start); elapsed > 5*time.Second {
		t.Fatalf("startup took %s, expected the capabilities check to time out", elapsed)
	}
	if !strings.Contains(logs.String(), "DeadlineExceeded") {
		t.Errorf("expected a warning about the timed out check, got %q", logs.String())
	}
}

func TestProxiesIncompatibleBackendIsAnError(t *testing.T) {
	useTestRegistry(t)

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	srv := grpc.NewServer()
	pb.RegisterCapabilitiesServer(srv, &capabilitiesServer{readOnly: true})
	go func() { _ = srv.Serve(listener) }()
	defer srv.Stop()

	u, err := url.Parse("grpc://" + listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}

	logger := log.New(io.Discard, "", 0)
	c := &Config{
		StorageMode:      "uncompressed",
		NumUploaders:     1,
		MaxQueuedUploads: 1,
		AccessLogger:     logger,
		ErrorLogger:      logger,
		// Writable, but the backend does not allow action cache updates.
		Proxies: []ProxyConfig{{GRPCBackend: &URLBackendConfig{BaseURL: u}}},
	}

	err = c.setProxy()
	if err == nil || !strings.Contains(err.Error(), "does not allow action cache updates") {
		t.Fatalf("expected an incompatible backend error, got %v", err)
	}
}
