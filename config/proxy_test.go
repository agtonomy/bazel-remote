package config

import (
	"context"
	"io"
	"log"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"sync"
	"testing"

	pb "github.com/buchgr/bazel-remote/v2/genproto/build/bazel/remote/execution/v2"
	prom "github.com/prometheus/client_golang/prometheus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

type capabilitiesServer struct {
	pb.UnimplementedCapabilitiesServer

	mu   sync.Mutex
	auth []string
}

func (s *capabilitiesServer) GetCapabilities(ctx context.Context, req *pb.GetCapabilitiesRequest) (*pb.ServerCapabilities, error) {
	md, _ := metadata.FromIncomingContext(ctx)

	s.mu.Lock()
	s.auth = append(s.auth, md.Get("authorization")...)
	s.mu.Unlock()

	return &pb.ServerCapabilities{
		CacheCapabilities: &pb.CacheCapabilities{
			DigestFunctions: []pb.DigestFunction_Value{pb.DigestFunction_SHA256},
			ActionCacheUpdateCapabilities: &pb.ActionCacheUpdateCapabilities{
				UpdateEnabled: true,
			},
		},
	}, nil
}

// useTestRegistry makes metrics registered by the test go to a fresh
// prometheus registry, so setProxy can be called more than once.
func useTestRegistry(t *testing.T) {
	orig := prom.DefaultRegisterer
	prom.DefaultRegisterer = prom.NewRegistry()
	t.Cleanup(func() { prom.DefaultRegisterer = orig })
}

func TestGRPCProxyCredentialHelper(t *testing.T) {
	useTestRegistry(t)
	dir := t.TempDir()

	// A credential helper which returns a token derived from the requested URI.
	helper := filepath.Join(dir, "helper.sh")
	script := `#!/bin/sh
[ "$1" = "get" ] || exit 1
uri=$(sed -e 's/.*"uri":"\([^"]*\)".*/\1/')
printf '{"headers":{"Authorization":["Bearer %s"]}}' "$uri"
`
	err := os.WriteFile(helper, []byte(script), 0755)
	if err != nil {
		t.Fatal(err)
	}

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	backend := &capabilitiesServer{}
	srv := grpc.NewServer()
	pb.RegisterCapabilitiesServer(srv, backend)
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
		GRPCBackend: &URLBackendConfig{
			BaseURL:          u,
			CredentialHelper: helper,
		},
	}

	// setProxy checks the backend's capabilities, which should be
	// authenticated with the headers from the credential helper.
	err = c.setProxy()
	if err != nil {
		t.Fatal(err)
	}

	want := "Bearer https://" + listener.Addr().String() + "/build.bazel.remote.execution.v2.Capabilities"

	backend.mu.Lock()
	defer backend.mu.Unlock()
	if len(backend.auth) != 1 || backend.auth[0] != want {
		t.Fatalf("expected authorization header %q, got %q", want, backend.auth)
	}
}
