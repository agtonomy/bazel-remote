package config

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"errors"
	"fmt"
	"net/http"
	"os"
	"time"

	"github.com/buchgr/bazel-remote/v2/cache"
	"github.com/buchgr/bazel-remote/v2/cache/azblobproxy"
	"github.com/buchgr/bazel-remote/v2/cache/gcsproxy"
	"github.com/buchgr/bazel-remote/v2/cache/grpcproxy"
	"github.com/buchgr/bazel-remote/v2/cache/httpproxy"
	"github.com/buchgr/bazel-remote/v2/cache/multiproxy"
	"github.com/buchgr/bazel-remote/v2/cache/s3proxy"
	"github.com/buchgr/bazel-remote/v2/utils/credentialhelper"
	"github.com/minio/minio-go/v7"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"

	grpc_prometheus "github.com/grpc-ecosystem/go-grpc-prometheus"
	prom "github.com/prometheus/client_golang/prometheus"
)

func getTLSConfig(certFile, keyFile, caFile string) (*tls.Config, error) {
	config := &tls.Config{}
	if certFile != "" && keyFile != "" {
		readCert, err := tls.LoadX509KeyPair(certFile, keyFile)
		if err != nil {
			return nil, err
		}

		config.Certificates = []tls.Certificate{readCert}
	}
	if caFile != "" {
		caCert, err := os.ReadFile(caFile)
		if err != nil {
			return nil, err
		}
		caCertPool := x509.NewCertPool()
		if added := caCertPool.AppendCertsFromPEM(caCert); !added {
			return nil, fmt.Errorf("failed to add CA cert to cert pool")
		}
		config.RootCAs = caCertPool
	}
	return config, nil
}

func (c *Config) setProxy() error {
	b := proxyBuilder{c: c}

	if len(c.Proxies) == 0 {
		single := c.singleProxyConfig()
		if single.count() == 0 {
			return nil
		}

		proxy, err := b.newProxy(single, false)
		if err != nil {
			return err
		}

		c.ProxyBackend = proxy
		return nil
	}

	var backends []multiproxy.Backend
	for i := range c.Proxies {
		p := &c.Proxies[i]

		proxy, err := b.newProxy(p, true)
		if err != nil {
			return fmt.Errorf("failed to set up %s: %w", p.name(), err)
		}

		backends = append(backends, multiproxy.Backend{
			Name:     p.name(),
			Proxy:    proxy,
			ReadOnly: p.ReadOnly,
		})
	}

	proxy, err := multiproxy.New(backends, c.ErrorLogger)
	if err != nil {
		return err
	}

	c.ProxyBackend = proxy
	return nil
}

// How long to wait at startup when checking the capabilities of a grpc
// backend in the "proxies" list, before treating it as unavailable.
// A variable so that tests can override it.
var capabilitiesCheckTimeout = 10 * time.Second

type proxyBuilder struct {
	c *Config

	// Shared by all grpc proxy backends, since metrics can only be
	// registered once.
	grpcMetrics *grpc_prometheus.ClientMetrics
}

func (b *proxyBuilder) getGRPCMetrics() (*grpc_prometheus.ClientMetrics, error) {
	if b.grpcMetrics != nil {
		return b.grpcMetrics, nil
	}

	metrics := grpc_prometheus.NewClientMetrics(func(o *prom.CounterOpts) { o.Namespace = "proxy" })
	metrics.EnableClientHandlingTimeHistogram(func(o *prom.HistogramOpts) { o.Namespace = "proxy" })
	err := prom.Register(metrics)
	if err != nil {
		return nil, err
	}

	b.grpcMetrics = metrics
	return metrics, nil
}

// newProxy creates the proxy backend configured by p. If tolerateUnavailable
// is true, then a grpc proxy backend which cannot be reached at startup is
// logged instead of being treated as an error.
func (b *proxyBuilder) newProxy(p *ProxyConfig, tolerateUnavailable bool) (cache.Proxy, error) {
	c := b.c

	// Read-only backends are never uploaded to, so don't start uploaders
	// or allocate an upload queue for them.
	numUploaders, maxQueuedUploads := c.NumUploaders, c.MaxQueuedUploads
	if p.ReadOnly {
		numUploaders, maxQueuedUploads = 0, 0
	}

	if p.GoogleCloudStorage != nil {
		return gcsproxy.New(p.GoogleCloudStorage.Bucket,
			p.GoogleCloudStorage.UseDefaultCredentials, p.GoogleCloudStorage.JSONCredentialsFile,
			c.StorageMode, c.AccessLogger, c.ErrorLogger, numUploaders, maxQueuedUploads)
	}

	if p.GRPCBackend != nil {
		var opts []grpc.DialOption
		if p.GRPCBackend.BaseURL.Scheme == "grpcs" {
			config, err := getTLSConfig(p.GRPCBackend.CertFile, p.GRPCBackend.KeyFile, p.GRPCBackend.CaFile)
			if err != nil {
				return nil, err
			}
			creds := credentials.NewTLS(config)
			opts = append(opts, grpc.WithTransportCredentials(creds))
		} else {
			opts = append(opts, grpc.WithTransportCredentials(insecure.NewCredentials()))
		}
		if password, ok := p.GRPCBackend.BaseURL.User.Password(); ok {
			username := p.GRPCBackend.BaseURL.User.Username()
			auth := base64.StdEncoding.EncodeToString([]byte(fmt.Sprintf("%s:%s", username, password)))
			header := fmt.Sprintf("Basic %s", auth)
			unaryAuth := func(ctx context.Context, method string, req, res interface{}, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
				return invoker(metadata.AppendToOutgoingContext(ctx, "Authorization", header), method, req, res, cc, opts...)
			}
			streamAuth := func(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, streamer grpc.Streamer, opts ...grpc.CallOption) (grpc.ClientStream, error) {
				return streamer(metadata.AppendToOutgoingContext(ctx, "Authorization", header), desc, cc, method, opts...)
			}
			opts = append(opts, grpc.WithChainUnaryInterceptor(unaryAuth), grpc.WithStreamInterceptor(streamAuth))
		}
		if p.GRPCBackend.CredentialHelper != "" {
			helper, err := credentialhelper.New(p.GRPCBackend.CredentialHelper)
			if err != nil {
				return nil, err
			}
			opts = append(opts, grpc.WithPerRPCCredentials(helper.PerRPCCredentials()))
		}

		metrics, err := b.getGRPCMetrics()
		if err != nil {
			return nil, err
		}
		opts = append(opts, grpc.WithChainStreamInterceptor(metrics.StreamClientInterceptor()))
		opts = append(opts, grpc.WithChainUnaryInterceptor(metrics.UnaryClientInterceptor()))

		conn, err := grpc.NewClient(p.GRPCBackend.BaseURL.Host, opts...)
		if err != nil {
			return nil, err
		}
		clients := grpcproxy.NewGrpcClients(conn)

		// Don't let a backend which is unreachable (e.g. packets silently
		// dropped) hold up startup for long, when it is allowed to be
		// unavailable.
		ctx := context.Background()
		if tolerateUnavailable {
			var cancel context.CancelFunc
			ctx, cancel = context.WithTimeout(ctx, capabilitiesCheckTimeout)
			defer cancel()
		}

		err = clients.CheckCapabilities(ctx, c.StorageMode == "zstd", !p.ReadOnly)
		if err != nil {
			// An incompatible backend is a configuration error, but one
			// which is unavailable might come back later.
			if !tolerateUnavailable || errors.Is(err, grpcproxy.ErrIncompatibleBackend) {
				return nil, err
			}
			c.ErrorLogger.Printf("WARNING: failed to check the capabilities of %s, continuing since it may become available later: %v",
				p.name(), err)
		}

		return grpcproxy.New(clients, c.StorageMode,
			c.AccessLogger, c.ErrorLogger, numUploaders, maxQueuedUploads), nil
	}

	if p.HTTPBackend != nil {
		httpClient := &http.Client{}
		if p.HTTPBackend.BaseURL.Scheme == "https" {
			config, err := getTLSConfig(p.HTTPBackend.CertFile, p.HTTPBackend.KeyFile, p.HTTPBackend.CaFile)
			if err != nil {
				return nil, err
			}
			tr := &http.Transport{TLSClientConfig: config}
			httpClient.Transport = tr
		}
		if p.HTTPBackend.CredentialHelper != "" {
			helper, err := credentialhelper.New(p.HTTPBackend.CredentialHelper)
			if err != nil {
				return nil, err
			}
			httpClient.Transport = helper.HTTPTransport(p.HTTPBackend.BaseURL.String(), httpClient.Transport)
		}

		return httpproxy.New(p.HTTPBackend.BaseURL, c.StorageMode,
			httpClient, c.AccessLogger, c.ErrorLogger, numUploaders, maxQueuedUploads)
	}

	if p.S3CloudStorage != nil {
		creds, err := p.S3CloudStorage.GetCredentials()
		if err != nil {
			return nil, err
		}

		bucketLookupType, err := parseBucketLookupType(p.S3CloudStorage.BucketLookupType)
		if err != nil {
			return nil, err
		}
		return s3proxy.New(
			p.S3CloudStorage.Endpoint,
			p.S3CloudStorage.Bucket,
			bucketLookupType,
			p.S3CloudStorage.Prefix,
			creds,
			p.S3CloudStorage.DisableSSL,
			p.S3CloudStorage.UpdateTimestamps,
			p.S3CloudStorage.Region,
			p.S3CloudStorage.MaxIdleConns,
			c.StorageMode, c.AccessLogger, c.ErrorLogger, numUploaders, maxQueuedUploads), nil
	}

	if p.AzBlobConfig != nil {
		creds, err := p.AzBlobConfig.GetCredentials()
		if err != nil {
			return nil, err
		}

		return azblobproxy.New(
			p.AzBlobConfig.StorageAccount,
			p.AzBlobConfig.ContainerName,
			p.AzBlobConfig.Prefix,
			creds,
			p.AzBlobConfig.SharedKey,
			p.AzBlobConfig.UpdateTimestamps,
			c.StorageMode, c.AccessLogger, c.ErrorLogger, numUploaders, maxQueuedUploads,
		), nil
	}

	return nil, errors.New("no proxy backend configured")
}

func parseBucketLookupType(typeStr string) (minio.BucketLookupType, error) {
	valMap := map[string]minio.BucketLookupType{
		"auto": minio.BucketLookupAuto,
		"dns":  minio.BucketLookupDNS,
		"path": minio.BucketLookupPath,
	}

	val, ok := valMap[typeStr]
	if !ok {
		return 0, fmt.Errorf("unsupported bucket_lookup_type value : %s", typeStr)
	}

	return val, nil
}
