package config

import (
	"errors"
	"fmt"

	"github.com/buchgr/bazel-remote/v2/cache/azblobproxy"
	"github.com/buchgr/bazel-remote/v2/cache/s3proxy"
)

// ProxyConfig stores the configuration of one proxy backend. Exactly one
// of the backend fields must be set for each entry in the "proxies" list.
type ProxyConfig struct {
	// ReadOnly backends are only read from, never uploaded to.
	ReadOnly bool `yaml:"read_only"`

	S3CloudStorage     *S3CloudStorageConfig     `yaml:"s3_proxy,omitempty"`
	AzBlobConfig       *AzBlobStorageConfig      `yaml:"azblob_proxy,omitempty"`
	GoogleCloudStorage *GoogleCloudStorageConfig `yaml:"gcs_proxy,omitempty"`
	HTTPBackend        *URLBackendConfig         `yaml:"http_proxy,omitempty"`
	GRPCBackend        *URLBackendConfig         `yaml:"grpc_proxy,omitempty"`
}

// singleProxyConfig returns the proxy backend configured with the
// top-level s3_proxy, gcs_proxy, etc keys/flags, rather than the "proxies"
// list.
func (c *Config) singleProxyConfig() *ProxyConfig {
	return &ProxyConfig{
		S3CloudStorage:     c.S3CloudStorage,
		AzBlobConfig:       c.AzBlobConfig,
		GoogleCloudStorage: c.GoogleCloudStorage,
		HTTPBackend:        c.HTTPBackend,
		GRPCBackend:        c.GRPCBackend,
	}
}

func (p *ProxyConfig) count() int {
	n := 0
	if p.S3CloudStorage != nil {
		n++
	}
	if p.AzBlobConfig != nil {
		n++
	}
	if p.GoogleCloudStorage != nil {
		n++
	}
	if p.HTTPBackend != nil {
		n++
	}
	if p.GRPCBackend != nil {
		n++
	}
	return n
}

// name returns a description of the proxy backend for log messages.
func (p *ProxyConfig) name() string {
	switch {
	case p.GRPCBackend != nil:
		return "grpc_proxy " + p.GRPCBackend.BaseURL.Redacted()
	case p.HTTPBackend != nil:
		return "http_proxy " + p.HTTPBackend.BaseURL.Redacted()
	case p.S3CloudStorage != nil:
		return fmt.Sprintf("s3_proxy %s/%s", p.S3CloudStorage.Endpoint, p.S3CloudStorage.Bucket)
	case p.GoogleCloudStorage != nil:
		return "gcs_proxy " + p.GoogleCloudStorage.Bucket
	case p.AzBlobConfig != nil:
		return fmt.Sprintf("azblob_proxy %s/%s", p.AzBlobConfig.StorageAccount, p.AzBlobConfig.ContainerName)
	}
	return "unknown proxy"
}

func (c *Config) validateProxies() error {
	single := c.singleProxyConfig()
	if single.count() > 1 {
		return errors.New("at most one of the S3/GCS/HTTP proxy backends is allowed, use 'proxies' to configure multiple proxy backends")
	}

	err := single.validate()
	if err != nil {
		return err
	}

	if len(c.Proxies) == 0 {
		return nil
	}

	if single.count() > 0 {
		return errors.New("'proxies' cannot be used together with the top-level s3_proxy/azblob_proxy/gcs_proxy/http_proxy/grpc_proxy settings")
	}

	writable := 0
	for i := range c.Proxies {
		p := &c.Proxies[i]
		if p.count() != 1 {
			return fmt.Errorf("each entry in 'proxies' must configure exactly one proxy backend, entry %d has %d", i, p.count())
		}

		err := p.validate()
		if err != nil {
			return fmt.Errorf("invalid entry %d in 'proxies': %w", i, err)
		}

		if !p.ReadOnly {
			writable++
		}
	}

	if writable > 1 {
		return errors.New("at most one entry in 'proxies' may be writable, set 'read_only: true' on the others")
	}

	return nil
}

func (p *ProxyConfig) validate() error {
	if p.GoogleCloudStorage != nil {
		if p.GoogleCloudStorage.Bucket == "" {
			return errors.New("the 'bucket' field is required for 'gcs_proxy'")
		}
	}

	if p.HTTPBackend != nil {
		if err := p.HTTPBackend.validate("http"); err != nil {
			return err
		}
	}

	if p.GRPCBackend != nil {
		if err := p.GRPCBackend.validate("grpc"); err != nil {
			return err
		}
	}

	if p.S3CloudStorage != nil {
		if !s3proxy.IsValidAuthMethod(p.S3CloudStorage.AuthMethod) {
			return fmt.Errorf("invalid s3.auth_method: %s", p.S3CloudStorage.AuthMethod)
		}

		if p.S3CloudStorage.KeyVersion != nil && *p.S3CloudStorage.KeyVersion != 2 {
			return fmt.Errorf("s3.key_version (deprecated) must be 2, found %d", p.S3CloudStorage.KeyVersion)
		}

		if p.S3CloudStorage.BucketLookupType != "" && p.S3CloudStorage.BucketLookupType != "auto" &&
			p.S3CloudStorage.BucketLookupType != "dns" && p.S3CloudStorage.BucketLookupType != "path" {
			return fmt.Errorf("s3.bucket_lookup_type must be one of: \"auto\", \"dns\", \"path\" or empty/unspecified, found: \"%s\"",
				p.S3CloudStorage.BucketLookupType)
		}

		if p.S3CloudStorage.SignatureType != "" && p.S3CloudStorage.SignatureType != "v2" &&
			p.S3CloudStorage.SignatureType != "v4" && p.S3CloudStorage.SignatureType != "v4streaming" &&
			p.S3CloudStorage.SignatureType != "anonymous" {
			return fmt.Errorf("s3.signature_type must be one of: \"v2\", \"v4\", \"v4streaming\", \"anonymous\" or empty/unspecified, found: \"%s\"",
				p.S3CloudStorage.SignatureType)
		}
	}

	if p.AzBlobConfig != nil {
		if p.AzBlobConfig.StorageAccount == "" {
			return errors.New("the 'storage_account' field is required for 'azblob_proxy'")
		}

		if p.AzBlobConfig.ContainerName == "" {
			return errors.New("the 'container_name' field is required for 'azblob_proxy'")
		}

		if !azblobproxy.IsValidAuthMethod(p.AzBlobConfig.AuthMethod) {
			return fmt.Errorf("invalid azblob.auth_method: %s", p.AzBlobConfig.AuthMethod)
		}
	}

	return nil
}
