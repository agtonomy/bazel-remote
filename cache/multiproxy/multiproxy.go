// Package multiproxy implements a cache.Proxy which reads from a list of
// proxy backends in order, and tolerates backends which are unavailable.
package multiproxy

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/buchgr/bazel-remote/v2/cache"
)

const (
	// The number of consecutive errors after which a backend is considered
	// unavailable.
	failureThreshold = 3

	// How long to skip an unavailable backend for, before trying it again.
	cooldown = 30 * time.Second
)

// Backend is a proxy backend to use in a multiproxy.
type Backend struct {
	// Name identifies the backend in log messages.
	Name string

	Proxy cache.Proxy

	// ReadOnly backends are never uploaded to.
	ReadOnly bool
}

type backend struct {
	Backend

	mu        sync.Mutex
	failures  int
	downUntil time.Time
}

type multiProxy struct {
	backends    []*backend
	writable    *backend
	errorLogger cache.Logger
	now         func() time.Time
}

// New returns a cache.Proxy which tries each of the given backends in order
// for Get and Contains requests, and uploads to the backend which is not
// read-only, if any. At most one backend may be writable.
//
// Errors from a backend are treated as cache misses, as long as some other
// backend returns a result. After several consecutive errors a backend is
// skipped for a while, so that an unavailable backend does not add latency
// to every request.
func New(backends []Backend, errorLogger cache.Logger) (cache.Proxy, error) {
	if len(backends) == 0 {
		return nil, errors.New("at least one proxy backend is required")
	}

	p := &multiProxy{
		errorLogger: errorLogger,
		now:         time.Now,
	}

	for _, b := range backends {
		be := &backend{Backend: b}
		p.backends = append(p.backends, be)

		if b.ReadOnly {
			continue
		}

		if p.writable != nil {
			return nil, fmt.Errorf("at most one proxy backend may be writable, found %q and %q (set read_only on all but one)",
				p.writable.Name, b.Name)
		}
		p.writable = be
	}

	return p, nil
}

func (p *multiProxy) available(b *backend) bool {
	b.mu.Lock()
	defer b.mu.Unlock()

	return b.failures < failureThreshold || !p.now().Before(b.downUntil)
}

func (p *multiProxy) recordSuccess(b *backend) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.failures >= failureThreshold {
		p.errorLogger.Printf("MULTI PROXY backend %s is available again", b.Name)
	}
	b.failures = 0
}

func (p *multiProxy) recordFailure(b *backend, err error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.failures++
	if b.failures < failureThreshold {
		return
	}

	// Log only when the backend first becomes unavailable, not each time
	// a retry after the cooldown fails.
	if b.failures == failureThreshold {
		p.errorLogger.Printf("MULTI PROXY backend %s is unavailable after %d consecutive errors, skipping it for %s: %v",
			b.Name, b.failures, cooldown, err)
	}
	b.downUntil = p.now().Add(cooldown)
}

func (p *multiProxy) Put(ctx context.Context, kind cache.EntryKind, hash string, logicalSize int64, sizeOnDisk int64, rc io.ReadCloser) {
	if p.writable == nil || !p.available(p.writable) {
		_ = rc.Close()
		return
	}

	p.writable.Proxy.Put(ctx, kind, hash, logicalSize, sizeOnDisk, rc)
}

func (p *multiProxy) Get(ctx context.Context, kind cache.EntryKind, hash string, size int64) (io.ReadCloser, int64, error) {
	var lastErr error
	failed := 0
	tried := 0

	for _, b := range p.backends {
		if !p.available(b) {
			continue
		}
		tried++

		rc, foundSize, err := b.Proxy.Get(ctx, kind, hash, size)
		if err != nil {
			if rc != nil {
				_ = rc.Close()
			}

			// The client went away, this is not the backend's fault.
			if ctx.Err() != nil {
				return nil, -1, ctx.Err()
			}

			p.recordFailure(b, err)
			p.errorLogger.Printf("MULTI PROXY GET %s %s from %s: %v", kind, hash, b.Name, err)
			lastErr = err
			failed++
			continue
		}

		p.recordSuccess(b)

		if rc != nil {
			return rc, foundSize, nil
		}
	}

	// Only report an error if no backend was able to give an answer.
	if tried > 0 && failed == tried {
		return nil, -1, lastErr
	}

	return nil, -1, nil
}

func (p *multiProxy) Contains(ctx context.Context, kind cache.EntryKind, hash string, size int64) (bool, int64) {
	for _, b := range p.backends {
		if !p.available(b) {
			continue
		}

		// Proxy.Contains does not return errors, so this cannot be used to
		// track backend availability.
		ok, foundSize := b.Proxy.Contains(ctx, kind, hash, size)
		if ok {
			return true, foundSize
		}
	}

	return false, -1
}
