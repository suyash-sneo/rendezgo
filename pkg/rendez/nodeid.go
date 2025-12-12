package rendez

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"os"
	"strings"
	"sync"
)

// NodeIDProvider builds stable node identifiers.
type NodeIDProvider interface {
	NodeID() (string, error)
}

// DefaultNodeIDProvider builds node IDs from host/env information.
type DefaultNodeIDProvider struct {
	prefix    string
	addSuffix bool

	once sync.Once
	id   string
	err  error
}

// DefaultNodeIDOption mutates provider construction.
type DefaultNodeIDOption func(*DefaultNodeIDProvider)

// WithNodePrefix sets a prefix (e.g., cluster or region).
func WithNodePrefix(prefix string) DefaultNodeIDOption {
	return func(p *DefaultNodeIDProvider) {
		p.prefix = prefix
	}
}

// WithoutRandomSuffix disables the random suffix appended for uniqueness.
func WithoutRandomSuffix() DefaultNodeIDOption {
	return func(p *DefaultNodeIDProvider) {
		p.addSuffix = false
	}
}

// NewDefaultNodeIDProvider constructs a provider using environment hints.
func NewDefaultNodeIDProvider(opts ...DefaultNodeIDOption) *DefaultNodeIDProvider {
	p := &DefaultNodeIDProvider{
		addSuffix: true,
	}
	for _, opt := range opts {
		opt(p)
	}
	return p
}

// NodeID returns a stable ID for the process lifetime.
func (p *DefaultNodeIDProvider) NodeID() (string, error) {
	p.once.Do(func() {
		base := firstNonEmpty(
			os.Getenv("POD_UID"),
			os.Getenv("HOSTNAME"),
			readHostname(),
		)
		if base == "" {
			p.err = fmt.Errorf("no hostname or env var found for node id")
			return
		}
		parts := []string{}
		if p.prefix != "" {
			parts = append(parts, sanitize(p.prefix))
		}
		parts = append(parts, sanitize(base))
		if p.addSuffix {
			parts = append(parts, randomSuffix(4))
		}
		p.id = strings.Join(parts, "-")
	})
	return p.id, p.err
}

func firstNonEmpty(values ...string) string {
	for _, v := range values {
		if strings.TrimSpace(v) != "" {
			return v
		}
	}
	return ""
}

func readHostname() string {
	h, _ := os.Hostname()
	return h
}

func sanitize(s string) string {
	return strings.ToLower(strings.ReplaceAll(strings.TrimSpace(s), " ", "-"))
}

func randomSuffix(bytesLen int) string {
	buf := make([]byte, bytesLen)
	if _, err := rand.Read(buf); err != nil {
		return "rnd"
	}
	return hex.EncodeToString(buf)
}
