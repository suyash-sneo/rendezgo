package redis_scripts

import (
	"crypto/sha1" //nolint:gosec // used for deterministic script hash
	"encoding/hex"
)

const (
	Renew   = `if redis.call("get", KEYS[1]) == ARGV[1] then return redis.call("pexpire", KEYS[1], ARGV[2]) else return 0 end`
	Release = `if redis.call("get", KEYS[1]) == ARGV[1] then return redis.call("del", KEYS[1]) else return 0 end`
)

// Script wraps a Lua source and precomputed sha.
type Script struct {
	Source string
	SHA    string
}

// NewScript builds a Script with deterministic sha1.
func NewScript(src string) Script {
	sum := sha1.Sum([]byte(src))
	return Script{
		Source: src,
		SHA:    hex.EncodeToString(sum[:]),
	}
}
