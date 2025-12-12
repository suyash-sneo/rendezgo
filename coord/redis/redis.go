package redis

import (
	"context"
	"errors"
	"fmt"
	"time"

	goredis "github.com/redis/go-redis/v9"

	"github.com/you/workdist/coord"
)

const defaultPrefix = "workdist:"

// Options configure the Redis store.
type Options struct {
	Addr           string
	SentinelAddrs  []string
	SentinelMaster string
	Username       string
	Password       string
	DB             int
	KeyPrefix      string
	TLS            bool
}

// Store implements coord.Store using Redis.
type Store struct {
	client goredis.UniversalClient
	prefix string
}

// ErrWatchUnsupported indicates config watch is not implemented yet.
var ErrWatchUnsupported = errors.New("watch config not supported in redis backend")

// New creates a Redis-backed store. Supports single instance or Sentinel via UniversalClient.
func New(opts Options) (*Store, error) {
	prefix := opts.KeyPrefix
	if prefix == "" {
		prefix = defaultPrefix
	}

	client := goredis.NewUniversalClient(&goredis.UniversalOptions{
		Addrs:      addrs(opts),
		MasterName: opts.SentinelMaster,
		Username:   opts.Username,
		Password:   opts.Password,
		DB:         opts.DB,
		TLSConfig:  nil,
	})

	if err := client.Ping(context.Background()).Err(); err != nil {
		return nil, fmt.Errorf("redis ping failed: %w", err)
	}

	return &Store{
		client: client,
		prefix: prefix,
	}, nil
}

func addrs(opts Options) []string {
	if len(opts.SentinelAddrs) > 0 {
		return opts.SentinelAddrs
	}
	if opts.Addr != "" {
		return []string{opts.Addr}
	}
	return []string{"127.0.0.1:6379"}
}

// Close releases the Redis client.
func (s *Store) Close() error {
	return s.client.Close()
}

func (s *Store) HeartbeatNode(ctx context.Context, nodeID string, ttl time.Duration) error {
	_, err := s.client.Pipelined(ctx, func(p goredis.Pipeliner) error {
		p.Set(ctx, s.nodeKey(nodeID), "1", ttl)
		p.SAdd(ctx, s.nodesSetKey(), nodeID)
		return nil
	})
	return err
}

func (s *Store) ListNodes(ctx context.Context) ([]string, error) {
	members, err := s.client.SMembers(ctx, s.nodesSetKey()).Result()
	if err != nil {
		return nil, err
	}
	if len(members) == 0 {
		return nil, nil
	}

	pipe := s.client.Pipeline()
	existsCmds := make([]*goredis.IntCmd, 0, len(members))
	for _, id := range members {
		existsCmds = append(existsCmds, pipe.Exists(ctx, s.nodeKey(id)))
	}
	if _, err := pipe.Exec(ctx); err != nil && !errors.Is(err, goredis.Nil) {
		return nil, err
	}

	live := make([]string, 0, len(members))
	for i, cmd := range existsCmds {
		if cmd.Val() > 0 {
			live = append(live, members[i])
		}
	}
	return live, nil
}

func (s *Store) PruneDeadNodes(ctx context.Context) (int, error) {
	members, err := s.client.SMembers(ctx, s.nodesSetKey()).Result()
	if err != nil {
		return 0, err
	}
	if len(members) == 0 {
		return 0, nil
	}

	pipe := s.client.Pipeline()
	existsCmds := make([]*goredis.IntCmd, 0, len(members))
	for _, id := range members {
		existsCmds = append(existsCmds, pipe.Exists(ctx, s.nodeKey(id)))
	}
	if _, err := pipe.Exec(ctx); err != nil && !errors.Is(err, goredis.Nil) {
		return 0, err
	}

	var removed int
	for i, cmd := range existsCmds {
		if cmd.Val() == 0 {
			if err := s.client.SRem(ctx, s.nodesSetKey(), members[i]).Err(); err != nil {
				return removed, err
			}
			removed++
		}
	}
	return removed, nil
}

func (s *Store) AcquireLease(ctx context.Context, leaseKey string, owner string, ttl time.Duration) (bool, error) {
	ok, err := s.client.SetNX(ctx, s.leaseKey(leaseKey), owner, ttl).Result()
	return ok, err
}

var renewScript = goredis.NewScript(`
if redis.call("GET", KEYS[1]) == ARGV[1] then
	return redis.call("PEXPIRE", KEYS[1], ARGV[2])
else
	return 0
end`)

func (s *Store) RenewLease(ctx context.Context, leaseKey string, owner string, ttl time.Duration) (bool, error) {
	res, err := renewScript.Run(ctx, s.client, []string{s.leaseKey(leaseKey)}, owner, int(ttl.Milliseconds())).Result()
	if err != nil && !errors.Is(err, goredis.Nil) {
		return false, err
	}
	switch v := res.(type) {
	case int64:
		return v > 0, nil
	case nil:
		return false, nil
	default:
		return false, fmt.Errorf("unexpected renew result: %v", res)
	}
}

var releaseScript = goredis.NewScript(`
if redis.call("GET", KEYS[1]) == ARGV[1] then
	return redis.call("DEL", KEYS[1])
else
	return 0
end`)

func (s *Store) ReleaseLease(ctx context.Context, leaseKey string, owner string) (bool, error) {
	res, err := releaseScript.Run(ctx, s.client, []string{s.leaseKey(leaseKey)}, owner).Result()
	if err != nil && !errors.Is(err, goredis.Nil) {
		return false, err
	}
	switch v := res.(type) {
	case int64:
		return v > 0, nil
	case nil:
		return false, nil
	default:
		return false, fmt.Errorf("unexpected release result: %v", res)
	}
}

func (s *Store) GetLeaseOwner(ctx context.Context, leaseKey string) (string, bool, error) {
	val, err := s.client.Get(ctx, s.leaseKey(leaseKey)).Result()
	if err == goredis.Nil {
		return "", false, nil
	}
	if err != nil {
		return "", false, err
	}
	return val, true, nil
}

func (s *Store) GetConfig(ctx context.Context, key string) ([]byte, bool, error) {
	val, err := s.client.Get(ctx, s.configKey(key)).Bytes()
	if err == goredis.Nil {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	return val, true, nil
}

func (s *Store) PutConfig(ctx context.Context, key string, value []byte, ttl time.Duration) error {
	return s.client.Set(ctx, s.configKey(key), value, ttl).Err()
}

func (s *Store) WatchConfig(ctx context.Context, keyPrefix string) (<-chan coord.ConfigEvent, error) {
	return nil, ErrWatchUnsupported
}

func (s *Store) nodeKey(nodeID string) string {
	return s.prefix + "node:" + nodeID
}

func (s *Store) nodesSetKey() string {
	return s.prefix + "nodes:all"
}

func (s *Store) leaseKey(leaseKey string) string {
	return s.prefix + leaseKey
}

func (s *Store) configKey(key string) string {
	return s.prefix + key
}
