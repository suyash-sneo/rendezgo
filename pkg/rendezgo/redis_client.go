package rendezgo

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	"github.com/redis/go-redis/v9"

	"github.com/suyash-sneo/rendezgo/internal/redis_scripts"
)

// RedisClient is the minimal surface used by the controller.
type RedisClient interface {
	redis.Cmdable
	Subscribe(ctx context.Context, channels ...string) *redis.PubSub
}

type redisScript struct {
	src string
	sha string
}

func newRedisScript(s redis_scripts.Script) redisScript {
	return redisScript{src: s.Source, sha: s.SHA}
}

func (s redisScript) run(ctx context.Context, client RedisClient, keys []string, args ...interface{}) (int64, error) {
	val, err := client.EvalSha(ctx, s.sha, keys, args...).Result()
	if err != nil && isNoScript(err) {
		val, err = client.Eval(ctx, s.src, keys, args...).Result()
	}
	if err != nil {
		return 0, err
	}
	switch v := val.(type) {
	case int64:
		return v, nil
	case string:
		// Some Redis proxies return string numbers.
		i, convErr := strconv.ParseInt(v, 10, 64)
		if convErr != nil {
			return 0, convErr
		}
		return i, nil
	default:
		return 0, fmt.Errorf("unexpected script return type %T", val)
	}
}

func isNoScript(err error) bool {
	return err != nil && strings.Contains(err.Error(), "NOSCRIPT")
}
