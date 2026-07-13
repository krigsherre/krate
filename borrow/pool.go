package borrow

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

func PoolKey(key string) string         { return fmt.Sprintf("krate:%s:pool", key) }
func BorrowedKey(key, id string) string { return fmt.Sprintf("krate:%s:borrowed:%s", key, id) }
func WindowStartKey(key string) string  { return fmt.Sprintf("krate:%s:window_start", key) }
func ConfigKey(key string) string       { return fmt.Sprintf("krate:%s:config", key) }
func ClusterKey(id string) string       { return fmt.Sprintf("krate:cluster:%s", id) }

type PoolState struct {
	Remaining   uint64
	WindowStart int64
	Limit       uint64
	WindowMs    int64
}

const borrowLua = `
local pool_key = KEYS[1]
local borrowed_key = KEYS[2]
local requested = tonumber(ARGV[1])
local lease_ttl = tonumber(ARGV[2])
local now = tonumber(ARGV[3])

local available = tonumber(redis.call('GET', pool_key) or '0')

if available <= 0 then
    return 0
end

local granted = math.min(requested, available)

redis.call('DECRBY', pool_key, granted)
redis.call('INCRBY', borrowed_key, granted)
redis.call('PEXPIRE', borrowed_key, lease_ttl)

return granted
`

const returnLua = `
local pool_key = KEYS[1]
local borrowed_key = KEYS[2]
local tokens = tonumber(ARGV[1])

local borrowed = tonumber(redis.call('GET', borrowed_key) or '0')
local returning = math.min(tokens, borrowed)

if returning > 0 then
    redis.call('INCRBY', pool_key, returning)
    redis.call('DECRBY', borrowed_key, returning)
end

return returning
`

const windowResetLua = `
redis.call('SET', KEYS[1], tonumber(ARGV[1]))
redis.call('SET', KEYS[2], tonumber(ARGV[2]))
return 1
`

type RedisPool struct {
	client  redis.UniversalClient
	scripts map[string]*redis.Script
}

func NewRedisPool(client redis.UniversalClient) *RedisPool {
	return &RedisPool{
		client: client,
		scripts: map[string]*redis.Script{
			"borrow": redis.NewScript(borrowLua),
			"return": redis.NewScript(returnLua),
			"reset":  redis.NewScript(windowResetLua),
		},
	}
}

func (rp *RedisPool) runScript(ctx context.Context, name string, keys []string, args ...interface{}) (interface{}, error) {
	script := rp.scripts[name]

	result, err := script.EvalSha(ctx, rp.client, keys, args...).Result()
	if err != nil && isNoScriptErr(err) {
		result, err = script.Eval(ctx, rp.client, keys, args...).Result()
	}
	return result, err
}

func isNoScriptErr(err error) bool {
	return err != nil && strings.Contains(err.Error(), "NOSCRIPT")
}

func (rp *RedisPool) Borrow(ctx context.Context, key, instanceID string, requested uint64, leaseTTLms int64) (uint64, error) {
	keys := []string{PoolKey(key), BorrowedKey(key, instanceID)}
	now := time.Now().UnixMilli()

	result, err := rp.runScript(ctx, "borrow", keys, int64(requested), leaseTTLms, now)
	if err != nil {
		return 0, fmt.Errorf("borrow script: %w", err)
	}

	return toUint64(result)
}

func (rp *RedisPool) Return(ctx context.Context, key, instanceID string, tokens uint64) error {
	keys := []string{PoolKey(key), BorrowedKey(key, instanceID)}
	_, err := rp.runScript(ctx, "return", keys, int64(tokens))
	if err != nil {
		return fmt.Errorf("return script: %w", err)
	}
	return nil
}

func (rp *RedisPool) ResetWindow(ctx context.Context, key string, newLimit uint64, windowStartMs int64) error {
	keys := []string{PoolKey(key), WindowStartKey(key), ConfigKey(key)}
	_, err := rp.runScript(ctx, "reset", keys, int64(newLimit), windowStartMs)
	return err
}

func (rp *RedisPool) DeleteBorrowedKeys(ctx context.Context, keyPattern string) error {
	return nil
}

func (rp *RedisPool) GetState(ctx context.Context, key string) (*PoolState, error) {
	pipe := rp.client.Pipeline()
	poolCmd := pipe.Get(ctx, PoolKey(key))
	windowCmd := pipe.Get(ctx, WindowStartKey(key))
	configCmd := pipe.HGetAll(ctx, ConfigKey(key))
	pipe.Exec(ctx)

	state := &PoolState{}

	if val, err := poolCmd.Result(); err == nil {
		state.Remaining, _ = strconv.ParseUint(val, 10, 64)
	}

	if val, err := windowCmd.Result(); err == nil {
		state.WindowStart, _ = strconv.ParseInt(val, 10, 64)
	}

	if config, err := configCmd.Result(); err == nil {
		if v, ok := config["limit"]; ok {
			state.Limit, _ = strconv.ParseUint(v, 10, 64)
		}
		if v, ok := config["window_ms"]; ok {
			state.WindowMs, _ = strconv.ParseInt(v, 10, 64)
		}
	}

	return state, nil
}

func (rp *RedisPool) EnsureConfig(ctx context.Context, key string, limit uint64, windowMs int64) error {
	return rp.client.HSet(ctx, ConfigKey(key), map[string]interface{}{
		"limit":     strconv.FormatUint(limit, 10),
		"window_ms": strconv.FormatInt(windowMs, 10),
	}).Err()
}

func toUint64(v interface{}) (uint64, error) {
	switch val := v.(type) {
	case int64:
		return uint64(val), nil
	case string:
		return strconv.ParseUint(val, 10, 64)
	default:
		return 0, fmt.Errorf("unexpected redis result type %T", v)
	}
}
