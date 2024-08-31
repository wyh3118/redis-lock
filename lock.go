package redis_lock

import (
	"context"
	"errors"
	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	"strconv"
	"time"
)

var (
	ErrRelock               = errors.New("")
	ErrWithoutLockOwnership = errors.New("")
)

type Lock struct {
	client     *redis.Client
	key        string
	token      string
	expiration time.Duration
	sleep      time.Duration

	LuaUnlockSha string
	LuaExtendSha string
}

func NewLock(client *redis.Client, key string, expiration, sleep time.Duration) (*Lock, error) {
	lock := &Lock{
		client:     client,
		key:        key,
		expiration: expiration,
		sleep:      sleep,
	}

	if err := lock.scriptLoad(); err != nil {
		return nil, err
	}

	return lock, nil
}

func (l *Lock) scriptLoad() error {
	LuaUnlockSha, err := l.client.ScriptLoad(context.TODO(), LuaUnlockScript).Result()
	if err != nil {
		return err
	}
	l.LuaUnlockSha = LuaUnlockSha

	LuaExtendSha, err := l.client.ScriptLoad(context.TODO(), LuaExtendScript).Result()
	if err != nil {
		return err
	}
	l.LuaExtendSha = LuaExtendSha

	return nil
}

func (l *Lock) Lock(ctx context.Context) error {
	if l.token != "" {
		return ErrRelock
	}

	token, err := uuid.NewUUID()
	if err != nil {
		return err
	}

	if err := l.lock(ctx, token.String()); err != nil {
		return err
	}

	l.token = token.String()

	return nil
}

func (l *Lock) lock(ctx context.Context, token string) error {
	for {
		ok, err := l.client.SetNX(ctx, l.key, token, l.expiration).Result()
		if err != nil {
			return err
		}

		if ok {
			return nil
		}

		time.Sleep(l.sleep)
	}
}

func (l *Lock) UnLock(ctx context.Context) error {
	if l.token == "" {
		return ErrWithoutLockOwnership
	}

	if err := l.unlock(ctx); err != nil {
		return err
	}

	l.token = ""

	return nil
}

func (l *Lock) unlock(ctx context.Context) error {
	ok, err := l.client.EvalSha(ctx, l.LuaUnlockSha, []string{l.key}, l.token).Bool()
	if err != nil {
		return err
	}
	if !ok {
		l.token = ""
		return ErrWithoutLockOwnership
	}

	return nil
}

func (l *Lock) Extend(ctx context.Context, extend time.Duration) error {
	if l.token == "" {
		return ErrWithoutLockOwnership
	}

	if err := l.extend(ctx, extend); err != nil {
		return err
	}

	return nil
}

func (l *Lock) extend(ctx context.Context, extend time.Duration) error {
	extendStr := strconv.Itoa(int(extend / time.Millisecond))
	ok, err := l.client.EvalSha(ctx, l.LuaExtendSha, []string{l.key}, l.token, extendStr).Bool()
	if err != nil {
		return err
	}
	if !ok {
		return ErrWithoutLockOwnership
	}

	return nil
}
