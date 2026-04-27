package redisx

import (
	"context"
	"net/url"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

func NewClient(redisURL string) *redis.Client {
	// Поддержка redis://user:pass@host:port/db
	u, err := url.Parse(redisURL)
	if err != nil || u.Scheme == "" {
		// fallback: host:port
		return redis.NewClient(&redis.Options{
			Addr: redisURL,
			DB:   0,
		})
	}
	addr := u.Host
	if !strings.Contains(addr, ":") {
		addr += ":6379"
	}
	db := 0
	if u.Path != "" && u.Path != "/" {
		// очень мягкий парсинг /0
		p := strings.TrimPrefix(u.Path, "/")
		n := 0
		for _, c := range p {
			if c < '0' || c > '9' {
				n = 0
				break
			}
			n = n*10 + int(c-'0')
		}
		db = n
	}
	// Extract credentials from URL (previously missing — caused default-user NOPERM errors)
	username := ""
	password := ""
	if u.User != nil {
		username = u.User.Username()
		password, _ = u.User.Password()
	}
	return redis.NewClient(&redis.Options{
		Addr:         addr,
		Username:     username,
		Password:     password,
		DB:           db,
		DialTimeout:  2 * time.Second,
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 10 * time.Second,
		PoolSize:     64,
		MinIdleConns: 8,
	})
}

func Ping(ctx context.Context, rdb *redis.Client) error {
	return rdb.Ping(ctx).Err()
}
