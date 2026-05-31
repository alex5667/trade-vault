// Package wsconn предоставляет переиспользуемые примитивы для WebSocket-соединений к Binance:
// dial с настраиваемыми таймаутами, единый ping/pong handler, exponential backoff.
//
// Цель: устранить дублирование между FuturesMultiplexManager и FuturesDepthStream.
package wsconn

import (
	"context"
	"math/rand"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

const (
	// DefaultReconnectBackoff — начальная задержка перед переподключением.
	DefaultReconnectBackoff = 2 * time.Second
	// MaxReconnectBackoff — максимальная задержка (exponential cap).
	MaxReconnectBackoff = 30 * time.Second
)

// Config описывает параметры WS-соединения. Все тайм-ауты переопределяются через ENV
// в точке вызова (getEnvDuration); здесь хранятся уже разрешённые значения.
type Config struct {
	HandshakeTimeout time.Duration
	DialTimeout      time.Duration
	TCPKeepAlive     time.Duration
	ReadTimeout      time.Duration
	PingPeriod       time.Duration
	WriteWait        time.Duration
	ReadBufferSize   int
	WriteBufferSize  int
	MsgChanCapacity  int
}

// DefaultConfig возвращает консервативные но достаточные дефолты.
// Переопределяйте поля при необходимости.
func DefaultConfig() Config {
	return Config{
		HandshakeTimeout: 45 * time.Second,
		DialTimeout:      30 * time.Second,
		TCPKeepAlive:     0, // Disable automatic OS keep-alive, rely on WS Ping
		ReadTimeout:      300 * time.Second,
		PingPeriod:       20 * time.Second, // единый дефолт (был 20s vs 27s в двух местах)
		WriteWait:        5 * time.Second,
		ReadBufferSize:   64 * 1024, // 64 KB
		WriteBufferSize:  64 * 1024,
		MsgChanCapacity:  5000,
	}
}

// Dial устанавливает WS-соединение по url с использованием cfg.
// Возвращает *websocket.Conn готовое к использованию с настроенными ping/pong handlers.
//
// pingMu — опциональный мьютекс, защищающий WriteControl; передайте nil если
// пользователь сам управляет write mutex.
func Dial(ctx context.Context, url string, cfg Config) (*websocket.Conn, error) {
	dialer := websocket.Dialer{
		Proxy:             http.ProxyFromEnvironment,
		HandshakeTimeout:  cfg.HandshakeTimeout,
		ReadBufferSize:    cfg.ReadBufferSize,
		WriteBufferSize:   cfg.WriteBufferSize,
		EnableCompression: false, // stability over throughput
		NetDial: func(network, addr string) (net.Conn, error) {
			nd := &net.Dialer{
				Timeout:   cfg.DialTimeout,
				KeepAlive: cfg.TCPKeepAlive,
			}
			// Принудительно IPv4 — избегаем проблем с dual-stack DNS на Ubuntu.
			return nd.Dial("tcp4", addr)
		},
	}
	conn, _, err := dialer.DialContext(ctx, url, nil)
	return conn, err
}

// SetupPingPong настраивает handlers ping/pong на уже подключённом conn.
// writeMu — опциональный мьютекс для защиты конкурентной записи в сокет.
// writeFn — функция, выполняющая саму запись.
func SetupPingPong(conn *websocket.Conn, readTimeout, writeWait time.Duration, writeMu *sync.Mutex, writeFn func([]byte) error) {
	conn.SetPingHandler(func(appData string) error {
		_ = conn.SetReadDeadline(time.Now().Add(readTimeout))
		if writeMu != nil {
			writeMu.Lock()
			defer writeMu.Unlock()
		}
		return writeFn([]byte(appData))
	})
	conn.SetPongHandler(func(string) error {
		return conn.SetReadDeadline(time.Now().Add(readTimeout))
	})
}

// NextBackoff вычисляет следующую задержку по стратегии exponential backoff с cap.
func NextBackoff(current time.Duration) time.Duration {
	next := current * 2
	if next > MaxReconnectBackoff {
		next = MaxReconnectBackoff
	}
	next += time.Duration(rand.Float64() * float64(next))
	return next
}

// IsContextError возвращает true если ошибка вызвана отменой контекста.
func IsContextError(err error) bool {
	if err == nil {
		return false
	}
	s := err.Error()
	return err == context.Canceled ||
		containsAny(s, "context canceled", "context deadline exceeded")
}

// IsConnectionReset возвращает true если ошибка — сетевое закрытие соединения.
func IsConnectionReset(err error) bool {
	if err == nil {
		return false
	}
	s := err.Error()
	return containsAny(s,
		"connection reset by peer",
		"connection closed",
		"unexpected EOF",
		"1006",
		"abnormal closure",
	)
}

// IsTimeout возвращает true если ошибка — таймаут сети или i/o.
func IsTimeout(err error) bool {
	if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
		return true
	}
	if err == nil {
		return false
	}
	return containsAny(err.Error(), "i/o timeout", "timeout")
}

// containsAny проверяет наличие хотя бы одного из подстрок в s.
func containsAny(s string, subs ...string) bool {
	for _, sub := range subs {
		if len(sub) > 0 && contains(s, sub) {
			return true
		}
	}
	return false
}

func contains(s, sub string) bool {
	return len(s) >= len(sub) && searchSubstring(s, sub)
}

func searchSubstring(s, sub string) bool {
	for i := 0; i <= len(s)-len(sub); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}
