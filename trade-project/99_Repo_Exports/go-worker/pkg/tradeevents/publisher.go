package tradeevents

import (
	"context"
	"encoding/json"
	"strings"
	"time"

	"go-worker/internal/streams"

	"github.com/redis/go-redis/v9"
)

type MetaEnforceInfo struct {
	CovBucket string `json:"meta_enforce_cov_bucket"`
	Applied   bool   `json:"meta_enforce_applied"`
}

type PositionClosedEvent struct {
	EventType string  `json:"event_type"`
	TsMs      int64   `json:"ts_ms"`
	Symbol    string  `json:"symbol"`
	SID       string  `json:"sid"`
	RMult     float64 `json:"r_mult"`

	MetaEnforceCovBucket string `json:"meta_enforce_cov_bucket"`
	MetaEnforceApplied   bool   `json:"meta_enforce_applied"`

	// aliases (optional rollout):
	MetaCovBucket string `json:"meta_cov_bucket,omitempty"`
	MetaApplied   bool   `json:"meta_applied,omitempty"`
}

type Publisher struct {
	rdb    *redis.Client
	stream string
	maxlen int64
}

func NewPublisher(rdb *redis.Client, stream string, maxlen int64) *Publisher {
	if stream == "" {
		stream = streams.EventsTrades
	}
	if maxlen <= 0 {
		maxlen = streams.MaxLenGlobal
	}
	return &Publisher{rdb: rdb, stream: stream, maxlen: maxlen}
}

func nowMs() int64 { return time.Now().UTC().UnixMilli() }

func sanitizeBucket(s string) string {
	s = strings.TrimSpace(s)
	if s == "" {
		return "unknown"
	}
	if len(s) > 64 {
		return s[:64]
	}
	return s
}

func (p *Publisher) PublishPositionClosed(ctx context.Context, symbol, sid string, rMult float64, meta *MetaEnforceInfo) error {
	m := MetaEnforceInfo{CovBucket: "unknown", Applied: false}
	if meta != nil {
		m.CovBucket = sanitizeBucket(meta.CovBucket)
		m.Applied = meta.Applied
	}
	ev := PositionClosedEvent{
		EventType:            "POSITION_CLOSED",
		TsMs:                 nowMs(),
		Symbol:               strings.ToUpper(symbol),
		SID:                  sid,
		RMult:                rMult,
		MetaEnforceCovBucket: sanitizeBucket(m.CovBucket),
		MetaEnforceApplied:   m.Applied,
		MetaCovBucket:        sanitizeBucket(m.CovBucket),
		MetaApplied:          m.Applied,
	}

	b, err := json.Marshal(ev)
	if err != nil {
		return err
	}

	// keep single field 'payload' for compatibility with existing consumers.
	return p.rdb.XAdd(ctx, &redis.XAddArgs{
		Stream: p.stream,
		MaxLen: p.maxlen,
		Approx: true,
		Values: map[string]interface{}{
			"payload": string(b),
		},
	}).Err()
}
