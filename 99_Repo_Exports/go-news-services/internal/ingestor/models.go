package ingestor

import (
	"crypto/sha256"
	"encoding/hex"
	"time"
)

// Контракт строго под ваш Python models.py (news_pipeline/models.py)

// stable_uid как в Python: sha256(parts joined with 0x1f)[:24]
func StableUID(parts ...string) string {
	h := sha256.New()
	sep := []byte{0x1f}
	for _, p := range parts {
		h.Write([]byte(p))
		h.Write(sep)
	}
	sum := h.Sum(nil)
	return hex.EncodeToString(sum)[:24]
}

type NewsRawItem struct {
	UID            string
	PublishedTSms  int64
	IngestedTSms   int64
	Source         string
	Title          string
	URL            string
	Summary        string
	SymbolsJSON    string // строка JSON массива
	Importance     float64
	PayloadJSON    string // строка JSON объекта
}

// ToStreamFields: только stringable поля
func (n NewsRawItem) ToStreamFields() map[string]any {
	return map[string]any{
		"uid":             n.UID,
		"published_ts_ms": itoa64(n.PublishedTSms),
		"ingested_ts_ms":  itoa64(n.IngestedTSms),
		"source":          n.Source,
		"title":           n.Title,
		"url":             n.URL,
		"summary":         n.Summary,
		"symbols":         n.SymbolsJSON,
		"importance":      ftoa(n.Importance),
		"payload":         n.PayloadJSON,
	}
}

type CalendarEvent struct {
	UID          string
	EventTSms    int64
	IngestedTSms int64
	Country      string
	Currency     string
	Title        string
	Importance   int
	Forecast     string // пусто или число
	Previous     string // пусто или число
	Unit         string
	Source       string
	PayloadJSON  string
}

func (c CalendarEvent) ToStreamFields() map[string]any {
	return map[string]any{
		"uid":            c.UID,
		"event_ts_ms":    itoa64(c.EventTSms),
		"ingested_ts_ms": itoa64(c.IngestedTSms),
		"country":        c.Country,
		"currency":       c.Currency,
		"title":          c.Title,
		"importance":     itoa(int64(c.Importance)),
		"forecast":       c.Forecast,
		"previous":       c.Previous,
		"unit":           c.Unit,
		"source":         c.Source,
		"payload":        c.PayloadJSON,
	}
}

func NowMs() int64 { return time.Now().UnixMilli() }
