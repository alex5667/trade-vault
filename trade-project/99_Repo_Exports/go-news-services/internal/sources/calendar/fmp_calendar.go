package calendar

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"go-news-services/internal/ingestor"
)

// FMPCalendarSource грузит события экономкалендаря из FMP.
// Docs: /stable/economic-calendar?from=YYYY-MM-DD&to=YYYY-MM-DD&apikey=... :contentReference[oaicite:1]{index=1}
//
// Важно:
// - делайте небольшой window (например, today..today+7d), чтобы не жечь лимиты
// - UID делаем стабильным: provider + title + country + currency + event_ts_bucket + provider_id(if exists)
type FMPCalendarSource struct {
	name    string
	apiKey  string
	baseURL string
	hc      *http.Client

	// окно выгрузки
	fromOffsetDays int
	toOffsetDays   int

	// bucket для UID (обычно 24h, т.к. события дневные)
	uidBucket time.Duration
}

type FMPCalendarConfig struct {
	Name           string
	APIKey         string
	BaseURL        string        // default https://financialmodelingprep.com
	HTTPTimeout    time.Duration
	FromOffsetDays int           // default 0
	ToOffsetDays   int           // default 7
	UIDBucket      time.Duration // default 24h
}

func NewFMPCalendarSource(cfg FMPCalendarConfig) *FMPCalendarSource {
	base := strings.TrimSpace(cfg.BaseURL)
	if base == "" {
		base = "https://financialmodelingprep.com"
	}
	to := cfg.HTTPTimeout
	if to <= 0 {
		to = 10 * time.Second
	}
	fo := cfg.FromOffsetDays
	toD := cfg.ToOffsetDays
	if toD <= fo {
		toD = fo + 7
	}
	b := cfg.UIDBucket
	if b <= 0 {
		b = 24 * time.Hour
	}

	return &FMPCalendarSource{
		name:           strings.TrimSpace(cfg.Name),
		apiKey:         strings.TrimSpace(cfg.APIKey),
		baseURL:        base,
		hc:             &http.Client{Timeout: to},
		fromOffsetDays: fo,
		toOffsetDays:   toD,
		uidBucket:      b,
	}
}

// FMP row (реальная структура зависит от ответа; делаем максимально tolerant parsing)
type fmpEvent struct {
	Date       string `json:"date"`       // часто "2025-01-31 13:30:00" или ISO
	Country    string `json:"country"`    // "US"
	Currency   string `json:"currency"`   // "USD"
	Event      string `json:"event"`      // "Nonfarm Payrolls"
	Impact     string `json:"impact"`     // "High"/"Medium"/"Low" (может быть)
	Importance any    `json:"importance"` // иногда int
	Forecast   any    `json:"forecast"`
	Previous   any    `json:"previous"`
	Unit       string `json:"unit"`
	// иногда бывает id
	ID any `json:"id"`
}

func (s *FMPCalendarSource) Fetch(ctx context.Context) ([]ingestor.CalendarEvent, error) {
	if s.apiKey == "" {
		// fail-open: источник "выключен"
		return nil, nil
	}

	now := time.Now().UTC()
	from := now.AddDate(0, 0, s.fromOffsetDays).Format("2006-01-02")
	to := now.AddDate(0, 0, s.toOffsetDays).Format("2006-01-02")

	u, _ := url.Parse(s.baseURL)
	u.Path = "/stable/economic-calendar"
	q := u.Query()
	q.Set("from", from)
	q.Set("to", to)
	q.Set("apikey", s.apiKey)
	u.RawQuery = q.Encode()

	req, _ := http.NewRequestWithContext(ctx, u.String(), nil)
	resp, err := s.hc.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	
	// Handle rate limit (429) gracefully - return empty result, pipeline will retry on next poll
	if resp.StatusCode == 429 {
		return nil, nil
	}
	
	// Handle auth errors gracefully - fail-open
	if resp.StatusCode == 401 || resp.StatusCode == 403 {
		return nil, nil
	}
	
	if resp.StatusCode/100 != 2 {
		b, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return nil, fmt.Errorf("fmp calendar http %d: %s", resp.StatusCode, string(b))
	}

	raw, err := io.ReadAll(io.LimitReader(resp.Body, 8<<20))
	if err != nil {
		return nil, err
	}

	var rows []fmpEvent
	if err := json.Unmarshal(raw, &rows); err != nil {
		return nil, err
	}

	nowMs := time.Now().UnixMilli()
	out := make([]ingestor.CalendarEvent, 0, len(rows))

	for _, r := range rows {
		eventTitle := strings.TrimSpace(r.Event)
		if eventTitle == "" {
			// иногда поле может называться иначе — оставим из map
			// здесь оставим как есть, так как парсим из структуры
			continue
		}
		country := strings.TrimSpace(r.Country)
		currency := strings.TrimSpace(r.Currency)

		// parse time (best-effort)
		eventMs := parseFMPEventTimeMs(r.Date)
		if eventMs <= 0 {
			eventMs = nowMs
		}

		imp := normalizeImportance(r.Impact, r.Importance)
		forecast := anyToString(r.Forecast)
		previous := anyToString(r.Previous)
		unit := strings.TrimSpace(r.Unit)

		// UID: provider + country + currency + title + date_ms (stability)
		providerID := fmt.Sprintf("%d", eventMs)
		uid := ingestor.StableUID("fmp-econ", country, currency, eventTitle, providerID)

		// Marshal raw event to JSON for PayloadJSON
		payloadBytes, _ := json.Marshal(r)
		payloadJSON := string(payloadBytes)

		out = append(out, ingestor.CalendarEvent{
			UID:          uid,
			EventTSms:    eventMs,
			IngestedTSms: nowMs,
			Country:      country,
			Currency:     currency,
			Title:        eventTitle,
			Importance:   imp,
			Forecast:     forecast,
			Previous:     previous,
			Unit:         unit,
			Source:       "fmp",
			PayloadJSON:  payloadJSON,
		})
	}

	return out, nil
}

// ----- helpers -----

func anyToString(v any) string {
	if v == nil {
		return ""
	}
	switch t := v.(type) {
	case string:
		return strings.TrimSpace(t)
	case float64:
		return strings.TrimSpace(fmt.Sprintf("%g", t))
	case int:
		return fmt.Sprintf("%d", t)
	default:
		b, _ := json.Marshal(v)
		return string(b)
	}
}

// Пример форматов:
// "2025-01-31 13:30:00" (часто)
// "2025-01-31T13:30:00Z" (иногда)
func parseFMPEventTimeMs(s string) int64 {
	s = strings.TrimSpace(s)
	if s == "" {
		return 0
	}
	layouts := []string{
		"2006-01-02 15:04:05",
		time.RFC3339,
		"2006-01-02T15:04:05",
		"2006-01-02",
	}
	for _, l := range layouts {
		if t, err := time.Parse(l, s); err == nil {
			return t.UTC().UnixMilli()
		}
	}
	return 0
}

// Ваша шкала importance:int (0..?) — подгоняем:
func normalizeImportance(impact string, importance any) int {
	impact = strings.ToLower(strings.TrimSpace(impact))
	switch impact {
	case "high":
		return 3
	case "medium":
		return 2
	case "low":
		return 1
	}
	// fallback на поле importance
	switch v := importance.(type) {
	case float64:
		if v >= 3 {
			return 3
		}
		if v == 2 {
			return 2
		}
		if v == 1 {
			return 1
		}
	case int:
		if v >= 3 {
			return 3
		}
		if v == 2 {
			return 2
		}
		if v == 1 {
			return 1
		}
	}
	return 0
}
