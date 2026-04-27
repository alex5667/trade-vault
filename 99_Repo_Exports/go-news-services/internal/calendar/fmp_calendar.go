package calendar

import (
	"context"
	"crypto/sha1"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"trade-news-ingestor/internal/ingestor"
)

type FMPCalendarConfig struct {
	Name         string
	BaseURL      string        // default: https://financialmodelingprep.com
	APIKey       string        // from env FMP_API_KEY
	HTTPTimeout  time.Duration // e.g. 8s
	UserAgent    string
	LookaheadDays int          // e.g. 10
	BackDays      int          // e.g. 1 (на случай таймзон/опозданий)
	Countries    []string      // e.g. ["US","EU"] или ["United States", ...] (как у FMP)
	Importance   []string      // e.g. ["High","Medium"]
	Enabled      bool
}

// FMPCalendarSource implements ingestor.CalendarSource
type FMPCalendarSource struct {
	cfg    FMPCalendarConfig
	client *http.Client

	// заранее нормализованные фильтры (ускорение, меньше аллокаций)
	countrySet    map[string]struct{}
	importanceSet map[string]struct{}
}

func NewFMPCalendarSource(cfg FMPCalendarConfig) *FMPCalendarSource {
	if cfg.BaseURL == "" {
		cfg.BaseURL = "https://financialmodelingprep.com"
	}
	if cfg.HTTPTimeout <= 0 {
		cfg.HTTPTimeout = 8 * time.Second
	}
	if cfg.UserAgent == "" {
		cfg.UserAgent = "trade-news-ingestor/1.0"
	}
	cs := map[string]struct{}{}
	for _, c := range cfg.Countries {
		cs[strings.ToUpper(strings.TrimSpace(c))] = struct{}{}
	}
	is := map[string]struct{}{}
	for _, x := range cfg.Importance {
		is[strings.ToUpper(strings.TrimSpace(x))] = struct{}{}
	}
	return &FMPCalendarSource{
		cfg:    cfg,
		client: &http.Client{Timeout: cfg.HTTPTimeout},
		countrySet:    cs,
		importanceSet: is,
	}
}

// fmpEvent — минимальный набор полей ответа FMP.
// (FMP может возвращать и другие поля: actual/previous/estimate и т.п.)
type fmpEvent struct {
	Date       string `json:"date"`       // часто "YYYY-MM-DD HH:MM:SS" либо "YYYY-MM-DD"
	Event      string `json:"event"`
	Country    string `json:"country"`
	Impact     string `json:"impact"`     // часто impact=High/Medium/Low
	Importance string `json:"importance"` // иногда так
	Currency   string `json:"currency"`
	Forecast   string `json:"forecast"`
	Previous   string `json:"previous"`
}

func (s *FMPCalendarSource) Fetch(ctx context.Context) ([]ingestor.CalendarEvent, error) {
	// fail-open: если выключено/нет ключа — не ломаем пайплайн
	if !s.cfg.Enabled || strings.TrimSpace(s.cfg.APIKey) == "" {
		return nil, nil
	}

	now := time.Now().UTC()
	from := now.AddDate(0, 0, -maxInt(0, s.cfg.BackDays)).Format("2006-01-02")
	to := now.AddDate(0, 0, maxInt(1, s.cfg.LookaheadDays)).Format("2006-01-02")

	// endpoint: /stable/economic-calendar :contentReference[oaicite:1]{index=1}
	endpoint := strings.TrimRight(s.cfg.BaseURL, "/") + "/stable/economic-calendar"
	q := url.Values{}
	q.Set("from", from)
	q.Set("to", to)
	q.Set("apikey", s.cfg.APIKey)

	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, endpoint+"?"+q.Encode(), nil)
	if s.cfg.UserAgent != "" {
		req.Header.Set("User-Agent", s.cfg.UserAgent)
	}

	resp, err := s.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode == 401 || resp.StatusCode == 403 {
		// ключ неверный/нет доступа — лучше "тихо отключиться", чем спамить ошибками
		return nil, nil
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		b, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return nil, fmt.Errorf("fmp calendar http %d: %s", resp.StatusCode, string(b))
	}

	body, err := io.ReadAll(io.LimitReader(resp.Body, 2<<20))
	if err != nil {
		return nil, err
	}

	var items []fmpEvent
	if err := json.Unmarshal(body, &items); err != nil {
		return nil, err
	}

	out := make([]ingestor.CalendarEvent, 0, len(items))
	for _, it := range items {
		if !s.passCountry(it.Country) {
			continue
		}
		imp := pickImportance(it.Impact, it.Importance)
		if !s.passImportance(imp) {
			continue
		}
		ev, ok := toCalendarEvent(s.cfg.Name, it, imp)
		if !ok {
			continue
		}
		out = append(out, ev)
	}
	return out, nil
}

func (s *FMPCalendarSource) passCountry(country string) bool {
	if len(s.countrySet) == 0 {
		return true
	}
	_, ok := s.countrySet[strings.ToUpper(strings.TrimSpace(country))]
	return ok
}

func (s *FMPCalendarSource) passImportance(imp string) bool {
	if len(s.importanceSet) == 0 {
		return true
	}
	_, ok := s.importanceSet[strings.ToUpper(strings.TrimSpace(imp))]
	return ok
}

func pickImportance(a, b string) string {
	x := strings.TrimSpace(a)
	if x != "" {
		return x
	}
	return strings.TrimSpace(b)
}

// toCalendarEvent — ЕДИНСТВЕННОЕ место, где нужно подогнать поля под ваш CalendarEvent.
func toCalendarEvent(source string, it fmpEvent, imp string) (ingestor.CalendarEvent, bool) {
	ts, err := parseFMPDateUTC(it.Date)
	if err != nil {
		return ingestor.CalendarEvent{}, false
	}

	uid := hashUID(source, it.Event, it.Country, it.Currency, it.Date)

	// Подгоните под ваши поля CalendarEvent:
	return ingestor.CalendarEvent{
		UID:          uid,
		EventTSms:    ts.UnixMilli(),
		IngestedTSms: time.Now().UnixMilli(),
		Country:      it.Country,
		Currency:     it.Currency,
		Title:        it.Event,
		Importance:   importanceToInt(imp),
		Forecast:     it.Forecast,
		Previous:     it.Previous,
		Unit:         "",
		Source:       source,
		PayloadJSON:  "",
	}, true
}

func parseFMPDateUTC(s string) (time.Time, error) {
	x := strings.TrimSpace(s)
	if x == "" {
		return time.Time{}, errors.New("empty date")
	}
	// часто приходит "YYYY-MM-DD HH:MM:SS"
	if t, err := time.Parse("2006-01-02 15:04:05", x); err == nil {
		return t.UTC(), nil
	}
	// иногда только дата
	if t, err := time.Parse("2006-01-02", x); err == nil {
		return t.UTC(), nil
	}
	return time.Time{}, fmt.Errorf("bad date: %s", x)
}

func hashUID(parts ...string) string {
	h := sha1.New()
	for _, p := range parts {
		_, _ = h.Write([]byte(p))
		_, _ = h.Write([]byte{0})
	}
	return hex.EncodeToString(h.Sum(nil))
}

func maxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func importanceToInt(s string) int {
	switch strings.ToLower(s) {
	case "high":
		return 3
	case "medium":
		return 2
	case "low":
		return 1
	default:
		return 0
	}
}
