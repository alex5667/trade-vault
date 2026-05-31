package liquidation

import (
	"strings"
	"sync"
	"time"
)

// DQPolicy — правила качества данных для liquidation events.
//
// Цели:
//   - валидировать минимальный набор полей (symbol, price, qty)
//   - отбрасывать bad time: stale/future-skew (detect → quarantine)
//   - обеспечивать монотонность времени по символу (в пределах MaxOutOfOrder)
//   - фильтровать по allowlist
//   - (опционально) дедуплицировать повторяющиеся события в коротком окне
//
// Принцип: detect → quarantine (сэмплируемо) → метрики.
//
// Важно:
//   - В MVP мы НЕ делаем "clamp" времени (это потенциально скрывает проблему источника).
//     Для bad time события уходят в quarantine.
//   - Дедуп — это "noise reduction". Его НЕ нужно отправлять в quarantine.
//     (см. controller.go: reason==dedup фильтруется без quarantine).
//
// NOTE: DQPolicy должен быть быстрым (O(1)), потому что вызывается на каждый event.
type DQPolicy struct {
	MaxEventAge   time.Duration
	MaxFutureSkew time.Duration
	MaxOutOfOrder time.Duration

	AllowSymbols map[string]struct{} // uppercase

	EnableQuarantine bool

	// Dedup* — опциональная дедупликация одинаковых событий.
	// Полезно для Binance !forceOrder@arr, где в одном окне может прилететь одно и то же.
	DedupEnabled bool
	DedupTTL     time.Duration
	DedupMaxKeys int

	mu       sync.Mutex
	lastTsMs map[string]int64
	// dedupCache уже содержит собственную блокировку, поэтому держим его отдельно.
	dedup *dedupCache
}

func DefaultDQPolicy(allowSymbols []string) DQPolicy {
	m := map[string]struct{}{}
	for _, s := range allowSymbols {
		s = strings.ToUpper(strings.TrimSpace(s))
		if s != "" {
			m[s] = struct{}{}
		}
	}
	return DQPolicy{
		MaxEventAge:      10 * time.Second,
		MaxFutureSkew:    2 * time.Second,
		MaxOutOfOrder:    2 * time.Second,
		AllowSymbols:     m,
		EnableQuarantine: true,
		lastTsMs:         map[string]int64{},
	}
}

// Validate возвращает (ok, reason).
func (p *DQPolicy) Validate(ev NormalizedEvent, nowMs int64) (bool, string) {
	if ev.Symbol == "" {
		return false, "missing_symbol"
	}
	if len(p.AllowSymbols) > 0 {
		if _, ok := p.AllowSymbols[ev.Symbol]; !ok {
			return false, "filtered_symbol"
		}
	}
	if ev.EventTsMs <= 0 {
		return false, "bad_ts"
	}

	// Быстрый sanity-check "похоже ли на epoch ms".
	// 1e11 ms ~= 1973-03-03; epoch seconds обычно ~ 1e9..1e10.
	// Мы не делаем преобразование (sanitize) — лучше quarantine, чтобы downstream не обучался на мусоре.
	if ev.EventTsMs < 100_000_000_000 {
		return false, "bad_ts_unit"
	}

	if strings.TrimSpace(ev.Price) == "" {
		return false, "missing_price"
	}
	if strings.TrimSpace(ev.Qty) == "" {
		return false, "missing_qty"
	}

	// --- bad time ---
	if p.MaxEventAge > 0 {
		if nowMs-ev.EventTsMs > p.MaxEventAge.Milliseconds() {
			return false, "stale"
		}
	}
	if p.MaxFutureSkew > 0 {
		if ev.EventTsMs-nowMs > p.MaxFutureSkew.Milliseconds() {
			return false, "future_skew"
		}
	}

	// --- dedup (noise reduction) ---
	// Выполняем после bad time, чтобы не тратить кеш на заведомо плохие события.
	if p.DedupEnabled {
		if p.dedup == nil {
			// ленивое создание (на случай если p был скопирован/переинициализирован)
			p.dedup = newDedupCache(p.DedupTTL, p.DedupMaxKeys)
		}
		key := p.eventDedupKey(ev)
		if p.dedup.SeenOrAdd(key, nowMs) {
			return false, "dedup"
		}
	}

	// --- out-of-order ---
	if p.MaxOutOfOrder > 0 {
		p.mu.Lock()
		last, ok := p.lastTsMs[ev.Symbol]
		if !ok || ev.EventTsMs >= last {
			p.lastTsMs[ev.Symbol] = ev.EventTsMs
			p.mu.Unlock()
			return true, ""
		}
		// allow small OOO window
		if last-ev.EventTsMs <= p.MaxOutOfOrder.Milliseconds() {
			p.mu.Unlock()
			return true, ""
		}
		p.mu.Unlock()
		return false, "out_of_order"
	}

	return true, ""
}

func (p *DQPolicy) eventDedupKey(ev NormalizedEvent) string {
	// Ключ — максимально "сильный", чтобы избежать коллизий.
	// Включаем venue, потому что одинаковые символ/цена/qty/ts могут совпасть между биржами.
	// Формат: venue|symbol|ts|side|price|qty
	//
	// Не используем notional_usd, потому что он derived и зависит от округления.
	// price/qty берём как строки (биржевой формат).
	return ev.Source + "|" + ev.Symbol + "|" + itoa64(ev.EventTsMs) + "|" + ev.RawSide + "|" + ev.Price + "|" + ev.Qty
}

// itoa64 — локальный быстрый конвертер int64->string без fmt.
func itoa64(v int64) string {
	// worst-case: -9223372036854775808
	if v == 0 {
		return "0"
	}

	neg := v < 0
	if neg {
		v = -v
	}

	var buf [32]byte
	i := len(buf)
	for v > 0 {
		i--
		buf[i] = byte('0' + v%10)
		v /= 10
	}
	if neg {
		i--
		buf[i] = '-'
	}
	return string(buf[i:])
}
