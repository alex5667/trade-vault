package bybit

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"sort"
	"strconv"
	"strings"
	"time"

	"go-worker/internal/models"
	"go-worker/internal/monitoring"
)

// Bybit V5 public WS endpoints:
//   Linear (USDT perpetual): wss://stream.bybit.com/v5/public/linear
// Topics used here:
//   - publicTrade.{symbol}
//   - orderbook.{depth}.{symbol}
//
// Цель нормализации: привести Bybit trades/books к тому же плоскому контракту,
// который уже используется Python worker для Binance:
//   stream:tick_<SYMBOL> и stream:book_<SYMBOL>
// Поля должны быть совместимыми: symbol, ts, price, qty, side, source, market, trade_id, ...

// models.NormalizedTick — нормализованный тик сделки.
// ВАЖНО: Bybit trade id — строковый UUID. Для совместимости с существующим контрактом
// (trade_id int64) мы:
//   - если trade_id выглядит как число — парсим в int64
//   - иначе считаем FNV-1a 64-bit hash (стабильный) и используем его как int64
//
// При этом в RedisValues также кладём trade_id_raw (строка), чтобы downstream мог
// при необходимости восстановить точный идентификатор.
//
// Side: BUY/SELL (taker side, как в Bybit docs: S=Buy/Sell).
// Ts: epoch ms (T из trade row; fallback to msg.ts; fallback to now).
// Source/Market: фиксированные строки для фильтрации downstream.
//
// Доп. поле Seq сохраняем (cross sequence), если оно есть.
// См. docs: https://bybit-exchange.github.io/docs/v5/websocket/public/trade
//
// NOTE: Bybit может отправлять up to 1024 trades в одном сообщении.

// models.NormalizedDepth — нормализованный top-N orderbook snapshot.
//
// В Binance depth20@100ms — это всегда полноценный top-N snapshot.
// В Bybit orderbook.{depth} — это snapshot + delta.
// Чтобы не заставлять Python worker держать локальную книгу, мы в контроллере
// поддерживаем локальное состояние (apply delta) и публикуем *полный* top-N
// в том же контракте, что и Binance.
//
// FirstID/FinalID/PrevFinal:
//
//	Bybit отдаёт u (update id) и seq (cross seq). Диапазона U..u как в Binance нет.
//	Поэтому:
//	  FinalID = u
//	  FirstID = u
//	  PrevFinal = предыдущий u (0 если неизвестно)
//
// Ts: используем cts (matching engine ts) если есть, иначе msg.ts.

var (
	errEmptyPayload = errors.New("empty bybit ws payload")
)

// Bybit publicTrade message.
type bybitTradeMsg struct {
	Topic string          `json:"topic"`
	Type  string          `json:"type"` // snapshot
	Ts    int64           `json:"ts"`
	Data  []bybitTradeRow `json:"data"`

	// server/control messages
	Op      string `json:"op"`
	Success *bool  `json:"success"`
	RetMsg  string `json:"ret_msg"`
}

type bybitTradeRow struct {
	T   int64  `json:"T"` // fill time ms
	Sym string `json:"s"`
	S   string `json:"S"` // Buy/Sell
	V   string `json:"v"` // qty
	P   string `json:"p"` // price
	ID  string `json:"i"` // trade id (uuid)
	Seq int64  `json:"seq"`
}

// Bybit orderbook message (snapshot/delta).
type bybitOrderbookMsg struct {
	Topic string `json:"topic"`
	Type  string `json:"type"` // snapshot|delta
	Ts    int64  `json:"ts"`
	Cts   int64  `json:"cts"`
	Data  struct {
		Sym string     `json:"s"`
		B   [][]string `json:"b"`
		A   [][]string `json:"a"`
		U   int64      `json:"u"`
		Seq int64      `json:"seq"`
	} `json:"data"`

	// server/control messages
	Op      string `json:"op"`
	Success *bool  `json:"success"`
	RetMsg  string `json:"ret_msg"`
}

// OrderbookUpdate — распарсенный orderbook update (snapshot or delta).
// Состояние/агрегация делаются снаружи (в контроллере).
type OrderbookUpdate struct {
	Symbol     string
	IsSnapshot bool
	UpdateID   int64
	Seq        int64
	TsMs       int64
	Bids       [][]string
	Asks       [][]string
	// QualityFlags: "ok" normally; "ts_fallback" when exchange timestamp was missing.
	QualityFlags string
}


// ParsePublicTrade parses one WS message and returns normalized ticks.
// Returns (nil, nil) for non-trade messages or control frames.
func ParsePublicTrade(raw []byte) ([]models.NormalizedTick, error) {
	if len(bytes.TrimSpace(raw)) == 0 {
		return nil, errEmptyPayload
	}
	var msg bybitTradeMsg
	if err := json.Unmarshal(raw, &msg); err != nil {
		return nil, fmt.Errorf("decode trade msg: %w", err)
	}
	if msg.Op != "" {
		return nil, nil
	}
	if !strings.HasPrefix(msg.Topic, "publicTrade.") {
		return nil, nil
	}
	if len(msg.Data) == 0 {
		return nil, nil
	}

	out := make([]models.NormalizedTick, 0, len(msg.Data))
	for _, row := range msg.Data {
		sym := strings.ToUpper(strings.TrimSpace(row.Sym))
		if sym == "" {
			// fallback: topic contains symbol
			sym = strings.ToUpper(strings.TrimPrefix(msg.Topic, "publicTrade."))
		}
		if sym == "" {
			return nil, errors.New("missing symbol")
		}

		side := strings.ToUpper(strings.TrimSpace(row.S))
		switch side {
		case "BUY", "SELL":
			// ok
		default:
			// Bybit uses Buy/Sell
			if strings.EqualFold(row.S, "Buy") {
				side = "BUY"
			} else if strings.EqualFold(row.S, "Sell") {
				side = "SELL"
			} else {
				return nil, fmt.Errorf("unknown side: %q", row.S)
			}
		}

		// Timestamp resolution with quality flag.
		// quality_flags="ts_fallback" when exchange provides no valid ts.
		ts := row.T
		qualityFlags := "ok"
		if ts <= 0 {
			ts = msg.Ts
		}
		if ts <= 0 {
			ts = time.Now().UnixMilli()
			qualityFlags = "ts_fallback"
			monitoring.RecordIngestTsFallback("bybit", "tick")
		}

		tradeID := normalizeTradeID(row.ID)
		out = append(out, models.NormalizedTick{
			Symbol:       sym,
			Ts:           ts,
			Price:        strings.TrimSpace(row.P),
			Qty:          strings.TrimSpace(row.V),
			Quantity:     strings.TrimSpace(row.V),
			Side:         side,
			Source:       "bybit-linear",
			Market:       "USDT-M",
			TradeID:      tradeID,
			TradeIDRaw:   strings.TrimSpace(row.ID),
			Seq:          row.Seq,
			QualityFlags: qualityFlags,
		})
	}
	return out, nil
}

func normalizeTradeID(raw string) int64 {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return 0
	}
	if n, err := strconv.ParseInt(raw, 10, 64); err == nil {
		return n
	}
	h := fnv.New64a()
	_, _ = h.Write([]byte(raw))
	// Convert uint64 -> int64 deterministically.
	return int64(h.Sum64())
}

// ParseOrderbook parses one WS message and returns orderbook update.
// Returns (nil, nil) for non-orderbook or control messages.
func ParseOrderbook(raw []byte) (*OrderbookUpdate, error) {
	if len(bytes.TrimSpace(raw)) == 0 {
		return nil, errEmptyPayload
	}
	var msg bybitOrderbookMsg
	if err := json.Unmarshal(raw, &msg); err != nil {
		return nil, fmt.Errorf("decode orderbook msg: %w", err)
	}
	if msg.Op != "" {
		return nil, nil
	}
	if !strings.HasPrefix(msg.Topic, "orderbook.") {
		return nil, nil
	}
	if msg.Data.Sym == "" {
		// topic is orderbook.<depth>.<symbol>
		parts := strings.Split(msg.Topic, ".")
		if len(parts) >= 3 {
			msg.Data.Sym = parts[len(parts)-1]
		}
	}
	sym := strings.ToUpper(strings.TrimSpace(msg.Data.Sym))
	if sym == "" {
		return nil, errors.New("missing symbol")
	}

	isSnapshot := strings.EqualFold(strings.TrimSpace(msg.Type), "snapshot")
	isDelta := strings.EqualFold(strings.TrimSpace(msg.Type), "delta")
	if !isSnapshot && !isDelta {
		// unknown type; ignore silently (Bybit may add fields)
		return nil, nil
	}

	// Timestamp resolution with quality flag.
	ts := msg.Cts
	qualityFlags := "ok"
	if ts <= 0 {
		ts = msg.Ts
	}
	if ts <= 0 {
		ts = time.Now().UnixMilli()
		qualityFlags = "ts_fallback"
		monitoring.RecordIngestTsFallback("bybit", "book")
	}

	upd := &OrderbookUpdate{
		Symbol:       sym,
		IsSnapshot:   isSnapshot,
		UpdateID:     msg.Data.U,
		Seq:          msg.Data.Seq,
		TsMs:         ts,
		Bids:         msg.Data.B,
		Asks:         msg.Data.A,
		QualityFlags: qualityFlags,
	}
	return upd, nil
}

// --- Local in-memory orderbook state (top-N) ---

// priceLevel is an internal struct used to keep stable string value while sorting by float.
// For top-N book this is acceptable (N <= 200). We do not keep deep levels.
//
// NOTE: float sorting is a tradeoff: simpler and fast, but may be sensitive to very long decimals.
// For exchange prices this is typically safe. If you later need strict decimal ordering,
// replace float with fixed-point int64 (requires tick-size aware parsing).
type priceLevel struct {
	PriceStr string
	PriceF   float64
	SizeStr  string
}

// BookSideState keeps top levels for one side.
type BookSideState struct {
	levels map[string]priceLevel // key = PriceStr
}

func newBookSideState() *BookSideState {
	return &BookSideState{levels: make(map[string]priceLevel, 256)}
}

func (s *BookSideState) reset(levels [][]string) {
	s.levels = make(map[string]priceLevel, 256)
	for _, lv := range levels {
		if len(lv) < 2 {
			continue
		}
		p := strings.TrimSpace(lv[0])
		q := strings.TrimSpace(lv[1])
		if p == "" {
			continue
		}
		pf, err := strconv.ParseFloat(p, 64)
		if err != nil {
			continue
		}
		if q == "0" || q == "0.0" || q == "0.00" {
			continue
		}
		s.levels[p] = priceLevel{PriceStr: p, PriceF: pf, SizeStr: q}
	}
}

// applyDelta applies Bybit delta updates.
// size==0 means remove.
func (s *BookSideState) applyDelta(delta [][]string) {
	for _, lv := range delta {
		if len(lv) < 2 {
			continue
		}
		p := strings.TrimSpace(lv[0])
		q := strings.TrimSpace(lv[1])
		if p == "" {
			continue
		}
		// remove on zero
		if q == "0" || q == "0.0" || q == "0.00" {
			delete(s.levels, p)
			continue
		}
		pf, err := strconv.ParseFloat(p, 64)
		if err != nil {
			continue
		}
		s.levels[p] = priceLevel{PriceStr: p, PriceF: pf, SizeStr: q}
	}
}

func (s *BookSideState) toSortedSlice(desc bool) []priceLevel {
	out := make([]priceLevel, 0, len(s.levels))
	for _, lv := range s.levels {
		out = append(out, lv)
	}
	if desc {
		sort.Slice(out, func(i, j int) bool { return out[i].PriceF > out[j].PriceF })
	} else {
		sort.Slice(out, func(i, j int) bool { return out[i].PriceF < out[j].PriceF })
	}
	return out
}

// trimToN keeps only top N levels (by sorted order) and drops the rest.
func (s *BookSideState) trimToN(sorted []priceLevel, n int) []priceLevel {
	if n <= 0 {
		s.levels = make(map[string]priceLevel, 16)
		return nil
	}
	if len(sorted) > n {
		sorted = sorted[:n]
	}
	newMap := make(map[string]priceLevel, n*2)
	for _, lv := range sorted {
		newMap[lv.PriceStr] = lv
	}
	s.levels = newMap
	return sorted
}

// BookState stores both sides and last update IDs.
type BookState struct {
	Bids *BookSideState
	Asks *BookSideState
	// LastUpdateID is the most-recently applied Bybit u (update sequence).
	// Used to fill PrevFinal and to validate continuity of delta updates.
	LastUpdateID int64
}

func newBookState() *BookState {
	return &BookState{Bids: newBookSideState(), Asks: newBookSideState(), LastUpdateID: 0}
}

// Reset clears the entire book state so that ApplyUpdate will block deltas
// until the next snapshot arrives (Bybit guarantees a snapshot on re-subscribe).
func (bs *BookState) Reset() {
	bs.Bids = newBookSideState()
	bs.Asks = newBookSideState()
	bs.LastUpdateID = 0
}

// ApplyUpdate applies snapshot/delta and returns a full snapshot (topN) ready for publishing.
//
// Gap detection (delta updates only):
//   Bybit guarantees that consecutive delta UpdateIDs are strictly sequential.
//   If upd.UpdateID != bs.LastUpdateID+1 the local book is desynchronised;
//   we flush the state and signal the caller via NeedResnapshot=true so that
//   the controller can write to dlq:book_deltas and wait for the next snapshot.
//
// Returns:
//   bids, asks  – empty slices when GapDetected; do NOT publish to main stream.
//   prevU       – previous LastUpdateID (before this update).
//   gapDetected – true when a continuity violation was found.
//   gapExpected – what UpdateID we expected (prevU+1).
//   gapActual   – what UpdateID we received.
func (bs *BookState) ApplyUpdate(upd *OrderbookUpdate, topN int) (
	bids [][]string,
	asks [][]string,
	prevU int64,
	gapDetected bool,
	gapExpected int64,
	gapActual int64,
) {
	if upd == nil {
		return nil, nil, bs.LastUpdateID, false, 0, 0
	}
	prevU = bs.LastUpdateID

	// Safety: ignore deltas until we have at least one snapshot.
	// Bybit гарантирует первый snapshot после subscribe, но при reconnect/lag
	// возможны ситуации, когда delta придёт раньше.
	if !upd.IsSnapshot && len(bs.Bids.levels) == 0 && len(bs.Asks.levels) == 0 {
		return nil, nil, prevU, false, 0, 0
	}

	if upd.IsSnapshot {
		bs.Bids.reset(upd.Bids)
		bs.Asks.reset(upd.Asks)
	} else {
		// ── Gap detection ────────────────────────────────────────────────────
		// Bybit delta UpdateIDs must be strictly sequential: upd.UpdateID == prevU+1.
		// prevU==0 means this is the very first delta after a snapshot whose
		// UpdateID we stored; allow it (the book was already validated by the
		// snapshot path above).
		if prevU > 0 && upd.UpdateID != prevU+1 {
			// Gap detected: the local book is now stale.  Flush state so that
			// subsequent deltas are blocked until the next snapshot arrives.
			bs.Reset()
			return nil, nil, prevU, true, prevU + 1, upd.UpdateID
		}

		bs.Bids.applyDelta(upd.Bids)
		bs.Asks.applyDelta(upd.Asks)
	}

	b := bs.Bids.toSortedSlice(true)
	b = bs.Bids.trimToN(b, topN)
	a := bs.Asks.toSortedSlice(false)
	a = bs.Asks.trimToN(a, topN)

	bids = make([][]string, 0, len(b))
	for _, lv := range b {
		bids = append(bids, []string{lv.PriceStr, lv.SizeStr})
	}
	asks = make([][]string, 0, len(a))
	for _, lv := range a {
		asks = append(asks, []string{lv.PriceStr, lv.SizeStr})
	}

	if upd.UpdateID > 0 {
		bs.LastUpdateID = upd.UpdateID
	}
	return bids, asks, prevU, false, 0, 0
}
