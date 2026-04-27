package binance

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"go-worker/internal/models"
	"go-worker/internal/monitoring"
	"go-worker/pkg/timeutil"
)

// BinanceStreamEnvelope описывает универсальную обертку сообщения Binance multiplex WS.
type BinanceStreamEnvelope struct {
	Stream string          `json:"stream"`
	Data   json.RawMessage `json:"data"`
}

// flexInt64 хранит int64 значение, поддерживая строковые и числовые JSON.
type flexInt64 struct {
	value int64
	valid bool
}

func (f *flexInt64) UnmarshalJSON(raw []byte) error {
	raw = bytes.TrimSpace(raw)
	if len(raw) == 0 || bytes.Equal(raw, []byte("null")) {
		f.valid = false
		f.value = 0
		return nil
	}

	if raw[0] == '"' {
		str, err := strconv.Unquote(string(raw))
		if err != nil {
			f.valid = false
			f.value = 0
			return nil
		}
		str = strings.TrimSpace(str)
		if str == "" {
			f.valid = false
			f.value = 0
			return nil
		}
		if parsed, err := strconv.ParseInt(str, 10, 64); err == nil {
			f.value = parsed
			f.valid = true
			return nil
		}
		f.valid = false
		f.value = 0
		return nil
	}

	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	var v any
	if err := decoder.Decode(&v); err != nil {
		f.valid = false
		f.value = 0
		return nil
	}

	switch val := v.(type) {
	case json.Number:
		if parsed, err := val.Int64(); err == nil {
			f.value = parsed
			f.valid = true
			return nil
		}
		if flt, err := val.Float64(); err == nil {
			f.value = int64(flt)
			f.valid = true
			return nil
		}
	case float64:
		f.value = int64(val)
		f.valid = true
		return nil
	case string:
		val = strings.TrimSpace(val)
		if parsed, err := strconv.ParseInt(val, 10, 64); err == nil {
			f.value = parsed
			f.valid = true
			return nil
		}
	}

	f.valid = false
	f.value = 0
	return nil
}

func (f flexInt64) Int64() int64 {
	return f.value
}

func (f flexInt64) Valid() bool {
	return f.valid
}

func (f flexInt64) Or(other flexInt64) flexInt64 {
	if f.valid {
		return f
	}
	return other
}

func (f flexInt64) OrDefault(def int64) int64 {
	if f.valid {
		return f.value
	}
	return def
}

// BinanceAggTrade – payload аггрегированных сделок (aggTrade).
type BinanceAggTrade struct {
	EventType  string    `json:"e"`
	EventTime  flexInt64 `json:"E"`
	TradeTime  flexInt64 `json:"T"`
	TradeID    flexInt64 `json:"a"`
	Price      string    `json:"p"`
	Quantity   string    `json:"q"`
	IsBuyerMkt bool      `json:"m"` // true = покупатель – маркетмейкер → агрессор продавец
}

// BinanceDepth – diff depth обновление.
type BinanceDepth struct {
	EventType       string     `json:"e"`
	EventTime       flexInt64  `json:"E"`
	TransactionTime flexInt64  `json:"T"`
	FirstID         flexInt64  `json:"U"`
	FinalID         flexInt64  `json:"u"`
	PrevFinal       flexInt64  `json:"pu"`
	Bids            [][]string `json:"b"`
	Asks            [][]string `json:"a"`
}

var (
	errEmptyPayload = errors.New("пустое сообщение Binance futures")
)

// NormalizeFuturesMessage преобразует raw WS сообщение Binance в нормализованные структуры.
func NormalizeFuturesMessage(symbol string, raw []byte) (ticks []models.NormalizedTick, books []models.NormalizedDepth, err error) {
	if len(raw) == 0 {
		return nil, nil, errEmptyPayload
	}

	var envelope BinanceStreamEnvelope
	if err = json.Unmarshal(raw, &envelope); err != nil {
		return nil, nil, fmt.Errorf("decode envelope: %w", err)
	}

	switch {
	case strings.Contains(envelope.Stream, "aggTrade"):
		var trade BinanceAggTrade
		if err = json.Unmarshal(envelope.Data, &trade); err != nil {
			return nil, nil, fmt.Errorf("decode aggTrade: %w", err)
		}

		side := "BUY"
		if trade.IsBuyerMkt {
			side = "SELL"
		}

		// Timestamp resolution with quality flag.
		qualityFlags := "ok"
		tradeTs := trade.EventTime.Or(trade.TradeTime).OrDefault(0)
		if tradeTs <= 0 {
			tradeTs = timeutil.GetCurrentTimestampMs()
			qualityFlags = "ts_fallback"
			monitoring.RecordIngestTsFallback("binance", "tick")
		}

		ticks = append(ticks, models.NormalizedTick{
			Symbol:       symbol,
			Ts:           tradeTs,
			Price:        trade.Price,
			Qty:          trade.Quantity,
			Quantity:     trade.Quantity,
			Side:         side,
			Source:       "binance-futures",
			Market:       "USDT-M",
			TradeID:      trade.TradeID.Int64(),
			QualityFlags: qualityFlags,
		})
		return ticks, nil, nil

	case strings.Contains(envelope.Stream, "depth"):
		var depth BinanceDepth
		if err = json.Unmarshal(envelope.Data, &depth); err != nil {
			return nil, nil, fmt.Errorf("decode depth: %w", err)
		}

		// Timestamp resolution with quality flag.
		qualityFlags := "ok"
		depthTs := depth.EventTime.Or(depth.TransactionTime).Int64()
		if !depth.EventTime.Valid() && !depth.TransactionTime.Valid() {
			depthTs = timeutil.GetCurrentTimestampMs()
			qualityFlags = "ts_fallback"
			monitoring.RecordIngestTsFallback("binance", "book")
		} else if depthTs <= 0 {
			depthTs = timeutil.GetCurrentTimestampMs()
			qualityFlags = "ts_fallback"
			monitoring.RecordIngestTsFallback("binance", "book")
		}

		books = append(books, models.NormalizedDepth{
			Symbol:       symbol,
			Ts:           depthTs,
			FirstID:      depth.FirstID.Int64(),
			FinalID:      depth.FinalID.Int64(),
			PrevFinal:    depth.PrevFinal.Int64(),
			Bids:         depth.Bids,
			Asks:         depth.Asks,
			Source:       "binance-futures",
			Market:       "USDT-M",
			QualityFlags: qualityFlags,
		})
		return nil, books, nil
	default:
		// Игнорируем нерелевантные сообщения без ошибки.
		return nil, nil, nil
	}
}
