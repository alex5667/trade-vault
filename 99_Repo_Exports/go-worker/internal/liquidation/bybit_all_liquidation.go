package liquidation

import (
	"encoding/json"
	"errors"
	"strings"
	"time"
)

// Bybit V5 Public allLiquidation stream.
// Topic: allLiquidation.{symbol}
// Endpoint (USDT perpetual / futures): wss://stream.bybit.com/v5/public/linear
//
// Нюанс:
//
//	поле data[].p в документации называется "Bankruptcy price".
//	Heatmap сервисы часто используют именно bankruptcy price как уровень.
//	Мы сохраняем его как Price.
//
// Сторона (data[].S):
//   - Buy  => long liquidation
//   - Sell => short liquidation
type bybitAllLiqMsg struct {
	Topic string           `json:"topic"`
	Ts    int64            `json:"ts"`
	Data  []bybitAllLiqRow `json:"data"`

	// служебные поля (pong/subscribe responses)
	Op      string `json:"op"`
	Success *bool  `json:"success"`
	RetMsg  string `json:"ret_msg"`
}

type bybitAllLiqRow struct {
	T  int64  `json:"T"` // timestamp (ms)
	S  string `json:"S"` // Buy/Sell
	Sx string `json:"s"` // symbol

	V string `json:"v"` // size
	P string `json:"p"` // bankruptcy price
}

// ParseBybitAllLiquidation может вернуть несколько событий (data array).
func ParseBybitAllLiquidation(raw []byte, recvTsMs int64) ([]NormalizedEvent, error) {
	var msg bybitAllLiqMsg
	if err := json.Unmarshal(raw, &msg); err != nil {
		return nil, err
	}

	// Серверные ответы (pong/subscribe) не содержат topic/data.
	if msg.Op != "" {
		return nil, nil
	}
	if len(msg.Data) == 0 {
		return nil, nil
	}

	out := make([]NormalizedEvent, 0, len(msg.Data))
	for _, row := range msg.Data {
		sym := strings.ToUpper(strings.TrimSpace(row.Sx))
		if sym == "" {
			return nil, errors.New("missing symbol")
		}
		rawSide := strings.TrimSpace(row.S)
		liqSide := ""
		switch strings.ToLower(rawSide) {
		case "buy":
			liqSide = "long"
		case "sell":
			liqSide = "short"
		default:
			return nil, errors.New("unknown side")
		}

		price := strings.TrimSpace(row.P)
		qty := strings.TrimSpace(row.V)
		ts := row.T
		if ts <= 0 {
			ts = msg.Ts
		}
		if ts <= 0 {
			ts = time.Now().UnixMilli()
		}

		notional, _ := mulDecimalStrings(price, qty)

		out = append(out, NormalizedEvent{
			Source:      "bybit_linear",
			Symbol:      sym,
			EventTsMs:   ts,
			RecvTsMs:    recvTsMs,
			Price:       price,
			Qty:         qty,
			NotionalUsd: notional,
			LiqSide:     liqSide,
			RawSide:     rawSide,
		})
	}
	return out, nil
}
