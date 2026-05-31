package liquidation

import (
	"encoding/json"
	"errors"
	"strings"
	"time"
)

// Binance USDT-M Force Order Stream.
//
// Для all-market stream используется endpoint:
//
//	wss://fstream.binance.com/ws/!forceOrder@arr
//
// Формат сообщения совпадает с обычным <symbol>@forceOrder.
//
// Семантика стороны:
//
//	В payload Binance поле o.S — сторона ордера (BUY/SELL).
//	Для force liquidation это обычно означает:
//	  * SELL ордер закрывает LONG позицию (т.е. ликвидирован LONG)
//	  * BUY  ордер закрывает SHORT позицию (т.е. ликвидирован SHORT)
type binanceForceOrderMsg struct {
	EventTime int64                `json:"E"`
	Order     binanceForceOrderObj `json:"o"`
}

type binanceForceOrderObj struct {
	Symbol    string `json:"s"`
	Side      string `json:"S"`  // BUY/SELL
	Price     string `json:"p"`  // order price
	AvgPrice  string `json:"ap"` // average price
	Qty       string `json:"q"`  // orig qty
	FilledQty string `json:"z"`  // filled qty
	TradeTime int64  `json:"T"`  // trade time
}

// ParseBinanceForceOrder нормализует одно Binance сообщение.
func ParseBinanceForceOrder(raw []byte, recvTsMs int64) (NormalizedEvent, error) {
	var msg binanceForceOrderMsg
	if err := json.Unmarshal(raw, &msg); err != nil {
		return NormalizedEvent{}, err
	}

	sym := strings.ToUpper(strings.TrimSpace(msg.Order.Symbol))
	if sym == "" {
		return NormalizedEvent{}, errors.New("missing symbol")
	}

	side := strings.ToUpper(strings.TrimSpace(msg.Order.Side))
	liqSide := ""
	switch side {
	case "SELL":
		liqSide = "long"
	case "BUY":
		liqSide = "short"
	default:
		return NormalizedEvent{}, errors.New("unknown side")
	}

	// Предпочитаем average price (ap), если она есть.
	price := strings.TrimSpace(msg.Order.AvgPrice)
	if price == "" || price == "0" {
		price = strings.TrimSpace(msg.Order.Price)
	}
	qty := strings.TrimSpace(msg.Order.FilledQty)
	if qty == "" || qty == "0" {
		qty = strings.TrimSpace(msg.Order.Qty)
	}

	// TradeTime обычно точнее для позиционирования в heatmap.
	ts := msg.Order.TradeTime
	if ts <= 0 {
		ts = msg.EventTime
	}
	if ts <= 0 {
		ts = time.Now().UnixMilli()
	}

	notional, _ := mulDecimalStrings(price, qty)

	return NormalizedEvent{
		Source:      "binance_usdm",
		Symbol:      sym,
		EventTsMs:   ts,
		RecvTsMs:    recvTsMs,
		Price:       price,
		Qty:         qty,
		NotionalUsd: notional,
		LiqSide:     liqSide,
		RawSide:     side,
	}, nil
}
