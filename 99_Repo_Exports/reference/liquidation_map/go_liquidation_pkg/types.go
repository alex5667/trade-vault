package liquidation

// NormalizedEvent — унифицированное liquidation событие.
//
// Контракт:
//   - все времена — epoch milliseconds (UTC)
//   - price/qty/notionalUsd — строки (Decimal-as-string), чтобы избежать float rounding
//   - LiqSide — сторона ликвидируемой позиции: "long" или "short"
type NormalizedEvent struct {
	Source string // e.g. "binance_usdm", "bybit_linear"

	Symbol string // "BTCUSDT"

	EventTsMs int64 // timestamp биржи (trade time, если доступно)
	RecvTsMs  int64 // timestamp приёма/нормализации

	Price string // bankruptcy/mark/exec price depending on venue
	Qty   string // executed size (обычно в базовой валюте)

	NotionalUsd string // Price * Qty

	LiqSide string // "long" | "short" — позиция, которую ликвидировали
	RawSide string // исходная сторона из payload
}
