// Пакет interfaces содержит интерфейсы для взаимодействия между компонентами
package interfaces

import (
	"context"
)

// CandlePublisher интерфейс для публикации данных свечей
type CandlePublisher interface {
	// PublishCandleData публикует данные свечи в Redis Stream для бэкенда
	PublishCandleData(ctx context.Context, symbol, timeframe string, candleData map[string]interface{}) error
}
