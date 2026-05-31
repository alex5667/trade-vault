package binance

import (
	"fmt"
	"sync"

	"go.uber.org/zap"
)

// Глобальный счетчик для отслеживания количества подключений по символам и таймфреймам
var (
	wsConnectionCounters = make(map[string]int)
	wsCounterMutex       sync.RWMutex
)

// getWSConnectionCounter возвращает и увеличивает счетчик для конкретного символа и таймфрейма
func getWSConnectionCounter(symbol, timeframe string) int {
	key := fmt.Sprintf("%s@%s", symbol, timeframe)
	wsCounterMutex.Lock()
	defer wsCounterMutex.Unlock()

	wsConnectionCounters[key]++
	return wsConnectionCounters[key]
}

// StartWSForPair - ОТКЛЮЧЕНО: теперь используется MultiplexedManager
func StartWSForPair(pair string, timeframe ...string) {
	// ВСЕ WebSocket соединения теперь управляются через MultiplexedManager
	// Старые отдельные соединения отключены для экономии ресурсов
	zap.S().Infof("📤 StartWSForPair: пара %s:%v передана в MultiplexedManager", pair, timeframe)
}
