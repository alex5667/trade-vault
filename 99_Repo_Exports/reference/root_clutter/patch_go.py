import re

with open("go-worker/binance/multiplexed_manager.go", "r") as f:
    text = f.read()

# 1. Add restartInProgress field
p1 = """	// Мониторинг
	monitor *monitoring.WebSocketMonitor
}"""

r1 = """	// Мониторинг
	monitor *monitoring.WebSocketMonitor

	// Флаг перезапуска
	restartInProgress int32
}"""
if p1 in text:
    text = text.replace(p1, r1)

# 2. Add Guard to updateConnectionsIfNeeded
p2 = """func (mm *MultiplexedManager) updateConnectionsIfNeeded() error {
	currentSymbols, err := mm.getAllSymbolsFromRedis()"""

r2 = """func (mm *MultiplexedManager) updateConnectionsIfNeeded() error {
	if !atomic.CompareAndSwapInt32(&mm.restartInProgress, 0, 1) {
		log.Printf("⚠️ Restart already in progress, skipping redundant update")
		return nil
	}
	defer atomic.StoreInt32(&mm.restartInProgress, 0)

	currentSymbols, err := mm.getAllSymbolsFromRedis()"""
if p2 in text:
    text = text.replace(p2, r2)
    
# 3. AddSymbol fix
p3 = """		mm.connections[connKey] = newConn
		mm.totalConnections++
		mm.totalSymbols++

		// Запускаем соединение
		go func() {
			if err := newConn.Start(); err != nil {
				// ОШИБКА: Оставляем логирование ошибок запуска
				log.Printf("❌ Ошибка запуска multiplexed соединения %s: %v", connKey, err)
			}
		}()"""

r3 = """		mm.connections[connKey] = newConn
		mm.totalConnections++

		// Запускаем соединение
		go func() {
			if err := newConn.Start(); err != nil {
				// ОШИБКА: Оставляем логирование ошибок запуска
				log.Printf("❌ Ошибка запуска multiplexed соединения %s: %v", connKey, err)
				return
			}
			// Start() завершился успешно — теперь безопасно учитываем символ.
			mm.connMutex.Lock()
			if _, exists := mm.connections[connKey]; exists {
				mm.totalSymbols++
			}
			mm.connMutex.Unlock()
		}()"""
if p3 in text:
    text = text.replace(p3, r3)

with open("go-worker/binance/multiplexed_manager.go", "w") as f:
    f.write(text)
print("done")
