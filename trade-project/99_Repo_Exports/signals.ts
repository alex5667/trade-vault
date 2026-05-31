/**
 * Типы сигналов и триггеров для Scanner Infrastructure
 */

// Базовые интерфейсы

/**
 * Общий интерфейс для всех типов сигналов
 */
export interface BaseSignal {
	type: string         // Тип сигнала
	symbol: string       // Символ торговой пары
	timestamp: number    // Временная метка
}

/**
 * Базовый интерфейс для сигналов, содержащих данные свечи
 */
export interface CandleSignal extends BaseSignal {
	interval: string     // Интервал свечи (1m, 5m, 15m, etc.)
	open: number         // Цена открытия
	high: number         // Максимальная цена
	low: number          // Минимальная цена
	close: number        // Цена закрытия
}

// Сигналы волатильности

/**
 * Сигнал о всплеске волатильности
 */
export interface VolatilitySpike extends CandleSignal {
	type: 'volatilitySpike'
	volatility: number   // Процент волатильности
}

/**
 * Сигнал о волатильности на основе диапазона цен
 */
export interface VolatilityRange extends CandleSignal {
	type: 'volatilityRange'
	range: number        // Текущий диапазон цен
	avgRange: number     // Средний исторический диапазон
	volatility: number   // Процент превышения
}

// Интерфейсы для метрик

/**
 * Базовый интерфейс для элемента списка (для gainers, losers, etc.)
 */
export interface ListEntry {
	symbol: string       // Символ торговой пары
	change: string       // Процент изменения
}

/**
 * Интерфейс для элемента списка с данными об объеме
 */
export interface VolumeEntry extends ListEntry {
	volume: string           // Общий объем
	volumePercent: string    // Процент от общего объема топ-пар
	volume005Level: string   // 0.05% от объема
	volume2Level: string     // 2% от объема
	volume5Level: string     // 5% от объема
	volume10Level: string    // 10% от объема
	trade_front: string      // Объем для фронт-трейдинга
	trade_back: string       // Объем для бэк-трейдинга
}

/**
 * Интерфейс для элемента списка с данными о funding rate
 */
export interface FundingEntry extends ListEntry {
	rate: string         // Значение funding rate
	rateAbs: string      // Абсолютное значение funding rate
}

// Интерфейсы для уведомлений (триггеров)

/**
 * Базовый интерфейс для триггера
 */
export interface TriggerNotification {
	event: string        // Тип события (обычно "updated")
	key: string          // Ключ в Redis с данными
	time: number         // Временная метка
}

/**
 * Типы триггеров
 */
export enum TriggerType {
	// 5min timeframe triggers removed
}

/**
 * Типы сигналов
 */
export enum SignalType {
	VOLATILITY_SPIKE = 'volatilitySpike',
	VOLATILITY_RANGE = 'volatilityRange'
}

/**
 * Типы каналов Redis для данных
 */
export enum RedisDataChannel {
	TICKER_24H = 'binance:ticker24h',
	FUNDING_RATE = 'binance:fundingRate',
	KLINE = 'binance:kline',
	NEW_PAIRS = 'ws:new_pairs'
}

/**
 * Типы каналов Redis для сигналов
 */
export enum RedisSignalChannel {
	VOLATILITY = 'signal:volatility',
	VOLATILITY_RANGE = 'signal:volatilityRange'
}

/**
 * Тип для данных свечи из Binance WebSocket
 */
export interface BinanceKline {
	s: string    // Symbol
	i: string    // Interval
	t: number    // Timestamp
	o: string    // Open price
	h: string    // High price
	l: string    // Low price
	c: string    // Close price
	v: string    // Volume
	q: string    // Quote asset volume
	n: number    // Number of trades
	V: string    // Taker buy base asset volume
	Q: string    // Taker buy quote asset volume
	B: string    // Ignore
}

/**
 * Тип для данных тикера из Binance API
 */
export interface BinanceTicker24h {
	symbol: string           // Символ торговой пары
	priceChange: string      // Абсолютное изменение цены
	priceChangePercent: string // Процентное изменение цены
	weightedAvgPrice: string // Средневзвешенная цена
	prevClosePrice: string   // Цена закрытия предыдущего дня
	lastPrice: string        // Последняя цена
	lastQty: string          // Количество последней сделки
	bidPrice: string         // Цена покупки
	bidQty: string           // Количество покупки
	askPrice: string         // Цена продажи
	askQty: string           // Количество продажи
	openPrice: string        // Цена открытия
	highPrice: string        // Максимальная цена
	lowPrice: string         // Минимальная цена
	volume: string           // Объем
	quoteVolume: string      // Объем в котируемой валюте
	openTime: number         // Время открытия
	closeTime: number        // Время закрытия
	firstId: number          // ID первой сделки
	lastId: number           // ID последней сделки
	count: number            // Количество сделок
}

/**
 * Тип для данных funding rate из Binance API
 */
export interface BinanceFundingRate {
	symbol: string           // Символ торговой пары
	markPrice: string        // Цена маркировки
	indexPrice: string       // Индексная цена
	estimatedSettlePrice: string // Расчетная цена
	lastFundingRate: string  // Последняя ставка финансирования
	nextFundingTime: number  // Время следующего финансирования
	interestRate: string     // Процентная ставка
	time: number             // Временная метка
} 