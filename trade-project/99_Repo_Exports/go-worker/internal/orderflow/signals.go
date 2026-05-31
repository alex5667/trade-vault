// internal/orderflow/signals.go
package orderflow

import (
	"go-worker/internal/config"
)

// Logger интерфейс для логгирования
type Logger interface {
	Errorf(format string, args ...interface{})
}

// Metrics интерфейс для метрик
type Metrics interface {
	IncUnifiedError(symbol string)
	LogUnifiedFallbackOnce(symbol string, err error)
}

// ShouldEmitUnified unified-путь возвращает (emit, error)
func ShouldEmitUnified(ctx *OrderflowCtx) (bool, error) {
	// 1. Проверяем Staleness (устаревание данных)
	// Если данные стакана слишком старые относительно тика - сигнал ненадежен.
	if ctx.L2IsStale {
		// Можно вернуть false без ошибки, так как это штатное отсечение
		return false, nil
	}

	// 2. В будущем сюда можно добавить:
	// - Проверку скоринга (Scoring)
	// - Фильтры волатильности / спреда
	// - Проверку режима рынка (Regime)

	// Пока считаем: если данные свежие -> сигнал валиден для унифицированного пайплайна
	return true, nil
}

// ShouldEmitLegacy legacy-путь просто bool, без ошибок
func ShouldEmitLegacy(ctx *OrderflowCtx) bool {
	// Ваш старый код принятия решения:
	// return legacyScorer.ShouldEmit(ctx)

	// Пока заглушка
	return false
}

// ShouldEmit центральный роутер с учётом режима unified pipeline
func ShouldEmit(ctx *OrderflowCtx, unifiedMode config.UnifiedPipelineMode, logger Logger, metrics Metrics) bool {
	switch unifiedMode {

	case config.UnifiedLegacy:
		// 0 → вообще не трогаем unified, только старый путь
		return ShouldEmitLegacy(ctx)

	case config.UnifiedStrict:
		// 1 → только unified, без fallback
		ok, err := ShouldEmitUnified(ctx)
		if err != nil {
			logger.Errorf("unified pipeline error — strict mode, skipping signal: symbol=%s, err=%v",
				ctx.Symbol, err)
			// В строгом режиме сигнал не шлём вообще
			if metrics != nil {
				metrics.IncUnifiedError(ctx.Symbol)
			}
			return false
		}
		return ok

	case config.UnifiedSafe:
		// safe → сначала unified, при ошибке — fallback
		ok, err := ShouldEmitUnified(ctx)
		if err != nil {
			// Логируем об ошибке unified и переходе на legacy
			if metrics != nil {
				metrics.LogUnifiedFallbackOnce(ctx.Symbol, err)
				metrics.IncUnifiedError(ctx.Symbol)
			}
			return ShouldEmitLegacy(ctx)
		}
		return ok

	default:
		// На всякий случай — если что-то пошло совсем не так, уходим в legacy
		return ShouldEmitLegacy(ctx)
	}
}
