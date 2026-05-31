// internal/config/unified.go
package config

import (
	"os"
	"strings"
)

// UnifiedPipelineMode определяет режим работы unified pipeline
type UnifiedPipelineMode int

const (
	UnifiedLegacy UnifiedPipelineMode = iota // 0 → полностью legacy
	UnifiedStrict                            // 1 → только unified (без fallback)
	UnifiedSafe                              // safe → unified + fallback
)

// ParseUnifiedModeFromEnv парсит режим из переменной окружения USE_UNIFIED_PIPELINE
func ParseUnifiedModeFromEnv() UnifiedPipelineMode {
	raw := strings.ToLower(strings.TrimSpace(os.Getenv("USE_UNIFIED_PIPELINE")))

	switch raw {
	case "0", "legacy":
		return UnifiedLegacy
	case "1", "unified":
		return UnifiedStrict
	case "safe", "":
		// По умолчанию "safe" — это удобно для постепенного rollout
		return UnifiedSafe
	default:
		// Неизвестное значение → логируем и идём в safe
		// TODO: добавить логгер если нужен
		return UnifiedSafe
	}
}
