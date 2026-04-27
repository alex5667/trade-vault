# signals/router.py
from typing import Optional, Tuple
from config.unified_pipeline import UnifiedPipelineMode, get_unified_mode

# Глобальный set для логирования fallback один раз на символ
_fallback_logged_symbols = set()


def should_emit_unified(ctx, logger, metrics) -> Tuple[bool, Optional[Exception]]:
    """
    Unified pipeline путь.
    Возвращает (should_emit, error).
    Если error != None, значит unified pipeline не смог обработать сигнал.
    """
    try:
        # Проверяем, есть ли unified pipeline в контексте
        # (должен быть установлен обработчиком)
        if hasattr(ctx, '_unified_pipeline') and ctx._unified_pipeline:
            # Тонкий роутер: ВСЯ логика внутри pipeline.process
            # process возвращает Signal или None - конвертируем в bool
            signal = ctx._unified_pipeline.process(ctx)
            return signal is not None, None
        else:
            return False, RuntimeError("Unified pipeline not configured")

    except Exception as e:
        return False, e


def should_emit_legacy(ctx) -> bool:
    """
    Legacy pipeline путь.
    Возвращает bool, без ошибок.
    """
    try:
        # Используем встроенную логику confidence check из обработчика
        if hasattr(ctx, '_get_min_confidence_for_symbol'):
            min_conf = ctx._get_min_confidence_for_symbol(getattr(ctx, 'symbol', ''))
            confidence = getattr(ctx, 'confidence', 0.0)
            return confidence >= min_conf
        else:
            # Fallback на простую проверку
            return getattr(ctx, 'confidence', 0.0) >= 0.5
    except Exception:
        # В случае ошибки возвращаем False
        return False


def should_emit(ctx, logger, metrics) -> bool:
    """
    Центральный роутер для принятия решения о эмиссии сигнала.
    Учитывает режим unified pipeline из ENV.
    """
    mode = get_unified_mode()

    if mode == UnifiedPipelineMode.LEGACY:
        # 0 → вообще не трогаем unified, только старый путь
        return should_emit_legacy(ctx)

    elif mode == UnifiedPipelineMode.UNIFIED:
        # 1 → только unified, без fallback
        try:
            ok, err = should_emit_unified(ctx, logger, metrics)
        except Exception as e:
            err = e
            ok = False

        if err is not None:
            logger.error(
                "unified pipeline error — strict mode, skipping signal",
                extra={
                    "symbol": getattr(ctx, 'symbol', 'unknown'),
                    "err": str(err)
                }
            )
            if metrics:
                metrics.inc_unified_error(getattr(ctx, 'symbol', 'unknown'))
            return False

        return ok

    elif mode == UnifiedPipelineMode.SAFE:
        # safe → сначала unified, при ошибке — fallback
        try:
            ok, err = should_emit_unified(ctx, logger, metrics)
        except Exception as e:
            err = e
            ok = False

        if err is not None:
            symbol = getattr(ctx, 'symbol', 'unknown')

            # Логируем fallback один раз на символ
            if symbol not in _fallback_logged_symbols:
                _fallback_logged_symbols.add(symbol)
                logger.error(
                    "unified pipeline failed — switching to legacy (safe mode)",
                    extra={
                        "symbol": symbol,
                        "err": str(err)
                    }
                )

            if metrics:
                metrics.inc_unified_error(symbol)
                metrics.inc_unified_fallback(symbol)

            return should_emit_legacy(ctx)

        return ok

    else:
        # На всякий случай — если что-то пошло совсем не так, уходим в legacy
        logger.warning(f"Unknown unified mode {mode}, falling back to legacy")
        return should_emit_legacy(ctx)
