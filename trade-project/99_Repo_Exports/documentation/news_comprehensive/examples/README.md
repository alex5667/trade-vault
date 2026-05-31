# Примеры Использования Новостного Пайплайна

## Обзор

Этот раздел содержит практические примеры использования новостного пайплайна в различных сценариях. Все примеры включают полный код, конфигурацию и объяснения.

## Пример 1: Базовая Интеграция Новостей в Торговые Сигналы

### Полная Реализация с Нуля

```python
#!/usr/bin/env python3
"""
Пример полной интеграции новостного анализа в торговую стратегию
"""

import asyncio
import redis
import json
import time
from dataclasses import dataclass
from typing import Optional, Dict, Any, List
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Импорт компонентов новостного пайплайна
from news_pipeline.enricher import NewsEnricher
from news_pipeline.filters import NewsBasedFilter, FilterAction
from news_pipeline.weights import NewsBasedWeightAdjuster
from news_pipeline.horizons import NewsBasedHorizonManager
from news_pipeline.risk import NewsAwareRiskManager

@dataclass
class TradeSignal:
    """Структура торгового сигнала"""
    signal_id: str
    symbol: str
    direction: str  # "long" or "short"
    confidence: float
    price: float
    timestamp_ms: int
    strategy_name: str
    news: Optional[Any] = None  # Будет добавлено enricher'ом

@dataclass
class TradeDecision:
    """Решение о торговле"""
    signal_id: str
    symbol: str
    direction: str
    weight: float
    position_size: float
    horizon_sec: int
    stop_loss: float
    take_profit: Optional[float]
    reasoning: str

class NewsAwareTradingBot:
    """
    Торговый бот с полной интеграцией новостного анализа
    """

    def __init__(self, config: Dict[str, Any]):
        self.config = config

        # Инициализация Redis
        self.redis = redis.from_url(config['redis_url'])

        # Инициализация компонентов новостного пайплайна
        self.news_enricher = NewsEnricher(config['redis_url'])
        self.signal_filter = NewsBasedFilter()
        self.weight_adjuster = NewsBasedWeightAdjuster()
        self.horizon_manager = NewsBasedHorizonManager()
        self.risk_manager = NewsAwareRiskManager(self._create_base_risk_manager(), self.news_enricher)

        # Статистика
        self.stats = {
            'signals_processed': 0,
            'signals_filtered': 0,
            'trades_opened': 0,
            'total_pnl': 0.0
        }

    def _create_base_risk_manager(self):
        """Создание базового менеджера рисков"""
        class BaseRiskManager:
            def calculate_position_size(self, signal, capital, risk_percent):
                # Простая реализация: 1% от капитала на сделку
                return capital * (risk_percent / 100)

            def calculate_stop_loss(self, signal, entry_price, direction):
                # 2% стоп-лосс
                if direction == "long":
                    return entry_price * 0.98
                else:
                    return entry_price * 1.02

            def should_exit(self, signal, pnl, hold_time):
                # Выход через 1 час или при -2% убытке
                return hold_time > 3600 or pnl < -0.02

        return BaseRiskManager()

    async def process_signal(self, raw_signal: TradeSignal) -> Optional[TradeDecision]:
        """
        Обработка торгового сигнала с учетом новостей
        """
        self.stats['signals_processed'] += 1

        try:
            # Шаг 1: Обогащение новостными данными
            self.news_enricher.attach(raw_signal, asset_class="crypto")

            # Логирование новостного контекста
            self._log_news_context(raw_signal)

            # Шаг 2: Применение фильтров
            filter_result = self.signal_filter.apply(raw_signal)

            if filter_result.action == FilterAction.BLOCK:
                self.stats['signals_filtered'] += 1
                logger.info(f"Signal {raw_signal.signal_id} blocked: {filter_result.reason}")
                return None

            # Шаг 3: Корректировка веса сигнала
            adjusted_weight = self.weight_adjuster.adjust_weight(raw_signal)

            # Шаг 4: Расчет временного горизонта
            horizon = self.horizon_manager.calculate_horizon(raw_signal)

            # Шаг 5: Расчет размера позиции
            position_size = self.risk_manager.calculate_position_size(
                raw_signal,
                capital=self.config['capital'],
                risk_percent=self.config['risk_per_trade_percent']
            )

            # Шаг 6: Расчет стоп-лосса
            stop_loss = self.risk_manager.adjust_stop_loss(
                raw_signal, raw_signal.price, raw_signal.direction
            )

            # Шаг 7: Расчет take-profit (опционально)
            take_profit = self._calculate_take_profit(raw_signal, stop_loss)

            # Шаг 8: Формирование решения
            decision = TradeDecision(
                signal_id=raw_signal.signal_id,
                symbol=raw_signal.symbol,
                direction=raw_signal.direction,
                weight=adjusted_weight,
                position_size=position_size,
                horizon_sec=horizon,
                stop_loss=stop_loss,
                take_profit=take_profit,
                reasoning=self._build_reasoning(raw_signal, filter_result, adjusted_weight, horizon)
            )

            self.stats['trades_opened'] += 1
            logger.info(f"Trade decision for {raw_signal.signal_id}: {decision.weight:.2f} weight, {decision.horizon_sec}s horizon")

            return decision

        except Exception as e:
            logger.error(f"Error processing signal {raw_signal.signal_id}: {e}")
            return None

    def _log_news_context(self, signal: TradeSignal):
        """Логирование новостного контекста"""
        if not signal.news:
            logger.debug(f"No news data for {signal.signal_id}")
            return

        news = signal.news
        logger.info(f"News context for {signal.signal_id}: "
                   f"grade={news.news_grade_id}, risk={news.news_risk:.2f}, "
                   f"surprise={news.surprise_score:.2f}, confidence={news.confidence:.2f}")

    def _calculate_take_profit(self, signal: TradeSignal, stop_loss: float) -> Optional[float]:
        """Расчет take-profit уровня"""
        risk_amount = abs(signal.price - stop_loss)
        reward_ratio = 2.0  # Risk:Reward = 1:2

        if signal.direction == "long":
            return signal.price + (risk_amount * reward_ratio)
        else:
            return signal.price - (risk_amount * reward_ratio)

    def _build_reasoning(self, signal: TradeSignal, filter_result, weight: float, horizon: int) -> str:
        """Формирование объяснения решения"""
        reasons = [f"Strategy: {signal.strategy_name}"]

        if signal.news:
            reasons.append(f"News grade: {signal.news.news_grade_id}")
            reasons.append(f"News risk: {signal.news.news_risk:.2f}")

        if filter_result.modifications:
            reasons.append(f"Filter modifications: {filter_result.modifications}")

        reasons.append(f"Final weight: {weight:.2f}")
        reasons.append(f"Horizon: {horizon}s")

        return " | ".join(reasons)

    def get_stats(self) -> Dict[str, Any]:
        """Получение статистики"""
        return self.stats.copy()

async def main():
    """Основная функция для демонстрации"""

    # Конфигурация
    config = {
        'redis_url': 'redis://localhost:6379/0',
        'capital': 10000.0,
        'risk_per_trade_percent': 1.0
    }

    # Создание бота
    bot = NewsAwareTradingBot(config)

    # Пример сигналов для обработки
    sample_signals = [
        TradeSignal(
            signal_id="signal_001",
            symbol="BTCUSDT",
            direction="long",
            confidence=0.85,
            price=45000.0,
            timestamp_ms=int(time.time() * 1000),
            strategy_name="RSI_Divergence"
        ),
        TradeSignal(
            signal_id="signal_002",
            symbol="ETHUSDT",
            direction="short",
            confidence=0.75,
            price=2800.0,
            timestamp_ms=int(time.time() * 1000),
            strategy_name="MACD_Crossover"
        )
    ]

    # Обработка сигналов
    for signal in sample_signals:
        decision = await bot.process_signal(signal)

        if decision:
            print(f"✓ Signal {signal.signal_id}: Trade {decision.direction} "
                  f"{decision.position_size:.2f} at {decision.weight:.2f} weight")
        else:
            print(f"✗ Signal {signal.signal_id}: Filtered out")

    # Вывод статистики
    stats = bot.get_stats()
    print(f"\nСтатистика: {stats}")

if __name__ == "__main__":
    asyncio.run(main())
```

### Конфигурация для Примера

```yaml
# config.yaml
redis:
  url: "redis://localhost:6379/0"
  cache_ttl_ms: 1500

trading:
  capital: 10000.0
  risk_per_trade_percent: 1.0
  max_open_positions: 5

news_integration:
  enabled: true
  asset_class: "crypto"
  filter_critical_news: true
  weight_adjustment: true
  horizon_management: true

logging:
  level: "INFO"
  format: "json"
```

## Пример 2: Анализ Качества Новостей

### Система Оценки и Улучшения Качества

```python
#!/usr/bin/env python3
"""
Пример системы анализа и улучшения качества новостей
"""

import asyncio
import redis
import json
import time
import statistics
from typing import Dict, List, Any, Optional, Tuple
from dataclasses import dataclass, asdict
from enum import Enum
import logging

logger = logging.getLogger(__name__)

class QualityLevel(Enum):
    EXCELLENT = "excellent"
    GOOD = "good"
    FAIR = "fair"
    POOR = "poor"
    UNUSABLE = "unusable"

@dataclass
class QualityMetrics:
    """Метрики качества новости"""
    risk_consistency: float
    surprise_reasonableness: float
    tag_relevance: float
    summary_quality: float
    overall_score: float
    level: QualityLevel

@dataclass
class AnalysisResult:
    """Результат анализа новости"""
    uid: str
    risk_score: float
    surprise_score: float
    confidence_score: float
    tags: List[str]
    primary_tag: str
    summary: str
    quality: QualityMetrics
    processing_time_ms: float
    model_used: str

class NewsQualityAnalyzer:
    """
    Анализатор качества новостного анализа
    """

    def __init__(self, redis_url: str, llm_client):
        self.redis = redis.from_url(redis_url)
        self.llm_client = llm_client
        self.historical_data_key = "news:quality:historical"

        # Пороги качества
        self.quality_thresholds = {
            'excellent': 0.9,
            'good': 0.75,
            'fair': 0.6,
            'poor': 0.4
        }

    async def analyze_quality(self, analysis_result: AnalysisResult) -> QualityMetrics:
        """
        Комплексный анализ качества результата анализа
        """
        # Расчет индивидуальных метрик
        risk_consistency = await self._evaluate_risk_consistency(analysis_result)
        surprise_reasonableness = self._evaluate_surprise_reasonableness(analysis_result)
        tag_relevance = await self._evaluate_tag_relevance(analysis_result)
        summary_quality = self._evaluate_summary_quality(analysis_result)

        # Композитный score
        overall_score = (
            risk_consistency * 0.3 +
            surprise_reasonableness * 0.2 +
            tag_relevance * 0.25 +
            summary_quality * 0.25
        )

        # Определение уровня качества
        level = self._determine_quality_level(overall_score)

        metrics = QualityMetrics(
            risk_consistency=risk_consistency,
            surprise_reasonableness=surprise_reasonableness,
            tag_relevance=tag_relevance,
            summary_quality=summary_quality,
            overall_score=overall_score,
            level=level
        )

        # Сохранение для исторического анализа
        await self._save_quality_metrics(analysis_result.uid, metrics)

        return metrics

    async def _evaluate_risk_consistency(self, result: AnalysisResult) -> float:
        """
        Оценка консистентности risk score с историческими данными
        """
        # Получение похожих анализов
        similar_results = await self._find_similar_analyses(result)

        if not similar_results:
            return 0.5  # Нейтральная оценка

        # Расчет отклонения
        historical_risks = [r['risk_score'] for r in similar_results]
        mean_risk = statistics.mean(historical_risks)
        std_risk = statistics.stdev(historical_risks) if len(historical_risks) > 1 else 0.1

        if std_risk == 0:
            return 1.0 if abs(result.risk_score - mean_risk) < 0.05 else 0.0

        z_score = abs(result.risk_score - mean_risk) / std_risk
        consistency = max(0.0, 1.0 - (z_score / 4.0))  # Нормализация

        return consistency

    def _evaluate_surprise_reasonableness(self, result: AnalysisResult) -> float:
        """
        Оценка разумности surprise score
        """
        risk = result.risk_score
        surprise = abs(result.surprise_score)

        # Логика: высокий риск должен коррелировать с surprise
        if risk > 0.7:  # Высокий риск
            if surprise > 0.5:
                return 1.0
            elif surprise > 0.3:
                return 0.7
            else:
                return 0.3
        elif risk > 0.4:  # Средний риск
            if surprise > 0.3:
                return 0.8
            elif surprise > 0.1:
                return 0.9
            else:
                return 0.6
        else:  # Низкий риск
            if surprise < 0.2:
                return 0.9
            elif surprise < 0.4:
                return 0.6
            else:
                return 0.3

    async def _evaluate_tag_relevance(self, result: AnalysisResult) -> float:
        """
        Оценка релевантности тегов
        """
        if not result.tags:
            return 0.0

        # Использование LLM для оценки релевантности
        prompt = f"""
        Evaluate if these tags are relevant to the news summary.
        Return only a number from 0.0 to 1.0.

        Summary: {result.summary}
        Tags: {', '.join(result.tags)}
        Relevance:
        """

        try:
            # Создание временного запроса для оценки
            temp_request = type('TempRequest', (), {
                'uid': f"relevance_{result.uid}",
                'title': "Tag Relevance Check",
                'url': "internal://relevance",
                'source': "quality_check"
            })()

            async with self.llm_client:
                relevance_response = await self.llm_client.analyze(temp_request)

            # Извлечение оценки из ответа
            relevance_text = relevance_response.summary
            relevance_score = float(relevance_text.strip()) if relevance_text.strip() else 0.0

            return max(0.0, min(1.0, relevance_score))

        except Exception as e:
            logger.warning(f"Tag relevance evaluation failed: {e}")
            # Fallback: базовая эвристика
            return min(1.0, len(result.tags) / 5.0)  # 0-1.0 на основе количества тегов

    def _evaluate_summary_quality(self, result: AnalysisResult) -> float:
        """
        Оценка качества summary
        """
        summary = result.summary.strip()
        score = 0.0

        # Длина summary (50-150 символов оптимально)
        length = len(summary)
        if 50 <= length <= 150:
            score += 0.4
        elif 30 <= length <= 200:
            score += 0.3
        elif length < 20:
            score += 0.1

        # Наличие ключевых слов
        keywords = ['fed', 'ecb', 'rate', 'inflation', 'earnings', 'crypto', 'bitcoin', 'ethereum']
        keyword_count = sum(1 for keyword in keywords if keyword.lower() in summary.lower())
        if keyword_count > 0:
            score += 0.3 * min(keyword_count / 3.0, 1.0)

        # Читаемость
        if not summary.isupper():  # Не все caps
            score += 0.2
        if not summary.replace('.', '').replace('%', '').isdigit():  # Не только цифры
            score += 0.1

        return min(1.0, score)

    def _determine_quality_level(self, score: float) -> QualityLevel:
        """Определение уровня качества по score"""
        if score >= self.quality_thresholds['excellent']:
            return QualityLevel.EXCELLENT
        elif score >= self.quality_thresholds['good']:
            return QualityLevel.GOOD
        elif score >= self.quality_thresholds['fair']:
            return QualityLevel.FAIR
        elif score >= self.quality_thresholds['poor']:
            return QualityLevel.POOR
        else:
            return QualityLevel.UNUSABLE

    async def _find_similar_analyses(self, result: AnalysisResult, limit: int = 20) -> List[Dict[str, Any]]:
        """
        Поиск похожих анализов в истории
        """
        try:
            # Получение недавних анализов
            recent_keys = self.redis.keys("news:analysis:*")
            recent_keys = recent_keys[-limit:] if len(recent_keys) > limit else recent_keys

            similar = []
            for key in recent_keys:
                data = self.redis.get(key)
                if data:
                    analysis = json.loads(data)
                    # Простая проверка на схожесть по тегам
                    existing_tags = set(analysis.get('tags', []))
                    current_tags = set(result.tags)
                    if existing_tags & current_tags:  # Есть общие теги
                        similar.append({
                            'risk_score': analysis.get('risk', 0.0),
                            'tags': list(existing_tags),
                            'similarity': len(existing_tags & current_tags)
                        })

            # Сортировка по similarity
            similar.sort(key=lambda x: x['similarity'], reverse=True)
            return similar[:10]

        except Exception as e:
            logger.warning(f"Failed to find similar analyses: {e}")
            return []

    async def _save_quality_metrics(self, uid: str, metrics: QualityMetrics):
        """Сохранение метрик качества для анализа"""
        try:
            quality_data = {
                'uid': uid,
                'timestamp': time.time(),
                **asdict(metrics)
            }

            # Сохранение в Redis hash
            self.redis.hset(self.historical_data_key, uid, json.dumps(quality_data))

            # Ограничение размера истории (удаление старых записей)
            if self.redis.hlen(self.historical_data_key) > 10000:
                # Получение всех ключей и удаление oldest 10%
                all_keys = self.redis.hkeys(self.historical_data_key)
                keys_to_remove = all_keys[:1000]  # Старые 10%
                if keys_to_remove:
                    self.redis.hdel(self.historical_data_key, *keys_to_remove)

        except Exception as e:
            logger.warning(f"Failed to save quality metrics: {e}")

    async def get_quality_report(self) -> Dict[str, Any]:
        """Генерация отчета по качеству анализа"""
        try:
            all_data = self.redis.hgetall(self.historical_data_key)
            if not all_data:
                return {'error': 'No quality data available'}

            # Парсинг данных
            quality_scores = []
            level_counts = {level.value: 0 for level in QualityLevel}

            for uid, data_str in all_data.items():
                try:
                    data = json.loads(data_str)
                    quality_scores.append(data['overall_score'])
                    level_counts[data['level']] += 1
                except (json.JSONDecodeError, KeyError):
                    continue

            if not quality_scores:
                return {'error': 'No valid quality data'}

            # Статистика
            report = {
                'total_analyses': len(quality_scores),
                'average_quality': statistics.mean(quality_scores),
                'median_quality': statistics.median(quality_scores),
                'quality_std': statistics.stdev(quality_scores) if len(quality_scores) > 1 else 0,
                'quality_distribution': level_counts,
                'quality_percentiles': {
                    '25th': statistics.quantiles(quality_scores, n=4)[0],
                    '50th': statistics.quantiles(quality_scores, n=4)[1],
                    '75th': statistics.quantiles(quality_scores, n=4)[2],
                    '90th': statistics.quantiles(quality_scores, n=4)[3] if len(quality_scores) >= 4 else max(quality_scores)
                },
                'timestamp': time.time()
            }

            return report

        except Exception as e:
            logger.error(f"Failed to generate quality report: {e}")
            return {'error': str(e)}

class QualityImprovementEngine:
    """
    Движок улучшения качества анализа
    """

    def __init__(self, quality_analyzer: NewsQualityAnalyzer, llm_client):
        self.quality_analyzer = quality_analyzer
        self.llm_client = llm_client

    async def improve_analysis(self, original_result: AnalysisResult) -> AnalysisResult:
        """
        Улучшение анализа низкого качества
        """
        # Оценка текущего качества
        quality = await self.quality_analyzer.analyze_quality(original_result)

        if quality.level in [QualityLevel.EXCELLENT, QualityLevel.GOOD]:
            return original_result  # Не нужно улучшать

        logger.info(f"Improving analysis for {original_result.uid} (quality: {quality.level.value})")

        if quality.level == QualityLevel.UNUSABLE:
            # Полная перегенерация
            return await self._regenerate_analysis(original_result)
        else:
            # Частичное улучшение
            return await self._refine_analysis(original_result, quality)

    async def _regenerate_analysis(self, original: AnalysisResult) -> AnalysisResult:
        """
        Полная перегенерация анализа
        """
        regeneration_prompt = f"""
        Previous analysis had very low quality. Please re-analyze this news carefully with improved accuracy.

        Original Analysis (LOW QUALITY):
        - Risk: {original.risk_score}
        - Surprise: {original.surprise_score}
        - Tags: {original.tags}
        - Summary: {original.summary}

        News Details:
        - Title: {getattr(original, 'title', 'Unknown')}
        - URL: {getattr(original, 'url', 'Unknown')}
        - Source: {getattr(original, 'source', 'Unknown')}

        Provide a high-quality re-analysis with:
        - Accurate risk assessment (0.0-1.0)
        - Proper surprise evaluation (-1.0-1.0)
        - Relevant tags from the allowed set
        - Clear, concise summary
        - High confidence score
        """

        try:
            # Создание нового запроса
            improved_request = type('ImprovedRequest', (), {
                'uid': f"improved_{original.uid}",
                'title': getattr(original, 'title', 'Unknown'),
                'url': getattr(original, 'url', 'Unknown'),
                'source': getattr(original, 'source', 'Unknown')
            })()

            async with self.llm_client:
                improved_result = await self.llm_client.analyze(improved_request)

            logger.info(f"Regenerated analysis for {original.uid}: "
                       f"risk {original.risk_score:.2f}->{improved_result.risk_score:.2f}, "
                       f"confidence {original.confidence_score:.2f}->{improved_result.confidence_score:.2f}")

            return improved_result

        except Exception as e:
            logger.error(f"Analysis regeneration failed: {e}")
            return original

    async def _refine_analysis(self, original: AnalysisResult, quality: QualityMetrics) -> AnalysisResult:
        """
        Частичное улучшение анализа
        """
        refinements_needed = []

        # Определение, что нужно улучшить
        if quality.risk_consistency < 0.7:
            refinements_needed.append("risk_consistency")
        if quality.tag_relevance < 0.7:
            refinements_needed.append("tag_relevance")
        if quality.summary_quality < 0.7:
            refinements_needed.append("summary_quality")

        if not refinements_needed:
            return original

        refinement_prompt = f"""
        Refine the following news analysis. Focus on improving: {', '.join(refinements_needed)}

        Current Analysis:
        - Risk: {original.risk_score}
        - Surprise: {original.surprise_score}
        - Tags: {original.tags}
        - Summary: {original.summary}

        News: {getattr(original, 'title', 'Unknown')}

        Provide refined values that improve the identified weaknesses while keeping good aspects unchanged.
        """

        try:
            refinement_request = type('RefinementRequest', (), {
                'uid': f"refine_{original.uid}",
                'title': "Analysis Refinement",
                'url': "internal://refinement",
                'source': "quality_improvement"
            })()

            async with self.llm_client:
                refinement = await self.llm_client.analyze(refinement_request)

            # Смешивание оригинального и уточненного анализа
            refined = AnalysisResult(
                uid=original.uid,
                risk_score=self._blend_scores(original.risk_score, refinement.risk_score, quality.risk_consistency),
                surprise_score=self._blend_scores(original.surprise_score, refinement.surprise_score, quality.surprise_reasonableness),
                confidence_score=max(original.confidence_score, refinement.confidence_score),
                tags=list(set(original.tags + refinement.tags)),  # Объединение тегов
                primary_tag=refinement.primary_tag or original.primary_tag,
                summary=refinement.summary if quality.summary_quality < 0.8 else original.summary,
                quality=quality,  # Оригинальные метрики качества
                processing_time_ms=original.processing_time_ms + refinement.processing_time_ms,
                model_used=f"{original.model_used}+refined"
            )

            logger.info(f"Refined analysis for {original.uid}, improvements: {refinements_needed}")
            return refined

        except Exception as e:
            logger.warning(f"Analysis refinement failed: {e}")
            return original

    def _blend_scores(self, original: float, refined: float, quality_score: float) -> float:
        """
        Смешивание оригинального и уточненного scores на основе качества
        """
        # Чем ниже качество, тем больше веса уточнению
        refinement_weight = max(0.0, 1.0 - quality_score)
        return (original * (1.0 - refinement_weight)) + (refined * refinement_weight)

async def demo_quality_analysis():
    """
    Демонстрация анализа качества
    """
    print("=== News Quality Analysis Demo ===\n")

    # Создание компонентов (в реальности нужно инициализировать LLM клиент)
    quality_analyzer = NewsQualityAnalyzer("redis://localhost:6379", llm_client=None)
    improvement_engine = QualityImprovementEngine(quality_analyzer, llm_client=None)

    # Пример результатов анализа
    sample_results = [
        AnalysisResult(
            uid="sample_1",
            risk_score=0.85,
            surprise_score=0.3,
            confidence_score=0.9,
            tags=["fomc", "rates"],
            primary_tag="fomc",
            summary="Federal Reserve signals potential rate increase",
            quality=None,
            processing_time_ms=1500,
            model_used="gemini-1.5-pro"
        ),
        AnalysisResult(
            uid="sample_2",
            risk_score=0.1,
            surprise_score=0.0,
            confidence_score=0.3,
            tags=["earnings"],
            primary_tag="earnings",
            summary="Company reports Q3 results",
            quality=None,
            processing_time_ms=1200,
            model_used="gemini-1.5-pro"
        )
    ]

    print("Analyzing quality of sample analyses...\n")

    for result in sample_results:
        try:
            # В реальности здесь был бы анализ качества
            # quality = await quality_analyzer.analyze_quality(result)
            # improved = await improvement_engine.improve_analysis(result)

            print(f"Analysis {result.uid}:")
            print(f"  Risk: {result.risk_score:.2f}, Surprise: {result.surprise_score:.2f}")
            print(f"  Tags: {result.tags}")
            print(f"  Summary: {result.summary}")
            print(f"  Confidence: {result.confidence_score:.2f}")
            print()

        except Exception as e:
            print(f"Error analyzing {result.uid}: {e}\n")

    # Получение отчета по качеству
    try:
        # report = await quality_analyzer.get_quality_report()
        # print("Quality Report:")
        # print(json.dumps(report, indent=2))
        print("Quality report generation would be shown here in real implementation")
    except Exception as e:
        print(f"Error generating report: {e}")

if __name__ == "__main__":
    asyncio.run(demo_quality_analysis())
```

## Пример 3: Мониторинг Новостного Пайплайна

### Полная Система Мониторинга

```python
#!/usr/bin/env python3
"""
Пример полной системы мониторинга новостного пайплайна
"""

import asyncio
import redis
import json
import time
import psutil
import requests
from typing import Dict, List, Any, Optional, Tuple
from dataclasses import dataclass
from collections import defaultdict, deque
import logging
import statistics

logger = logging.getLogger(__name__)

@dataclass
class HealthStatus:
    """Статус здоровья компонента"""
    component: str
    status: str  # "healthy", "degraded", "unhealthy"
    last_check: float
    message: str
    metrics: Dict[str, Any]

@dataclass
class PipelineMetrics:
    """Метрики пайплайна"""
    ingestion_rate: float  # новостей/мин
    analysis_rate: float   # анализов/мин
    error_rate: float      # ошибок/мин
    avg_latency: float     # средняя задержка (мс)
    queue_depth: int       # глубина очередей
    cache_hit_rate: float  # процент попаданий в кеш

class NewsPipelineMonitor:
    """
    Монитор новостного пайплайна
    """

    def __init__(self, redis_url: str, config: Dict[str, Any]):
        self.redis = redis.from_url(redis_url)
        self.config = config

        # Компоненты для мониторинга
        self.components = {
            'news_ingestor': config['ingestor_health_url'],
            'news_analyzer': config['analyzer_health_url'],
            'redis': None,  # Redis мониторится напрямую
            'llm_api': None  # API мониторится через запросы
        }

        # История метрик
        self.metrics_history = defaultdict(lambda: deque(maxlen=100))
        self.health_history = defaultdict(lambda: deque(maxlen=50))

        # Алерты
        self.alerts = []
        self.alert_callbacks = []

    async def start_monitoring(self):
        """
        Запуск мониторинга
        """
        logger.info("Starting news pipeline monitoring")

        # Запуск фоновых задач
        tasks = [
            asyncio.create_task(self._health_check_loop()),
            asyncio.create_task(self._metrics_collection_loop()),
            asyncio.create_task(self._alert_processing_loop()),
            asyncio.create_task(self._performance_monitoring_loop())
        ]

        await asyncio.gather(*tasks)

    async def _health_check_loop(self):
        """
        Цикл проверки здоровья компонентов
        """
        check_interval = self.config.get('health_check_interval_sec', 30)

        while True:
            try:
                await self._perform_health_checks()
                await asyncio.sleep(check_interval)
            except Exception as e:
                logger.error(f"Health check loop error: {e}")
                await asyncio.sleep(5)

    async def _perform_health_checks(self):
        """
        Выполнение проверок здоровья всех компонентов
        """
        health_statuses = {}

        # Проверка Redis
        health_statuses['redis'] = await self._check_redis_health()

        # Проверка HTTP сервисов
        for component, url in self.components.items():
            if url:
                health_statuses[component] = await self._check_http_health(component, url)

        # Проверка LLM API
        health_statuses['llm_api'] = await self._check_llm_health()

        # Сохранение результатов
        timestamp = time.time()
        for component, status in health_statuses.items():
            self.health_history[component].append({
                'timestamp': timestamp,
                'status': status.status,
                'message': status.message,
                'metrics': status.metrics
            })

            # Проверка на алерты
            await self._check_health_alerts(component, status)

        # Логирование проблем
        unhealthy = [comp for comp, status in health_statuses.items() if status.status == 'unhealthy']
        if unhealthy:
            logger.warning(f"Unhealthy components: {unhealthy}")

    async def _check_redis_health(self) -> HealthStatus:
        """
        Проверка здоровья Redis
        """
        try:
            start_time = time.time()

            # Проверка подключения
            await self.redis.ping()

            # Получение информации о Redis
            info = await self.redis.info()
            memory_usage = info.get('used_memory', 0)
            connected_clients = info.get('connected_clients', 0)

            # Проверка очередей
            raw_queue_len = await self.redis.xlen('news:raw')
            analysis_queue_len = await self.redis.xlen('news:analysis')

            response_time = (time.time() - start_time) * 1000

            # Оценка здоровья
            status = 'healthy'
            message = 'OK'

            if response_time > 100:
                status = 'degraded'
                message = f'High latency: {response_time:.1f}ms'
            elif raw_queue_len > 10000 or analysis_queue_len > 50000:
                status = 'degraded'
                message = f'Queue overflow: raw={raw_queue_len}, analysis={analysis_queue_len}'

            return HealthStatus(
                component='redis',
                status=status,
                last_check=time.time(),
                message=message,
                metrics={
                    'response_time_ms': response_time,
                    'memory_usage': memory_usage,
                    'connected_clients': connected_clients,
                    'raw_queue_length': raw_queue_len,
                    'analysis_queue_length': analysis_queue_len
                }
            )

        except Exception as e:
            return HealthStatus(
                component='redis',
                status='unhealthy',
                last_check=time.time(),
                message=f'Redis error: {e}',
                metrics={}
            )

    async def _check_http_health(self, component: str, url: str) -> HealthStatus:
        """
        Проверка здоровья HTTP сервиса
        """
        try:
            start_time = time.time()

            # HTTP запрос с таймаутом
            response = requests.get(url, timeout=5)
            response_time = (time.time() - start_time) * 1000

            if response.status_code == 200:
                data = response.json()
                status = data.get('status', 'unknown')
                message = data.get('message', 'OK')

                # Преобразование статуса
                if status in ['healthy', 'ok']:
                    health_status = 'healthy'
                elif status == 'degraded':
                    health_status = 'degraded'
                else:
                    health_status = 'unhealthy'

                return HealthStatus(
                    component=component,
                    status=health_status,
                    last_check=time.time(),
                    message=message,
                    metrics={
                        'response_time_ms': response_time,
                        'http_status_code': response.status_code,
                        'data': data
                    }
                )
            else:
                return HealthStatus(
                    component=component,
                    status='unhealthy',
                    last_check=time.time(),
                    message=f'HTTP {response.status_code}',
                    metrics={'http_status_code': response.status_code}
                )

        except requests.exceptions.RequestException as e:
            return HealthStatus(
                component=component,
                status='unhealthy',
                last_check=time.time(),
                message=f'HTTP error: {e}',
                metrics={}
            )

    async def _check_llm_health(self) -> HealthStatus:
        """
        Проверка здоровья LLM API
        """
        # В реальной реализации здесь был бы тестовый запрос к LLM API
        # Для демонстрации возвращаем mock статус
        return HealthStatus(
            component='llm_api',
            status='healthy',
            last_check=time.time(),
            message='Mock LLM health check',
            metrics={'quota_remaining': 95, 'response_time_ms': 250}
        )

    async def _check_health_alerts(self, component: str, status: HealthStatus):
        """
        Проверка условий для алертов
        """
        if status.status == 'unhealthy':
            alert = {
                'type': 'health',
                'component': component,
                'severity': 'critical',
                'message': f'{component} is unhealthy: {status.message}',
                'timestamp': time.time(),
                'details': status.metrics
            }
            await self._trigger_alert(alert)

        elif status.status == 'degraded':
            # Алерт только если degradation длится > 5 минут
            recent_health = list(self.health_history[component])[-5:]  # Последние 5 проверок
            degraded_count = sum(1 for h in recent_health if h['status'] == 'degraded')

            if degraded_count >= 3:  # 3 из 5 проверок degraded
                alert = {
                    'type': 'health',
                    'component': component,
                    'severity': 'warning',
                    'message': f'{component} is degraded: {status.message}',
                    'timestamp': time.time(),
                    'details': status.metrics
                }
                await self._trigger_alert(alert)

    async def _trigger_alert(self, alert: Dict[str, Any]):
        """
        Триггер алерта
        """
        self.alerts.append(alert)
        logger.warning(f"Alert triggered: {alert}")

        # Вызов callback'ов
        for callback in self.alert_callbacks:
            try:
                await callback(alert)
            except Exception as e:
                logger.error(f"Alert callback error: {e}")

    async def _metrics_collection_loop(self):
        """
        Цикл сбора метрик
        """
        collection_interval = self.config.get('metrics_collection_interval_sec', 15)

        while True:
            try:
                await self._collect_metrics()
                await asyncio.sleep(collection_interval)
            except Exception as e:
                logger.error(f"Metrics collection error: {e}")
                await asyncio.sleep(5)

    async def _collect_metrics(self):
        """
        Сбор метрик пайплайна
        """
        timestamp = time.time()

        try:
            # Метрики ingestion
            ingestion_metrics = await self._collect_ingestion_metrics()

            # Метрики analysis
            analysis_metrics = await self._collect_analysis_metrics()

            # Метрики системы
            system_metrics = self._collect_system_metrics()

            # Комбинированные метрики
            combined = {
                'timestamp': timestamp,
                **ingestion_metrics,
                **analysis_metrics,
                **system_metrics
            }

            # Сохранение в историю
            for key, value in combined.items():
                if key != 'timestamp':
                    self.metrics_history[key].append({'timestamp': timestamp, 'value': value})

            # Сохранение в Redis для внешнего доступа
            await self.redis.setex(
                'news:pipeline:metrics',
                300,  # 5 минут TTL
                json.dumps(combined)
            )

        except Exception as e:
            logger.error(f"Metrics collection failed: {e}")

    async def _collect_ingestion_metrics(self) -> Dict[str, Any]:
        """
        Сбор метрик ingestion
        """
        try:
            # Длины потоков
            raw_len = await self.redis.xlen('news:raw')
            analysis_len = await self.redis.xlen('news:analysis')

            # Скорость обработки (изменение длины очередей)
            prev_raw_len = self._get_previous_metric('raw_queue_length', 0)
            prev_analysis_len = self._get_previous_metric('analysis_queue_length', 0)

            # Счетчики из Redis
            total_ingested = await self.redis.get('metrics:news:ingested:total') or 0
            total_analyzed = await self.redis.get('metrics:news:analyzed:total') or 0

            return {
                'raw_queue_length': raw_len,
                'analysis_queue_length': analysis_len,
                'total_ingested': int(total_ingested),
                'total_analyzed': int(total_analyzed),
                'queue_growth_rate': raw_len - prev_raw_len  # Изменение за интервал
            }

        except Exception as e:
            logger.error(f"Ingestion metrics collection failed: {e}")
            return {}

    async def _collect_analysis_metrics(self) -> Dict[str, Any]:
        """
        Сбор метрик analysis
        """
        try:
            # Ошибки анализа
            analysis_errors = await self.redis.get('metrics:analysis:errors:total') or 0

            # Качество анализа
            quality_data = await self.redis.hgetall('metrics:analysis:quality')
            avg_quality = 0.0
            if quality_data:
                qualities = [float(v) for v in quality_data.values() if v.replace('.', '').isdigit()]
                if qualities:
                    avg_quality = statistics.mean(qualities)

            # Латентность
            latency_data = self.metrics_history['analysis_latency']
            avg_latency = statistics.mean([d['value'] for d in latency_data]) if latency_data else 0

            return {
                'analysis_errors': int(analysis_errors),
                'average_quality': avg_quality,
                'analysis_latency_ms': avg_latency
            }

        except Exception as e:
            logger.error(f"Analysis metrics collection failed: {e}")
            return {}

    def _collect_system_metrics(self) -> Dict[str, Any]:
        """
        Сбор системных метрик
        """
        try:
            # CPU и память
            cpu_percent = psutil.cpu_percent(interval=1)
            memory = psutil.virtual_memory()
            disk = psutil.disk_usage('/')

            return {
                'cpu_percent': cpu_percent,
                'memory_percent': memory.percent,
                'memory_used_gb': memory.used / (1024**3),
                'disk_percent': disk.percent,
                'disk_used_gb': disk.used / (1024**3)
            }

        except Exception as e:
            logger.error(f"System metrics collection failed: {e}")
            return {}

    def _get_previous_metric(self, metric_name: str, default: float) -> float:
        """
        Получение предыдущего значения метрики
        """
        history = self.metrics_history[metric_name]
        if history:
            return history[-1]['value']
        return default

    async def _alert_processing_loop(self):
        """
        Цикл обработки алертов
        """
        while True:
            try:
                # Обработка накопленных алертов
                await self._process_alerts()
                await asyncio.sleep(60)  # Каждую минуту
            except Exception as e:
                logger.error(f"Alert processing error: {e}")
                await asyncio.sleep(5)

    async def _process_alerts(self):
        """
        Обработка алертов (группировка, escalation, etc.)
        """
        if not self.alerts:
            return

        # Группировка алертов по типу и компоненту
        alert_groups = defaultdict(list)
        for alert in self.alerts:
            key = f"{alert['type']}:{alert['component']}"
            alert_groups[key].append(alert)

        # Обработка каждой группы
        for group_key, group_alerts in alert_groups.items():
            if len(group_alerts) >= 3:  # 3+ алерта = серьезная проблема
                # Создание escalated алерта
                escalated = {
                    'type': 'escalated',
                    'component': group_key,
                    'severity': 'critical',
                    'message': f'Multiple alerts for {group_key}: {len(group_alerts)} alerts in last period',
                    'timestamp': time.time(),
                    'alerts': group_alerts[-5:]  # Последние 5 алертов
                }
                await self._trigger_alert(escalated)

        # Очистка обработанных алертов (старше 1 часа)
        cutoff = time.time() - 3600
        self.alerts = [a for a in self.alerts if a['timestamp'] > cutoff]

    async def _performance_monitoring_loop(self):
        """
        Цикл мониторинга производительности
        """
        while True:
            try:
                await self._analyze_performance()
                await asyncio.sleep(300)  # Каждые 5 минут
            except Exception as e:
                logger.error(f"Performance analysis error: {e}")
                await asyncio.sleep(60)

    async def _analyze_performance(self):
        """
        Анализ производительности пайплайна
        """
        try:
            # Анализ трендов
            trends = self._analyze_metric_trends()

            # Прогнозные алерты
            await self._predictive_alerts(trends)

            # Рекомендации по оптимизации
            recommendations = self._generate_optimization_recommendations(trends)

            # Сохранение анализа
            analysis = {
                'timestamp': time.time(),
                'trends': trends,
                'recommendations': recommendations
            }

            await self.redis.setex(
                'news:pipeline:performance_analysis',
                3600,  # 1 час
                json.dumps(analysis)
            )

        except Exception as e:
            logger.error(f"Performance analysis failed: {e}")

    def _analyze_metric_trends(self) -> Dict[str, Any]:
        """
        Анализ трендов метрик
        """
        trends = {}

        for metric_name, history in self.metrics_history.items():
            if len(history) < 10:  # Нужно минимум 10 точек
                continue

            values = [h['value'] for h in history]

            try:
                # Базовая статистика
                current = values[-1]
                average = statistics.mean(values)
                trend = self._calculate_trend(values)

                trends[metric_name] = {
                    'current': current,
                    'average': average,
                    'trend': trend,  # positive/negative/stable
                    'volatility': statistics.stdev(values) if len(values) > 1 else 0
                }
            except Exception as e:
                logger.debug(f"Trend calculation failed for {metric_name}: {e}")

        return trends

    def _calculate_trend(self, values: List[float]) -> str:
        """
        Расчет тренда (упрощенная версия)
        """
        if len(values) < 5:
            return 'insufficient_data'

        # Сравнение последних значений с более ранними
        recent_avg = statistics.mean(values[-3:])
        earlier_avg = statistics.mean(values[:-3])

        if recent_avg > earlier_avg * 1.1:  # Рост > 10%
            return 'increasing'
        elif recent_avg < earlier_avg * 0.9:  # Падение > 10%
            return 'decreasing'
        else:
            return 'stable'

    async def _predictive_alerts(self, trends: Dict[str, Any]):
        """
        Прогнозные алерты на основе трендов
        """
        # Алерт на рост латентности
        if 'analysis_latency_ms' in trends:
            latency_trend = trends['analysis_latency_ms']
            if latency_trend['trend'] == 'increasing' and latency_trend['current'] > 2000:
                alert = {
                    'type': 'predictive',
                    'component': 'analysis_latency',
                    'severity': 'warning',
                    'message': f'Analysis latency trending up: {latency_trend["current"]:.1f}ms',
                    'timestamp': time.time(),
                    'details': latency_trend
                }
                await self._trigger_alert(alert)

        # Алерт на переполнение очередей
        if 'raw_queue_length' in trends:
            queue_trend = trends['raw_queue_length']
            if queue_trend['trend'] == 'increasing' and queue_trend['current'] > 5000:
                alert = {
                    'type': 'predictive',
                    'component': 'queue_overflow',
                    'severity': 'warning',
                    'message': f'Raw news queue growing: {queue_trend["current"]} items',
                    'timestamp': time.time(),
                    'details': queue_trend
                }
                await self._trigger_alert(alert)

    def _generate_optimization_recommendations(self, trends: Dict[str, Any]) -> List[str]:
        """
        Генерация рекомендаций по оптимизации
        """
        recommendations = []

        # Анализ латентности
        if 'analysis_latency_ms' in trends:
            latency = trends['analysis_latency_ms']['current']
            if latency > 3000:
                recommendations.append("Consider upgrading to faster LLM model or increasing parallel processing")
            elif latency > 5000:
                recommendations.append("URGENT: Analysis latency critically high, consider failover to backup system")

        # Анализ использования CPU
        if 'cpu_percent' in trends:
            cpu = trends['cpu_percent']['current']
            if cpu > 80:
                recommendations.append("High CPU usage detected, consider horizontal scaling")
            elif cpu > 95:
                recommendations.append("CRITICAL: CPU usage near 100%, immediate scaling required")

        # Анализ очередей
        if 'raw_queue_length' in trends:
            queue_len = trends['raw_queue_length']['current']
            if queue_len > 10000:
                recommendations.append("News ingestion queue backing up, consider increasing processing capacity")
            elif queue_len > 50000:
                recommendations.append("URGENT: Critical queue overflow, risk of data loss")

        return recommendations

    def get_dashboard_data(self) -> Dict[str, Any]:
        """
        Получение данных для дашборда
        """
        return {
            'current_health': {comp: list(hist)[-1] if hist else None
                             for comp, hist in self.health_history.items()},
            'recent_metrics': {metric: list(hist)[-10:] if hist else []
                              for metric, hist in self.metrics_history.items()},
            'active_alerts': self.alerts[-10:],  # Последние 10 алертов
            'performance_analysis': self.redis.get('news:pipeline:performance_analysis'),
            'timestamp': time.time()
        }

    def add_alert_callback(self, callback):
        """
        Добавление callback'а для алертов
        """
        self.alert_callbacks.append(callback)

async def demo_monitoring():
    """
    Демонстрация системы мониторинга
    """
    print("=== News Pipeline Monitoring Demo ===\n")

    config = {
        'redis_url': 'redis://localhost:6379',
        'ingestor_health_url': 'http://localhost:8097/health',
        'analyzer_health_url': 'http://localhost:8098/health',
        'health_check_interval_sec': 30,
        'metrics_collection_interval_sec': 15
    }

    monitor = NewsPipelineMonitor(config['redis_url'], config)

    # Добавление callback'а для алертов
    async def alert_handler(alert):
        print(f"🚨 ALERT: {alert['component']} - {alert['message']}")

    monitor.add_alert_callback(alert_handler)

    print("Starting monitoring (press Ctrl+C to stop)...\n")

    try:
        # Запуск мониторинга на 60 секунд для демонстрации
        await asyncio.wait_for(monitor.start_monitoring(), timeout=60.0)
    except asyncio.TimeoutError:
        print("\nMonitoring demo completed")
    except KeyboardInterrupt:
        print("\nMonitoring stopped by user")

    # Получение финального состояния
    dashboard = monitor.get_dashboard_data()
    print("\nFinal Dashboard State:")
    print(f"Health checks: {len(dashboard['current_health'])} components")
    print(f"Metrics collected: {len(dashboard['recent_metrics'])} types")
    print(f"Active alerts: {len(dashboard['active_alerts'])}")

if __name__ == "__main__":
    asyncio.run(demo_monitoring())
```

## Пример 4: A/B Тестирование

### Система для Сравнения Разных Подходов

```python
#!/usr/bin/env python3
"""
Пример системы A/B тестирования для новостного пайплайна
"""

import asyncio
import redis
import json
import time
import random
import hashlib
import statistics
from typing import Dict, List, Any, Optional, Callable, Tuple
from dataclasses import dataclass
from enum import Enum
import logging

logger = logging.getLogger(__name__)

class ExperimentStatus(Enum):
    DRAFT = "draft"
    RUNNING = "running"
    COMPLETED = "completed"
    STOPPED = "stopped"

@dataclass
class ExperimentConfig:
    """Конфигурация эксперимента"""
    name: str
    description: str
    variants: List[str]  # Названия вариантов (control, treatment_a, treatment_b, etc.)
    traffic_distribution: Dict[str, float]  # Распределение трафика по вариантам
    target_metric: str  # Основная метрика для оценки
    secondary_metrics: List[str]  # Дополнительные метрики
    min_sample_size: int  # Минимальный размер выборки
    confidence_level: float  # Уровень доверия для статистической значимости
    duration_days: int  # Продолжительность эксперимента

@dataclass
class ExperimentResult:
    """Результат эксперимента"""
    experiment_name: str
    variant: str
    sample_size: int
    metrics: Dict[str, float]
    confidence_intervals: Dict[str, Tuple[float, float]]
    statistical_significance: Dict[str, bool]

class ABTestingFramework:
    """
    Фреймворк для A/B тестирования новостного пайплайна
    """

    def __init__(self, redis_url: str):
        self.redis = redis.from_url(redis_url)
        self.experiments: Dict[str, ExperimentConfig] = {}
        self.variant_assignments: Dict[str, str] = {}  # user_id -> variant

    async def create_experiment(self, config: ExperimentConfig) -> bool:
        """
        Создание нового эксперимента
        """
        try:
            # Валидация конфигурации
            if not self._validate_experiment_config(config):
                logger.error(f"Invalid experiment config: {config.name}")
                return False

            # Сохранение конфигурации
            experiment_key = f"ab:experiment:{config.name}"
            await self.redis.set(
                experiment_key,
                json.dumps({
                    'name': config.name,
                    'description': config.description,
                    'variants': config.variants,
                    'traffic_distribution': config.traffic_distribution,
                    'target_metric': config.target_metric,
                    'secondary_metrics': config.secondary_metrics,
                    'min_sample_size': config.min_sample_size,
                    'confidence_level': config.confidence_level,
                    'duration_days': config.duration_days,
                    'created_at': time.time(),
                    'status': ExperimentStatus.DRAFT.value
                })
            )

            self.experiments[config.name] = config
            logger.info(f"Created experiment: {config.name}")
            return True

        except Exception as e:
            logger.error(f"Failed to create experiment {config.name}: {e}")
            return False

    def _validate_experiment_config(self, config: ExperimentConfig) -> bool:
        """
        Валидация конфигурации эксперимента
        """
        # Проверка распределения трафика
        total_traffic = sum(config.traffic_distribution.values())
        if abs(total_traffic - 1.0) > 0.001:
            logger.error(f"Traffic distribution must sum to 1.0, got {total_traffic}")
            return False

        # Проверка что все варианты имеют распределение
        if set(config.variants) != set(config.traffic_distribution.keys()):
            logger.error("All variants must have traffic distribution")
            return False

        # Проверка минимальных значений
        if config.min_sample_size < 100:
            logger.warning("Minimum sample size is very low, consider increasing")

        if config.confidence_level < 0.8:
            logger.warning("Confidence level is low, consider 0.95 or higher")

        return True

    async def start_experiment(self, experiment_name: str) -> bool:
        """
        Запуск эксперимента
        """
        try:
            experiment_key = f"ab:experiment:{experiment_name}"
            experiment_data = await self.redis.get(experiment_key)

            if not experiment_data:
                logger.error(f"Experiment {experiment_name} not found")
                return False

            experiment = json.loads(experiment_data)

            if experiment['status'] != ExperimentStatus.DRAFT.value:
                logger.error(f"Experiment {experiment_name} is not in draft status")
                return False

            # Обновление статуса
            experiment['status'] = ExperimentStatus.RUNNING.value
            experiment['started_at'] = time.time()

            await self.redis.set(experiment_key, json.dumps(experiment))

            logger.info(f"Started experiment: {experiment_name}")
            return True

        except Exception as e:
            logger.error(f"Failed to start experiment {experiment_name}: {e}")
            return False

    def assign_variant(self, experiment_name: str, user_id: str) -> str:
        """
        Присвоение варианта пользователю (детерминированное)
        """
        if experiment_name not in self.experiments:
            # Загрузка из Redis если не в памяти
            # В реальной реализации здесь будет загрузка
            return "control"

        config = self.experiments[experiment_name]

        # Детерминированное присвоение на основе hash
        hash_input = f"{experiment_name}:{user_id}"
        hash_value = int(hashlib.md5(hash_input.encode()).hexdigest()[:8], 16)
        normalized_hash = hash_value / 2**32  # 0.0 - 1.0

        # Присвоение варианта на основе распределения
        cumulative = 0.0
        for variant, weight in config.traffic_distribution.items():
            cumulative += weight
            if normalized_hash <= cumulative:
                return variant

        return config.variants[0]  # Fallback

    async def track_event(self, experiment_name: str, user_id: str,
                         event_name: str, event_value: float = 1.0,
                         metadata: Optional[Dict[str, Any]] = None):
        """
        Отслеживание события для эксперимента
        """
        try:
            variant = self.assign_variant(experiment_name, user_id)

            event_data = {
                'experiment': experiment_name,
                'user_id': user_id,
                'variant': variant,
                'event_name': event_name,
                'event_value': event_value,
                'timestamp': time.time(),
                'metadata': metadata or {}
            }

            # Сохранение события
            event_key = f"ab:event:{experiment_name}:{int(time.time())}:{user_id}"
            await self.redis.setex(event_key, 86400 * 30, json.dumps(event_data))  # 30 дней

            # Агрегация метрик
            await self._update_experiment_metrics(experiment_name, variant, event_name, event_value)

        except Exception as e:
            logger.error(f"Failed to track event: {e}")

    async def _update_experiment_metrics(self, experiment_name: str, variant: str,
                                       event_name: str, event_value: float):
        """
        Обновление агрегированных метрик эксперимента
        """
        try:
            metrics_key = f"ab:metrics:{experiment_name}:{variant}"

            # Использование Redis pipelines для атомарных обновлений
            pipe = self.redis.pipeline()

            # Инкремент счетчика событий
            pipe.hincrby(metrics_key, f"event_count:{event_name}", 1)

            # Сумма значений событий
            pipe.hincrbyfloat(metrics_key, f"event_sum:{event_name}", event_value)

            # Квадраты значений для расчета дисперсии
            pipe.hincrbyfloat(metrics_key, f"event_sq_sum:{event_name}", event_value ** 2)

            # Количество уникальных пользователей (приблизительно)
            pipe.hincrby(metrics_key, "unique_users", 1)

            await pipe.execute()

        except Exception as e:
            logger.error(f"Failed to update metrics: {e}")

    async def get_experiment_results(self, experiment_name: str) -> Optional[Dict[str, Any]]:
        """
        Получение результатов эксперимента
        """
        try:
            config = self.experiments.get(experiment_name)
            if not config:
                return None

            results = {}

            for variant in config.variants:
                metrics_key = f"ab:metrics:{experiment_name}:{variant}"
                metrics_data = await self.redis.hgetall(metrics_key)

                if not metrics_data:
                    continue

                variant_results = {}

                # Расчет статистик для каждой метрики
                for event_name in [config.target_metric] + config.secondary_metrics:
                    event_count = int(metrics_data.get(f"event_count:{event_name}", 0))
                    event_sum = float(metrics_data.get(f"event_sum:{event_name}", 0))
                    event_sq_sum = float(metrics_data.get(f"event_sq_sum:{event_name}", 0))

                    if event_count == 0:
                        variant_results[event_name] = {
                            'count': 0,
                            'mean': 0,
                            'std': 0,
                            'confidence_interval': (0, 0)
                        }
                        continue

                    mean = event_sum / event_count
                    variance = (event_sq_sum / event_count) - (mean ** 2)
                    std = max(0, variance ** 0.5)  # Избегание отрицательных значений из-за floating point

                    # 95% доверительный интервал (приблизительно)
                    confidence_interval = (
                        mean - 1.96 * std / (event_count ** 0.5),
                        mean + 1.96 * std / (event_count ** 0.5)
                    )

                    variant_results[event_name] = {
                        'count': event_count,
                        'mean': mean,
                        'std': std,
                        'confidence_interval': confidence_interval
                    }

                results[variant] = {
                    'metrics': variant_results,
                    'sample_size': int(metrics_data.get('unique_users', 0))
                }

            # Статистический анализ
            statistical_analysis = self._perform_statistical_analysis(results, config)

            return {
                'experiment_name': experiment_name,
                'config': {
                    'target_metric': config.target_metric,
                    'variants': config.variants
                },
                'results': results,
                'statistical_analysis': statistical_analysis,
                'generated_at': time.time()
            }

        except Exception as e:
            logger.error(f"Failed to get experiment results: {e}")
            return None

    def _perform_statistical_analysis(self, results: Dict[str, Any], config: ExperimentConfig) -> Dict[str, Any]:
        """
        Выполнение статистического анализа результатов
        """
        analysis = {
            'has_significant_result': False,
            'winner': None,
            'confidence_level': config.confidence_level,
            'recommendation': 'continue_testing'
        }

        if len(results) < 2:
            return analysis

        # Сравнение с control группой
        control_variant = None
        treatment_variants = []

        for variant in config.variants:
            if 'control' in variant.lower():
                control_variant = variant
            else:
                treatment_variants.append(variant)

        if not control_variant or not treatment_variants:
            return analysis

        control_data = results.get(control_variant, {})
        if not control_data or not control_data.get('metrics'):
            return analysis

        # Анализ основной метрики
        target_metric = config.target_metric
        control_metric = control_data['metrics'].get(target_metric, {})

        if not control_metric or control_metric['count'] < config.min_sample_size:
            analysis['recommendation'] = 'insufficient_data'
            return analysis

        # Поиск лучшего варианта
        best_variant = control_variant
        best_mean = control_metric['mean']

        significant_improvements = []

        for treatment in treatment_variants:
            treatment_data = results.get(treatment, {})
            if not treatment_data or not treatment_data.get('metrics'):
                continue

            treatment_metric = treatment_data['metrics'].get(target_metric, {})

            if treatment_metric['count'] < config.min_sample_size:
                continue

            # Простой t-test (приблизительный)
            improvement = self._calculate_relative_improvement(
                control_metric, treatment_metric
            )

            # Проверка статистической значимости (упрощенная)
            is_significant = self._check_statistical_significance(
                control_metric, treatment_metric, config.confidence_level
            )

            if is_significant and treatment_metric['mean'] > best_mean:
                best_variant = treatment
                best_mean = treatment_metric['mean']
                significant_improvements.append(treatment)

        if significant_improvements:
            analysis['has_significant_result'] = True
            analysis['winner'] = best_variant
            analysis['significant_improvements'] = significant_improvements

            if best_variant != control_variant:
                analysis['recommendation'] = f'implement_{best_variant}'
            else:
                analysis['recommendation'] = 'maintain_control'

        return analysis

    def _calculate_relative_improvement(self, control: Dict, treatment: Dict) -> float:
        """
        Расчет относительного улучшения
        """
        if control['mean'] == 0:
            return 0
        return (treatment['mean'] - control['mean']) / abs(control['mean'])

    def _check_statistical_significance(self, control: Dict, treatment: Dict, confidence: float) -> bool:
        """
        Проверка статистической значимости (упрощенная версия)
        """
        # Минимальный размер выборки
        min_sample = min(control['count'], treatment['count'])
        if min_sample < 100:
            return False

        # Проверка на пересечение доверительных интервалов
        control_ci = control['confidence_interval']
        treatment_ci = treatment['confidence_interval']

        # Если интервалы не пересекаются, результат значим
        return treatment_ci[0] > control_ci[1] or control_ci[0] > treatment_ci[1]

class NewsABTesting:
    """
    A/B тестирование для новостного пайплайна
    """

    def __init__(self, ab_framework: ABTestingFramework):
        self.ab = ab_framework

        # Определение экспериментов
        self.experiments = {
            'news_filtering': ExperimentConfig(
                name='news_filtering',
                description='Testing different news filtering strategies',
                variants=['control', 'strict_filtering', 'loose_filtering'],
                traffic_distribution={'control': 0.5, 'strict_filtering': 0.25, 'loose_filtering': 0.25},
                target_metric='trade_win_rate',
                secondary_metrics=['trade_pnl', 'signal_rejection_rate'],
                min_sample_size=1000,
                confidence_level=0.95,
                duration_days=14
            ),
            'analysis_quality': ExperimentConfig(
                name='analysis_quality',
                description='Testing LLM analysis quality improvements',
                variants=['control', 'enhanced_prompts', 'multi_model'],
                traffic_distribution={'control': 0.4, 'enhanced_prompts': 0.4, 'multi_model': 0.2},
                target_metric='analysis_confidence',
                secondary_metrics=['processing_time', 'error_rate'],
                min_sample_size=5000,
                confidence_level=0.95,
                duration_days=7
            )
        }

    async def initialize_experiments(self):
        """
        Инициализация экспериментов
        """
        for experiment in self.experiments.values():
            success = await self.ab.create_experiment(experiment)
            if success:
                await self.ab.start_experiment(experiment.name)
                logger.info(f"Initialized experiment: {experiment.name}")

    def get_user_variant(self, user_id: str, experiment_name: str) -> str:
        """
        Получение варианта для пользователя
        """
        return self.ab.assign_variant(experiment_name, user_id)

    async def track_news_event(self, user_id: str, experiment_name: str,
                             event_type: str, event_data: Dict[str, Any]):
        """
        Отслеживание событий новостного пайплайна
        """
        if event_type == 'news_analyzed':
            await self.ab.track_event(
                experiment_name, user_id, 'analysis_confidence',
                event_data.get('confidence', 0.0)
            )
            await self.ab.track_event(
                experiment_name, user_id, 'processing_time',
                event_data.get('processing_time_ms', 0.0)
            )

        elif event_type == 'signal_processed':
            await self.ab.track_event(
                experiment_name, user_id, 'signal_rejection_rate',
                1.0 if event_data.get('rejected', False) else 0.0
            )

        elif event_type == 'trade_completed':
            await self.ab.track_event(
                experiment_name, user_id, 'trade_win_rate',
                1.0 if event_data.get('pnl', 0) > 0 else 0.0
            )
            await self.ab.track_event(
                experiment_name, user_id, 'trade_pnl',
                event_data.get('pnl', 0.0)
            )

    async def get_experiment_summary(self) -> Dict[str, Any]:
        """
        Получение сводки по всем экспериментам
        """
        summary = {}

        for experiment_name in self.experiments.keys():
            results = await self.ab.get_experiment_results(experiment_name)
            if results:
                summary[experiment_name] = {
                    'status': 'completed' if results.get('statistical_analysis', {}).get('has_significant_result') else 'running',
                    'winner': results.get('statistical_analysis', {}).get('winner'),
                    'recommendation': results.get('statistical_analysis', {}).get('recommendation'),
                    'variants_tested': len(results.get('results', {})),
                    'total_sample_size': sum(v.get('sample_size', 0) for v in results.get('results', {}).values())
                }

        return summary

async def demo_ab_testing():
    """
    Демонстрация A/B тестирования
    """
    print("=== News Pipeline A/B Testing Demo ===\n")

    # Создание фреймворка
    ab_framework = ABTestingFramework("redis://localhost:6379")
    news_ab = NewsABTesting(ab_framework)

    # Инициализация экспериментов
    await news_ab.initialize_experiments()

    print("Initialized experiments:")
    for name, config in news_ab.experiments.items():
        print(f"  - {name}: {config.description}")
        print(f"    Variants: {config.variants}")
        print(f"    Target metric: {config.target_metric}")
    print()

    # Симуляция пользовательских взаимодействий
    print("Simulating user interactions...")

    for user_id in range(1, 1001):  # 1000 пользователей
        user_id_str = f"user_{user_id:04d}"

        # Определение варианта для каждого эксперимента
        filtering_variant = news_ab.get_user_variant(user_id_str, 'news_filtering')
        quality_variant = news_ab.get_user_variant(user_id_str, 'analysis_quality')

        print(f"User {user_id_str}: filtering={filtering_variant}, quality={quality_variant}")

        # Симуляция анализа новостей
        confidence_score = random.gauss(0.75, 0.15)  # Нормальное распределение
        processing_time = random.gauss(2000, 500)    # Нормальное распределение

        await news_ab.track_news_event(
            user_id_str, 'analysis_quality', 'news_analyzed',
            {'confidence': confidence_score, 'processing_time_ms': processing_time}
        )

        # Симуляция обработки сигналов (70% принимаются)
        signal_accepted = random.random() < 0.7
        await news_ab.track_news_event(
            user_id_str, 'news_filtering', 'signal_processed',
            {'rejected': not signal_accepted}
        )

        # Симуляция результатов торговли (55% прибыльных сделок)
        if random.random() < 0.6:  # 60% пользователей торгуют
            pnl = random.gauss(50, 200) if random.random() < 0.55 else random.gauss(-50, 150)
            await news_ab.track_news_event(
                user_id_str, 'news_filtering', 'trade_completed',
                {'pnl': pnl}
            )

    print("\nGenerating experiment results...")

    # Получение результатов
    for experiment_name in news_ab.experiments.keys():
        results = await ab_framework.get_experiment_results(experiment_name)

        if results:
            print(f"\nExperiment: {experiment_name}")
            print(f"Status: {results['statistical_analysis']['recommendation']}")

            for variant, data in results['results'].items():
                target_metric = news_ab.experiments[experiment_name].target_metric
                metric_data = data['metrics'].get(target_metric, {})
                print(f"  {variant}: n={data['sample_size']}, "
                      f"mean={metric_data.get('mean', 0):.3f}")

            winner = results['statistical_analysis'].get('winner')
            if winner:
                print(f"  Winner: {winner}")

    # Общая сводка
    summary = await news_ab.get_experiment_summary()
    print("
Experiment Summary:")
    for exp_name, exp_summary in summary.items():
        print(f"  {exp_name}: {exp_summary['status']} - {exp_summary['recommendation']}")

if __name__ == "__main__":
    asyncio.run(demo_ab_testing())
```

## Заключение

Эти примеры демонстрируют:

1. **Полную интеграцию новостей в торговую стратегию** - от сбора данных до принятия решений
2. **Анализ качества новостного анализа** - с использованием метрик и улучшением результатов
3. **Комплексный мониторинг** - здоровье компонентов, метрики производительности, алерты
4. **A/B тестирование** - для оценки эффективности различных подходов

Каждый пример включает:
- Полный код реализации
- Конфигурацию
- Обработку ошибок
- Метрики и мониторинг
- Лучшие практики

Эти примеры можно использовать как основу для создания production-ready системы новостного анализа в торговых приложениях.
