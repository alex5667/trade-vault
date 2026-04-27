# LLM Анализ Новостей

## Обзор

Система LLM анализа новостей использует Google Gemini для автоматической оценки важности, категоризации и анализа эмоциональной окраски новостей. Анализатор преобразует сырые новости в структурированные данные для использования в торговых сигналах.

## Современная Архитектура Анализа

### Компоненты Системы

#### 1. Multi-Provider LLM Client Architecture
```python
from abc import ABC, abstractmethod
from typing import Dict, Any, Optional, List
from dataclasses import dataclass
from enum import Enum
import asyncio
import aiohttp
import json
import time
import logging

logger = logging.getLogger(__name__)

class LLMProvider(Enum):
    GEMINI = "gemini"
    GPT = "gpt"
    CLAUDE = "claude"
    LOCAL = "local"

@dataclass
class LLMConfig:
    """Конфигурация LLM клиента"""
    provider: LLMProvider
    model_name: str
    api_key: str
    base_url: str = ""
    timeout_sec: float = 30.0
    max_retries: int = 3
    temperature: float = 0.2
    max_tokens: int = 512
    rate_limit_rpm: int = 60
    cache_enabled: bool = True
    cache_ttl_sec: int = 3600

class LLMClient(ABC):
    """Абстрактный базовый класс для LLM клиентов"""

    def __init__(self, config: LLMConfig):
        self.config = config
        self._rate_limiter = RateLimiter(config.rate_limit_rpm)
        self._cache = AnalysisCache() if config.cache_enabled else None
        self._session: Optional[aiohttp.ClientSession] = None

    async def __aenter__(self):
        self._session = aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=self.config.timeout_sec)
        )
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self._session:
            await self._session.close()

    @abstractmethod
    async def analyze(self, request: AnalysisRequest) -> AnalysisResponse:
        """Анализ одной новости"""
        pass

    @abstractmethod
    async def analyze_batch(self, requests: List[AnalysisRequest]) -> List[AnalysisResponse]:
        """Пакетный анализ новостей"""
        pass

    @abstractmethod
    def get_model_info(self) -> Dict[str, Any]:
        """Информация о модели"""
        pass
```

#### 2. Distributed Analysis Worker
```python
class DistributedNewsAnalyzer:
    """
    Распределенный анализатор новостей с поддержкой множественных моделей
    """

    def __init__(self, redis_url: str, config: Dict[str, Any]):
        self.redis = redis.from_url(redis_url)
        self.config = config

        # Инициализация LLM клиентов
        self.llm_clients = self._initialize_llm_clients()

        # Компоненты обработки
        self.batch_processor = BatchProcessor(config.get('batch_size', 10))
        self.quality_assurance = QualityAssuranceEngine()
        self.fallback_handler = FallbackHandler()

        # Метрики и мониторинг
        self.metrics = AnalysisMetricsCollector(self.redis)
        self.health_monitor = HealthMonitor()

    def _initialize_llm_clients(self) -> Dict[str, LLMClient]:
        """Инициализация клиентов различных LLM провайдеров"""
        clients = {}

        # Gemini (primary)
        if self.config.get('gemini_enabled', True):
            clients['gemini'] = GeminiClient(LLMConfig(
                provider=LLMProvider.GEMINI,
                model_name=self.config.get('gemini_model', 'gemini-1.5-pro'),
                api_key=os.getenv('GEMINI_API_KEY'),
                temperature=self.config.get('temperature', 0.2)
            ))

        # GPT-4 (backup)
        if self.config.get('gpt4_enabled', False):
            clients['gpt4'] = GPTClient(LLMConfig(
                provider=LLMProvider.GPT,
                model_name='gpt-4',
                api_key=os.getenv('OPENAI_API_KEY')
            ))

        # Claude (tertiary)
        if self.config.get('claude_enabled', False):
            clients['claude'] = ClaudeClient(LLMConfig(
                provider=LLMProvider.CLAUDE,
                model_name='claude-3-opus-20240229',
                api_key=os.getenv('ANTHROPIC_API_KEY')
            ))

        return clients

    async def process_news_stream(self):
        """Основной цикл обработки новостей"""
        logger.info("Starting distributed news analysis")

        while True:
            try:
                # Получение пачки новостей
                news_batch = await self._fetch_news_batch()

                if not news_batch:
                    await asyncio.sleep(1)
                    continue

                # Параллельный анализ
                analysis_results = await self._analyze_batch_parallel(news_batch)

                # Качество и постобработка
                processed_results = await self._post_process_results(analysis_results)

                # Сохранение результатов
                await self._save_results(processed_results)

                # Обновление метрик
                await self.metrics.record_batch_processing(
                    batch_size=len(news_batch),
                    success_count=len(processed_results),
                    processing_time=time.time()
                )

            except Exception as e:
                logger.error(f"Error in news processing: {e}")
                await asyncio.sleep(5)

    async def _analyze_batch_parallel(self, news_batch: List[AnalysisRequest]) -> List[AnalysisResponse]:
        """Параллельный анализ пачки новостей"""
        # Распределение по моделям
        model_assignments = self._distribute_workload(news_batch)

        # Параллельное выполнение
        tasks = []
        for model_name, requests in model_assignments.items():
            if model_name in self.llm_clients:
                client = self.llm_clients[model_name]
                task = asyncio.create_task(client.analyze_batch(requests))
                tasks.append(task)

        # Сбор результатов
        results = await asyncio.gather(*tasks, return_exceptions=True)

        # Объединение результатов
        all_results = []
        for result in results:
            if isinstance(result, Exception):
                logger.error(f"Analysis task failed: {result}")
                continue
            all_results.extend(result)

        return all_results

    def _distribute_workload(self, requests: List[AnalysisRequest]) -> Dict[str, List[AnalysisRequest]]:
        """Распределение работы по моделям"""
        distribution = {}

        for request in requests:
            # Выбор модели на основе типа новости
            model = self._select_model_for_news(request)
            if model not in distribution:
                distribution[model] = []
            distribution[model].append(request)

        return distribution

    def _select_model_for_news(self, request: AnalysisRequest) -> str:
        """Выбор оптимальной модели для новости"""
        # Логика выбора модели на основе характеристик новости
        title_length = len(request.title)
        has_complex_terms = any(term in request.title.lower() for term in
                              ['federal reserve', 'monetary policy', 'quantitative easing'])

        if has_complex_terms or title_length > 100:
            return 'claude'  # Лучше для сложных текстов
        elif title_length > 50:
            return 'gpt4'    # Хороший баланс
        else:
            return 'gemini'  # Быстрый для простых текстов
```

#### 3. Quality Assurance & Fallback System
```python
class QualityAssuranceEngine:
    """
    Система контроля качества анализа
    """

    def __init__(self):
        self.quality_thresholds = {
            'min_confidence': 0.6,
            'max_reanalysis_attempts': 2,
            'fallback_enabled': True
        }

    async def validate_and_improve(self, results: List[AnalysisResponse]) -> List[AnalysisResponse]:
        """Валидация и улучшение результатов анализа"""
        validated_results = []

        for result in results:
            # Оценка качества
            quality_score = await self._assess_quality(result)

            if quality_score < self.quality_thresholds['min_confidence']:
                # Попытка улучшения
                improved_result = await self._attempt_improvement(result)
                if improved_result:
                    validated_results.append(improved_result)
                else:
                    # Fallback к базовому анализу
                    fallback_result = self._create_fallback_result(result)
                    validated_results.append(fallback_result)
            else:
                validated_results.append(result)

        return validated_results

    async def _assess_quality(self, result: AnalysisResponse) -> float:
        """Оценка качества анализа"""
        score = result.confidence_score

        # Дополнительные факторы качества
        if result.summary and len(result.summary) > 20:
            score += 0.1  # Хороший summary

        if len(result.tags) >= 1:
            score += 0.1  # Есть теги

        if abs(result.surprise_score) > 0.1:
            score += 0.05  # Разумная surprise

        return min(1.0, score)

    async def _attempt_improvement(self, result: AnalysisResponse) -> Optional[AnalysisResponse]:
        """Попытка улучшения анализа низкого качества"""
        # Здесь можно использовать другую модель или улучшенный промпт
        # Для демонстрации возвращаем None (не удалось улучшить)
        return None

    def _create_fallback_result(self, original: AnalysisResponse) -> AnalysisResponse:
        """Создание fallback результата"""
        return AnalysisResponse(
            uid=original.uid,
            risk_score=0.0,
            surprise_score=0.0,
            confidence_score=0.0,
            tags=[],
            primary_tag="",
            summary=f"Analysis failed for {original.uid[:16]}...",
            processing_time_ms=original.processing_time_ms,
            model_used=f"{original.model_used}(fallback)",
            tokens_used=0
        )
```

## Prompt Engineering

### Основной Prompt
```python
prompt = (
    "Return ONLY compact JSON object with keys:\n"
    "risk (0..1 float), surprise (-1..1 float), confidence (0..1 float),\n"
    "tags (array of strings from allowed set),\n"
    "primary_tag (string from same set), summary (<=160 chars).\n\n"
    f"allowed_tags={sorted(self.allowed_tags)}\n"
    f"source={source}\nurl={url}\ntitle={title}\n"
)
```

### Ожидаемый JSON Ответ
```json
{
  "risk": 0.85,
  "surprise": 0.3,
  "confidence": 0.92,
  "tags": ["fomc", "rates", "inflation"],
  "primary_tag": "fomc",
  "summary": "Fed signals potential rate hike due to inflation concerns"
}
```

## Детальные Метрики Анализа

### Risk Score (0.0-1.0) - Уровень Риска
Комплексная оценка потенциального влияния новости на финансовые рынки:

#### Шкала Оценки Риска
- **0.0-0.1**: Незначительное влияние (локальные новости, мелкие компании)
- **0.1-0.2**: Минимальное влияние (корпоративные новости средней компании)
- **0.2-0.3**: Низкое влияние (отчеты компаний, отраслевые новости)
- **0.3-0.4**: Низкое-среднее влияние (секторные новости, регуляторные изменения)
- **0.4-0.5**: Среднее влияние (важные корпоративные события, экономические данные)
- **0.5-0.6**: Среднее-высокое влияние (макроэкономические новости, M&A)
- **0.6-0.7**: Высокое влияние (FOMC заявления, крупные экономические релизы)
- **0.7-0.8**: Очень высокое влияние (неожиданные изменения политики, кризисы)
- **0.8-0.9**: Критическое влияние (глобальные события, войны, катастрофы)
- **0.9-1.0**: Экстремальное влияние (системные кризисы, рецессии)

#### Факторы Влияния на Risk Score
```python
def calculate_risk_factors(title: str, source: str, content: str = "") -> Dict[str, float]:
    """
    Расчет факторов влияния на risk score
    """
    factors = {
        'source_credibility': 0.0,
        'content_severity': 0.0,
        'market_impact': 0.0,
        'timing_sensitivity': 0.0,
        'geographic_scope': 0.0
    }

    # Source credibility (0-1)
    source_weights = {
        'reuters': 0.95, 'bloomberg': 0.93, 'wsj': 0.92,
        'cnbc': 0.85, 'coindesk': 0.80, 'cryptopanic': 0.75
    }
    factors['source_credibility'] = source_weights.get(source.lower(), 0.5)

    # Content severity based on keywords
    severity_keywords = {
        'crisis': 0.9, 'crash': 0.9, 'recession': 0.9, 'bankruptcy': 0.8,
        'war': 0.9, 'sanctions': 0.8, 'disaster': 0.7, 'scandal': 0.7,
        'fed': 0.8, 'ecb': 0.8, 'cpi': 0.8, 'gdp': 0.8, 'unemployment': 0.8,
        'merger': 0.6, 'acquisition': 0.6, 'earnings': 0.5, 'dividend': 0.4
    }

    text = (title + " " + content).lower()
    max_severity = max([severity_keywords.get(word, 0.0)
                       for word in severity_keywords.keys()
                       if word in text], default=0.0)
    factors['content_severity'] = max_severity

    # Market impact based on affected assets
    asset_keywords = {
        'global': 0.9, 'world': 0.8, 'international': 0.7,
        'crypto': 0.6, 'bitcoin': 0.7, 'ethereum': 0.6,
        'stock': 0.5, 'equity': 0.5, 'bond': 0.6, 'commodity': 0.5,
        'forex': 0.6, 'currency': 0.5, 'dollar': 0.7, 'euro': 0.6
    }

    max_impact = max([asset_keywords.get(word, 0.0)
                      for word in asset_keywords.keys()
                      if word in text], default=0.3)
    factors['market_impact'] = max_impact

    # Timing sensitivity
    timing_indicators = ['breaking', 'urgent', 'emergency', 'immediate', 'flash']
    factors['timing_sensitivity'] = 0.8 if any(word in text for word in timing_indicators) else 0.2

    # Geographic scope
    global_indicators = ['global', 'worldwide', 'international', 'united states', 'europe', 'china', 'russia']
    factors['geographic_scope'] = 0.9 if any(word in text for word in global_indicators) else 0.3

    return factors

def compute_risk_score(factors: Dict[str, float]) -> float:
    """
    Вычисление итогового risk score
    """
    # Weighted combination
    weights = {
        'source_credibility': 0.1,
        'content_severity': 0.4,
        'market_impact': 0.3,
        'timing_sensitivity': 0.1,
        'geographic_scope': 0.1
    }

    score = sum(factors[key] * weights[key] for key in factors.keys())

    # Apply non-linear scaling for extreme events
    if score > 0.7:
        score = 0.7 + (score - 0.7) * 1.5  # Boost high-risk events

    return min(1.0, max(0.0, score))
```

### Surprise Score (-1.0..1.0) - Фактор Неожиданности
Оценка степени неожиданности события относительно рыночных ожиданий:

#### Шкала Surprise Score
- **-1.0..-0.8**: Крайне ожидаемое негативное событие (предсказуемый кризис)
- **-0.8..-0.6**: Очень ожидаемое негативное событие (ожидаемая рецессия)
- **-0.6..-0.4**: Ожидаемое негативное событие (прогнозируемый спад)
- **-0.4..-0.2**: Скорее ожидаемое негативное событие
- **-0.2..-0.1**: Немного негативное отклонение от ожиданий
- **-0.1..0.1**: Нейтральное/ожидаемое событие (в пределах консенсуса)
- **0.1..0.2**: Немного позитивное отклонение от ожиданий
- **0.2..0.4**: Скорее неожиданное позитивное событие
- **0.4..0.6**: Неожиданное позитивное событие (лучше ожиданий)
- **0.6..0.8**: Очень неожиданное позитивное событие
- **0.8..1.0**: Крайне неожиданное позитивное событие (сюрприз)

#### Surprise Factors
```python
def analyze_surprise_factors(title: str, content: str = "") -> Dict[str, float]:
    """
    Анализ факторов неожиданности
    """
    text = (title + " " + content).lower()

    surprise_indicators = {
        # Positive surprise
        'beat': 0.6, 'exceed': 0.5, 'surprise': 0.7, 'unexpected': 0.6,
        'better than expected': 0.8, 'above forecast': 0.7, 'stronger': 0.4,
        'boost': 0.3, 'rally': 0.4, 'jump': 0.5, 'surge': 0.6,

        # Negative surprise
        'miss': -0.6, 'disappoint': -0.5, 'worse than expected': -0.8,
        'below forecast': -0.7, 'weaker': -0.4, 'decline': -0.3,
        'slump': -0.5, 'plunge': -0.6, 'crash': -0.8, 'crisis': -0.7
    }

    # Find surprise words
    found_indicators = []
    for phrase, score in surprise_indicators.items():
        if phrase in text:
            found_indicators.append((phrase, score))

    if not found_indicators:
        # No explicit surprise indicators - neutral
        return {'base_surprise': 0.0, 'context_modifier': 0.0, 'final_score': 0.0}

    # Take the strongest indicator
    strongest_indicator = max(found_indicators, key=lambda x: abs(x[1]))

    base_surprise = strongest_indicator[1]

    # Context modifiers
    context_modifier = 0.0

    # Intensifiers
    intensifiers = ['dramatically', 'sharply', 'significantly', 'massive', 'huge']
    if any(word in text for word in intensifiers):
        context_modifier += 0.2 * (1 if base_surprise > 0 else -1)

    # Timing context
    timing_words = ['sudden', 'abrupt', 'overnight', 'immediate']
    if any(word in text for word in timing_words):
        context_modifier += 0.1 * (1 if base_surprise > 0 else -1)

    # Market reaction words
    reaction_words = ['shock', 'stun', 'rock', 'shake', 'surprise']
    if any(word in text for word in reaction_words):
        context_modifier += 0.3 * (1 if base_surprise > 0 else -1)

    final_score = base_surprise + context_modifier
    final_score = max(-1.0, min(1.0, final_score))

    return {
        'base_surprise': base_surprise,
        'context_modifier': context_modifier,
        'final_score': final_score,
        'indicator': strongest_indicator[0]
    }
```

### Confidence Score (0-1)
Уверенность модели в анализе:

- **0.0-0.3**: Низкая уверенность (неоднозначный контент)
- **0.3-0.7**: Средняя уверенность (стандартный анализ)
- **0.7-1.0**: Высокая уверенность (ясный контент)

## Система Тегов

### Битовая Маска Тегов
```python
TAG_BITS = {
    "cpi": 0,          # Consumer Price Index
    "ppi": 1,          # Producer Price Index
    "fomc": 2,         # Federal Open Market Committee
    "fed_speech": 3,   # Федеральная резервная система
    "nfp": 4,          # Non-Farm Payrolls
    "rates": 5,        # Процентные ставки
    "inflation": 6,    # Инфляция
    "risk_off": 7,     # Риск-офф события
    "risk_on": 8,      # Риск-он события
    "earnings": 9,     # Корпоративные отчеты
    "geopolitics": 10, # Геополитика
    "crypto_reg": 11,  # Крипто-регулирование
    "exchange": 12,    # Биржевые инциденты
    "hack": 13,        # Кибератаки
    "etf": 14,         # ETF новости
    "liquidation": 15, # Массовые ликвидации
    "macro": 16,       # Макроэкономика
}
```

### Primary Tag IDs
```python
PRIMARY_TAG_ID = {
    "cpi": 1, "ppi": 2, "fomc": 3, "nfp": 4, "rates": 5,
    "geopolitics": 6, "hack": 7, "etf": 8, "crypto_reg": 9,
    "exchange": 10, "macro": 11, "inflation": 12,
    "risk_off": 13, "risk_on": 14, "earnings": 15, "liquidation": 16
}
```

### Преобразование Тегов
```python
def tags_to_mask(tags: Iterable[str]) -> int:
    """Преобразует список тегов в битовую маску"""
    mask = 0
    for tag in tags:
        tag = tag.strip().lower()
        bit = TAG_BITS.get(tag)
        if bit is not None and 0 <= bit < 63:
            mask |= (1 << bit)
    return mask

def pick_primary_tag(tags: Iterable[str]) -> int:
    """Выбирает основной тег по приоритету"""
    best = 0
    for tag in tags:
        tag = tag.strip().lower()
        tag_id = PRIMARY_TAG_ID.get(tag, 0)
        if tag_id and (best == 0 or tag_id < best):
            best = tag_id
    return best
```

## Обработка Ошибок

### Fail-Open Стратегия
```python
def analyze(self, *, title: str, url: str, source: str) -> Dict[str, Any]:
    if not self.api_key:
        # Fail-open: возвращаем нейтральный анализ
        return {
            "risk": 0.0,
            "surprise": 0.0,
            "tags": [],
            "primary_tag": "",
            "confidence": 0.0,
            "summary": ""
        }

    # Попытка LLM анализа с повторными попытками
    for attempt in range(self.max_retries + 1):
        try:
            return self._call_gemini(title, url, source)
        except Exception as e:
            if attempt == self.max_retries:
                # При неудаче возвращаем fallback
                return {
                    "risk": 0.0,
                    "surprise": 0.0,
                    "tags": [],
                    "primary_tag": "",
                    "confidence": 0.0,
                    "summary": f"analysis_failed: {str(e)[:100]}"
                }
            # Exponential backoff
            time.sleep((0.4 * (2 ** attempt)) * (0.7 + 0.6 * random.random()))
```

### Rate Limiting
```python
class _TokenBucket:
    """In-process rate limiter"""

    def __init__(self, rpm: float):
        self.capacity = max(1.0, float(rpm))
        self.tokens = self.capacity
        self.fill_rate = self.capacity / 60.0  # tokens per second
        self.last = time.monotonic()
        self._lock = threading.Lock()

    def acquire(self) -> None:
        with self.lock:
            now = time.monotonic()
            elapsed = now - self.last
            self.last = now

            self.tokens = min(self.capacity, self.tokens + elapsed * self.fill_rate)
            if self.tokens >= 1.0:
                self.tokens -= 1.0
                return

            # Wait for tokens
            need = (1.0 - self.tokens) / max(1e-6, self.fill_rate)
            time.sleep(need)
```

## Gemini API Интеграция

### HTTP Клиент
```python
def _call_gemini(self, title: str, url: str, source: str) -> Dict[str, Any]:
    endpoint = f"https://generativelanguage.googleapis.com/v1beta/models/{self.model}:generateContent"

    prompt = self._build_prompt(title, url, source)

    body = json.dumps({
        "contents": [{"parts": [{"text": prompt}]}],
        "generationConfig": {
            "temperature": self.temperature,
            "maxOutputTokens": self.max_tokens
        }
    }).encode("utf-8")

    req = urllib.request.Request(
        url=endpoint,
        data=body,
        headers={
            "Content-Type": "application/json",
            "x-goog-api-key": self.api_key
        },
        method="POST"
    )

    with urllib.request.urlopen(req, timeout=self.timeout_sec) as resp:
        data = json.loads(resp.read().decode("utf-8", errors="replace"))

    # Extract response
    text = data["candidates"][0]["content"]["parts"][0]["text"]
    return self._parse_response(text)
```

### Парсинг Ответа
```python
def _extract_json_obj(text: str) -> Optional[dict]:
    """Извлекает JSON объект из ответа LLM"""
    text = (text or "").strip()
    if not text:
        return None

    # Прямой JSON
    try:
        return json.loads(text)
    except Exception:
        pass

    # Поиск JSON внутри текста
    i = text.find("{")
    j = text.rfind("}")
    if i >= 0 and j > i:
        frag = text[i : j + 1]
        try:
            return json.loads(frag)
        except Exception:
            pass

    return None

def _parse_response(self, text: str) -> Dict[str, Any]:
    """Парсит и валидирует ответ LLM"""
    obj = _extract_json_obj(text) or {}

    # Извлечение и валидация полей
    risk = _clamp01(float(obj.get("risk", 0.0) or 0.0))
    surprise = _clamp(float(obj.get("surprise", 0.0) or 0.0), -1.0, 1.0)
    confidence = _clamp01(float(obj.get("confidence", 0.0) or 0.0))

    # Валидация тегов
    tags = obj.get("tags") or []
    if not isinstance(tags, list):
        tags = []
    tags = [t for t in tags if isinstance(t, str) and t in self.allowed_tags]

    primary_tag = obj.get("primary_tag") or ""
    if not isinstance(primary_tag, str) or primary_tag not in self.allowed_tags:
        primary_tag = ""

    summary = str(obj.get("summary") or "")[:200]

    return {
        "risk": risk,
        "surprise": surprise,
        "confidence": confidence,
        "tags": tags,
        "primary_tag": primary_tag,
        "summary": summary,
    }
```

## Конфигурация

### Environment Variables
```bash
# Основные настройки
GEMINI_API_KEY=your_gemini_api_key
GEMINI_MODEL=gemini-1.5-pro

# Производительность
GEMINI_TIMEOUT_SEC=10
GEMINI_RETRIES=2
GEMINI_TEMPERATURE=0.2
GEMINI_MAX_TOKENS=256

# Rate limiting (RPM)
GEMINI_RPM=60
```

### Модельные Параметры

| Параметр | Диапазон | Описание | Рекомендация |
|----------|----------|----------|--------------|
| `temperature` | 0.0-2.0 | Креативность ответа | 0.2 (низкая для консистентности) |
| `max_tokens` | 1-8192 | Максимальная длина ответа | 256 (достаточно для JSON) |
| `timeout_sec` | 1-60 | Таймаут запроса | 10 сек |
| `retries` | 0-5 | Количество повторных попыток | 2 |

## Качество и Точность

### Метрики Качества
```python
def calculate_analysis_quality(analysis: Dict[str, Any]) -> float:
    """Оценивает качество анализа на основе confidence и consistency"""
    confidence = analysis.get("confidence", 0.0)
    tags_count = len(analysis.get("tags", []))
    has_primary = bool(analysis.get("primary_tag"))
    summary_len = len(analysis.get("summary", ""))

    # Quality score: confidence + теги + первичный тег + длина summary
    quality = confidence
    quality += min(tags_count * 0.1, 0.5)  # До 0.5 за теги
    quality += 0.2 if has_primary else 0    # 0.2 за первичный тег
    quality += min(summary_len / 160.0 * 0.3, 0.3)  # До 0.3 за summary

    return min(quality, 1.0)
```

### Валидация Анализа
```python
def validate_analysis(analysis: Dict[str, Any]) -> List[str]:
    """Валидирует корректность анализа"""
    errors = []

    risk = analysis.get("risk", 0.0)
    if not (0.0 <= risk <= 1.0):
        errors.append(f"Invalid risk score: {risk}")

    surprise = analysis.get("surprise", 0.0)
    if not (-1.0 <= surprise <= 1.0):
        errors.append(f"Invalid surprise score: {surprise}")

    confidence = analysis.get("confidence", 0.0)
    if not (0.0 <= confidence <= 1.0):
        errors.append(f"Invalid confidence score: {confidence}")

    tags = analysis.get("tags", [])
    if not isinstance(tags, list):
        errors.append("Tags must be a list")
    else:
        for tag in tags:
            if tag not in ALLOWED_TAGS:
                errors.append(f"Unknown tag: {tag}")

    primary_tag = analysis.get("primary_tag", "")
    if primary_tag and primary_tag not in ALLOWED_TAGS:
        errors.append(f"Unknown primary tag: {primary_tag}")

    summary = analysis.get("summary", "")
    if len(summary) > 200:
        errors.append(f"Summary too long: {len(summary)} chars")

    return errors
```

## Оптимизация Производительности

### Batch Processing
```python
async def analyze_batch(self, news_items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Анализ нескольких новостей в одном запросе"""
    if not news_items:
        return []

    # Группировка новостей для batch запроса
    batch_prompt = self._build_batch_prompt(news_items)

    # Gemini поддерживает batch через contents array
    contents = [{"parts": [{"text": batch_prompt}]}]

    # ... API call ...

    # Парсинг batch ответа
    return self._parse_batch_response(response)
```

### Кеширование
```python
class AnalysisCache:
    """Кеш для избежания повторного анализа похожих новостей"""

    def __init__(self, redis: redis.Redis, ttl_sec: int = 3600):
        self.redis = redis
        self.ttl_sec = ttl_sec

    def get_cached_analysis(self, title_hash: str) -> Optional[Dict[str, Any]]:
        """Получить кешированный анализ"""
        key = f"analysis:cache:{title_hash}"
        cached = self.redis.get(key)
        if cached:
            return json.loads(cached)
        return None

    def cache_analysis(self, title_hash: str, analysis: Dict[str, Any]):
        """Сохранить анализ в кеш"""
        key = f"analysis:cache:{title_hash}"
        self.redis.setex(key, self.ttl_sec, json.dumps(analysis))
```

### Асинхронная Обработка
```python
import asyncio
import aiohttp

class AsyncGeminiClient:
    """Асинхронный клиент для параллельного анализа"""

    async def analyze_multiple(self, news_items: List[Dict]) -> List[Dict]:
        """Параллельный анализ нескольких новостей"""
        semaphore = asyncio.Semaphore(10)  # Ограничение concurrency

        async def analyze_one(item):
            async with semaphore:
                return await self._analyze_single(item)

        tasks = [analyze_one(item) for item in news_items]
        return await asyncio.gather(*tasks, return_exceptions=True)
```

## Мониторинг и Отладка

### Метрики Анализа
```python
# Prometheus метрики
ANALYSIS_REQUESTS = Counter('news_analysis_requests_total', 'Total analysis requests')
ANALYSIS_LATENCY = Histogram('news_analysis_latency_seconds', 'Analysis latency')
ANALYSIS_ERRORS = Counter('news_analysis_errors_total', 'Analysis errors by type')

# Кастомные метрики
RISK_DISTRIBUTION = Histogram('news_risk_score', 'Distribution of risk scores')
CONFIDENCE_DISTRIBUTION = Histogram('news_confidence_score', 'Distribution of confidence scores')
TAG_USAGE = Counter('news_tag_usage', 'Usage of specific tags', ['tag'])
```

### Логирование
```python
def log_analysis(analysis: Dict[str, Any], processing_time: float):
    """Структурированное логирование анализа"""
    log.info("news_analyzed", {
        "risk": analysis["risk"],
        "surprise": analysis["surprise"],
        "confidence": analysis["confidence"],
        "tags": analysis["tags"],
        "primary_tag": analysis["primary_tag"],
        "processing_time_ms": processing_time,
        "summary_length": len(analysis["summary"])
    })
```

### Отладка Проблем
```python
def debug_analysis_failure(title: str, error: Exception) -> Dict[str, Any]:
    """Отладочная информация при неудачном анализе"""
    return {
        "title": title,
        "error_type": type(error).__name__,
        "error_message": str(error),
        "title_length": len(title),
        "has_special_chars": bool(re.search(r'[^\w\s]', title)),
        "word_count": len(title.split()),
        "timestamp": time.time()
    }
```

## Примеры Анализа

### Высокорискованная Новость
```python
# Вход: "Fed Unexpectedly Raises Interest Rates by 0.75%"
analysis = {
    "risk": 0.95,
    "surprise": 0.8,
    "confidence": 0.88,
    "tags": ["fomc", "rates", "inflation"],
    "primary_tag": "fomc",
    "summary": "Federal Reserve unexpectedly increases rates by 75bps"
}
```

### Низкорискованная Новость
```python
# Вход: "Apple Releases New iPhone Color Option"
analysis = {
    "risk": 0.15,
    "surprise": -0.2,
    "confidence": 0.92,
    "tags": ["earnings"],
    "primary_tag": "earnings",
    "summary": "Apple introduces new color variant for iPhone"
}
```

### Геополитическая Новость
```python
# Вход: "US and China Reach Trade Agreement"
analysis = {
    "risk": 0.75,
    "surprise": 0.4,
    "confidence": 0.85,
    "tags": ["geopolitics", "macro"],
    "primary_tag": "geopolitics",
    "summary": "Major trade agreement reached between US and China"
}
```

## Безопасность

### API Key Защита
```python
# Никогда не логировать API ключи
log.info("API call completed")  # OK
log.info(f"Using key: {api_key}")  # НЕ ДЕЛАТЬ!

# Использовать environment variables
api_key = os.getenv("GEMINI_API_KEY")
if not api_key:
    raise ValueError("GEMINI_API_KEY not set")
```

### Input Sanitization
```python
def sanitize_input(text: str) -> str:
    """Очистка входных данных"""
    # Удаление потенциально опасных символов
    text = re.sub(r'[^\w\s.,!?-]', '', text)
    # Ограничение длины
    return text[:1000]
```

### Rate Limiting Защита
```python
# Защита от abuse
class AbuseProtection:
    def __init__(self):
        self.requests_per_ip = defaultdict(lambda: deque(maxlen=100))

    def is_allowed(self, ip: str) -> bool:
        now = time.time()
        requests = self.requests_per_ip[ip]

        # Очистка старых запросов (старше 1 минуты)
        while requests and now - requests[0] > 60:
            requests.popleft()

        # Проверка лимита
        if len(requests) >= 30:  # 30 запросов в минуту
            return False

        requests.append(now)
        return True
```

## Расширение Системы

### Добавление Новых Тегов
```python
# 1. Добавить в TAG_BITS
TAG_BITS["yield_curve"] = 17
TAG_BITS["central_bank"] = 18

# 2. Добавить в PRIMARY_TAG_ID (если нужно)
PRIMARY_TAG_ID["yield_curve"] = 17

# 3. Обновить allowed_tags в LLM клиенте
self.allowed_tags.update(["yield_curve", "central_bank"])

# 4. Переобучить/обновить промпт если необходимо
```

### Кастомные Модели
```python
class CustomNewsAnalyzer(LLMClient):
    """Кастомный анализатор для специфических доменов"""

    def analyze(self, *, title: str, url: str, source: str) -> Dict[str, Any]:
        # Специфическая логика для крипто-новостей
        if "crypto" in title.lower():
            return self._analyze_crypto_news(title, url, source)

        # Специфическая логика для макро-новостей
        if any(word in title.lower() for word in ["fed", "ecb", "boj"]):
            return self._analyze_central_bank_news(title, url, source)

        # Fallback to base analysis
        return super().analyze(title=title, url=url, source=source)
```

### Мульти-модель Анализ
```python
class EnsembleAnalyzer:
    """Ансамбль разных моделей для повышения точности"""

    def __init__(self):
        self.gemini = GeminiHTTPClient()
        self.claude = ClaudeClient()  # Будущая модель
        self.gpt = GPTClient()       # Будущая модель

    def analyze(self, title: str, url: str, source: str) -> Dict[str, Any]:
        # Получить анализы от всех моделей
        analyses = []
        for model in [self.gemini, self.claude, self.gpt]:
            try:
                analysis = model.analyze(title, url, source)
                analyses.append(analysis)
            except Exception as e:
                log.warning(f"Model {model.__class__.__name__} failed: {e}")

        # Агрегировать результаты
        return self._ensemble_vote(analyses)
```

## Производительность и Масштабирование

### Бенчмарки

| Метрика | Значение | Комментарий |
|---------|----------|-------------|
| Latency (P50) | 2.1 сек | Среднее время анализа |
| Latency (P95) | 4.8 сек | 95-й перцентиль |
| Throughput | 25 новостей/мин | На одном инстансе |
| Error Rate | 0.5% | Доля неудачных анализов |
| CPU Usage | 15% | При 20 новостях/мин |

### Оптимизации

1. **Connection Pooling**: Переиспользование HTTP соединений
2. **Request Batching**: Группировка запросов к API
3. **Response Caching**: Кеш для похожих новостей
4. **Async Processing**: Параллельная обработка
5. **Model Selection**: Выбор модели по сложности контента

### Масштабирование

#### Горизонтальное
```python
# Запуск нескольких инстансов анализаторов
for i in range(NUM_INSTANCES):
    analyzer = NewsAnalyzerWorker(consumer=f"analyzer-{i}")
    analyzers.append(analyzer)

# Kubernetes deployment
apiVersion: apps/v1
kind: Deployment
metadata:
  name: news-analyzer
spec:
  replicas: 5  # Автомасштабирование
  template:
    spec:
      containers:
      - name: analyzer
        resources:
          requests:
            cpu: 500m
            memory: 1Gi
          limits:
            cpu: 1000m
            memory: 2Gi
```

#### Вертикальное
- Увеличение ресурсов CPU/Memory
- Использование GPU для локальных моделей
- Оптимизация размера промпта
- Кеширование эмбеддингов
