# Ответы на вопросы по ML Confirm Gate и конфигурациям

## 1. `services/ml_confirm_gate.py` и `tick_flow_full/services/ml_confirm_gate.py`
**Обе папки развиваются параллельно, но основным prod-входом является `services/`.**
В файле `python-worker/main.py` импортируется `services.crypto_orderflow_service`, которая в свою очередь использует `services.ml_confirm_gate.py`. 
Код внутри `tick_flow_full/` выступает как зеркальный модуль (возможно, для тестирования, новой архитектуры tick-level пайплайна или отдельного воркера). На практике во все последние патчи изменения вносятся **в оба** пути одновременно, поэтому они обе важны для работы и консистентности.

## 2. В каком месте runtime "истина" для denylist
Истиной (Source of Truth) является файл в репозитории/контейнере, путь к которому определяется через **`ENV ML_FEATURE_DENYLIST_PATH`**. 
Redis cfg для этого не используется. 
Если переменная окружения не задана, используется fallback-путь к файлу в репозитории: `core/feature_denylist_v1.json`.

## 3. Выбор `v5_of_stable`
**Уже выбран в ENV прод-воркера.** 
В `docker-compose-crypto-orderflow.yml` для сервиса `crypto-orderflow-service` прямо сейчас жестко прописано:
`ML_FEATURE_SCHEMA_VER=v5_of_stable`

## 4. Какая политика по умолчанию в ENFORCE
По умолчанию включена политика **`OPEN` (fail-open)**.
В `docker-compose-crypto-orderflow.yml` задано: 
`ML_CONFIRM_FAIL_POLICY=OPEN`
Это означает, что при ошибках загрузки модели или несоответствии фич (mismatch) система пропустит сигнал, а не заблокирует его.

## 5. Нужен ли hard requirement: "stable model MUST include denylist_hash16"
**Не нужен (рекомендуется допускать legacy модели с warning).**
Если сделать hard requirement, то любые откаты (rollback) на старые, но проверенные (stable) модели, которые были обучены до внедрения хэша, приведут к их блокировке. В коде из недавнего патча P107 уже заложен fallback `denylist_hash16 = "na"`. Лучшая практика: допускать legacy модели (hash="na") с варнингом, чтобы не нарушить обратную совместимость и надежность продакшена.
