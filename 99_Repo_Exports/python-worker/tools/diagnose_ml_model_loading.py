#!/usr/bin/env python3
from __future__ import annotations

"""
Диагностика проблемы загрузки ML модели.

Проверяет:
1. Наличие конфигурации в Redis (cfg:ml_confirm:champion)
2. Наличие файла модели по пути из конфигурации
3. Права доступа к файлу модели
4. Наличие и доступность joblib
5. Попытка загрузки модели
6. Проверка логов на ошибки загрузки
"""


import json
import os
from typing import Any

import redis


def _safe_loads(s: Any) -> dict[str, Any]:
    """Safe JSON loads."""
    try:
        if s is None:
            return {}
        if isinstance(s, dict):
            return s
        if isinstance(s, bytes):
            s = s.decode("utf-8", "ignore")
        return json.loads(str(s))
    except Exception:
        return {}


def check_redis_config(redis_url: str, champion_key: str) -> tuple[bool, dict[str, Any] | None, str | None]:
    """Проверка конфигурации в Redis."""
    print("=" * 60)
    print("1. Проверка конфигурации в Redis")
    print("=" * 60)

    try:
        r = redis.Redis.from_url(redis_url, decode_responses=True)
        raw = r.get(champion_key)

        if not raw:
            print(f"❌ Ключ {champion_key} не найден в Redis")

            # Проверка fallback ключа
            fallback_key = "cfg:ml_confirm"
            h = r.hgetall(fallback_key)
            if h and len(h) > 0:
                print(f"⚠️  Найден fallback ключ {fallback_key} (hash)")
                print(f"   Поля: {list(h.keys())[:10]}")
                return True, dict(h), fallback_key
            else:
                print(f"❌ Fallback ключ {fallback_key} также не найден")
                return False, None, None

        cfg = _safe_loads(raw)
        if not cfg or not isinstance(cfg, dict):
            print("❌ Конфигурация не является валидным JSON объектом")
            return False, None, None

        print(f"✅ Конфигурация найдена в {champion_key}")
        print(f"   Поля: {list(cfg.keys())[:10]}")

        model_path = cfg.get("model_path", "")
        if not model_path:
            print("⚠️  Поле 'model_path' отсутствует или пустое")
        else:
            print(f"   model_path: {model_path}")

        return True, cfg, champion_key

    except Exception as e:
        print(f"❌ Ошибка при подключении к Redis: {e}")
        return False, None, None


def check_model_file(model_path: str) -> tuple[bool, str | None]:
    """Проверка наличия и доступности файла модели."""
    print("\n" + "=" * 60)
    print("2. Проверка файла модели")
    print("=" * 60)

    if not model_path:
        print("❌ Путь к модели не указан")
        return False, None

    print(f"Путь: {model_path}")

    # Проверка существования
    if not os.path.exists(model_path):
        print("❌ Файл не существует")

        # Проверка родительской директории
        parent_dir = os.path.dirname(model_path)
        if os.path.exists(parent_dir):
            print(f"   Родительская директория существует: {parent_dir}")
            print("   Содержимое директории:")
            try:
                files = os.listdir(parent_dir)
                for f in sorted(files)[:10]:
                    print(f"     - {f}")
            except Exception as e:
                print(f"     Ошибка при чтении директории: {e}")
        else:
            print(f"   Родительская директория не существует: {parent_dir}")

        return False, None

    print("✅ Файл существует")

    # Проверка размера
    try:
        size = os.path.getsize(model_path)
        print(f"   Размер: {size:,} байт ({size / 1024 / 1024:.2f} MB)")
        if size == 0:
            print("   ⚠️  Файл пустой!")
            return False, model_path
    except Exception as e:
        print(f"   ⚠️  Ошибка при получении размера: {e}")

    # Проверка прав доступа
    try:
        file_stat = os.stat(model_path)
        mode = file_stat.st_mode

        readable = os.access(model_path, os.R_OK)
        writable = os.access(model_path, os.W_OK)
        executable = os.access(model_path, os.X_OK)

        print(f"   Права доступа: {oct(mode)[-3:]}")
        print(f"   Чтение: {'✅' if readable else '❌'}")
        print(f"   Запись: {'✅' if writable else '❌'}")
        print(f"   Выполнение: {'✅' if executable else '❌'}")

        if not readable:
            print("   ⚠️  Файл не доступен для чтения!")
            return False, model_path

        # Проверка владельца
        import pwd
        try:
            owner = pwd.getpwuid(file_stat.st_uid).pw_name
            print(f"   Владелец: {owner} (UID: {file_stat.st_uid})")
        except Exception:
            print(f"   Владелец: UID {file_stat.st_uid}")

        # Проверка группы
        import grp
        try:
            group = grp.getgrgid(file_stat.st_gid).gr_name
            print(f"   Группа: {group} (GID: {file_stat.st_gid})")
        except Exception:
            print(f"   Группа: GID {file_stat.st_gid}")

    except Exception as e:
        print(f"   ⚠️  Ошибка при проверке прав доступа: {e}")
        return False, model_path

    return True, model_path


def check_joblib() -> tuple[bool, str | None]:
    """Проверка наличия joblib."""
    print("\n" + "=" * 60)
    print("3. Проверка joblib")
    print("=" * 60)

    try:
        import joblib
        print("✅ joblib установлен")
        print(f"   Версия: {joblib.__version__}")
        print(f"   Путь: {joblib.__file__}")
        return True, joblib.__version__
    except ImportError:
        print("❌ joblib не установлен")
        print("   Установите: pip install joblib")
        return False, None
    except Exception as e:
        print(f"❌ Ошибка при импорте joblib: {e}")
        return False, None


def try_load_model(model_path: str) -> tuple[bool, Any | None, str | None]:
    """Попытка загрузки модели."""
    print("\n" + "=" * 60)
    print("4. Попытка загрузки модели")
    print("=" * 60)

    try:
        import joblib
    except ImportError:
        print("❌ joblib не установлен, загрузка невозможна")
        return False, None, "joblib_not_installed"

    try:
        print(f"Загрузка модели из: {model_path}")
        model = joblib.load(model_path)
        print("✅ Модель успешно загружена")
        print(f"   Тип: {type(model).__name__}")
        print(f"   Модуль: {type(model).__module__}")

        # Проверка методов для util_mh
        has_predict_util = hasattr(model, "predict_util")
        has_predict_unc = hasattr(model, "predict_unc")

        print(f"   predict_util: {'✅' if has_predict_util else '❌'}")
        print(f"   predict_unc: {'✅' if has_predict_unc else '❌'}")

        if has_predict_util and has_predict_unc:
            print("✅ Модель соответствует формату UtilMHModelV1")
        else:
            print("⚠️  Модель не имеет требуемых методов для util_mh")

        # Проверка дополнительных атрибутов
        if hasattr(model, "horizons"):
            print(f"   horizons: {getattr(model, 'horizons', 'N/A')}")
        if hasattr(model, "feature_cols"):
            cols = getattr(model, "feature_cols", [])
            print(f"   feature_cols: {len(cols)} колонок")
        if hasattr(model, "unc_k"):
            print(f"   unc_k: {getattr(model, 'unc_k', 'N/A')}")

        return True, model, None

    except FileNotFoundError:
        print(f"❌ Файл не найден: {model_path}")
        return False, None, "file_not_found"
    except PermissionError:
        print(f"❌ Нет прав доступа к файлу: {model_path}")
        return False, None, "permission_denied"
    except Exception as e:
        print(f"❌ Ошибка при загрузке модели: {e}")
        import traceback
        print("\nДетали ошибки:")
        traceback.print_exc()
        return False, None, str(e)


def check_logs_for_errors(redis_url: str, metrics_stream: str = "metrics:ml_confirm", limit: int = 100) -> None:
    """Проверка логов на ошибки загрузки модели."""
    print("\n" + "=" * 60)
    print("5. Проверка логов на ошибки")
    print("=" * 60)

    try:
        r = redis.Redis.from_url(redis_url, decode_responses=True)

        # Читаем последние записи из stream
        try:
            messages = r.xrevrange(metrics_stream, count=limit)
            if not messages:
                print(f"⚠️  Stream {metrics_stream} пуст или не существует")
                return
        except Exception as e:
            print(f"⚠️  Ошибка при чтении stream {metrics_stream}: {e}")
            return

        print(f"Проверка последних {len(messages)} записей из {metrics_stream}")

        error_count = 0
        no_model_count = 0
        other_errors = {}

        for msg_id, fields in messages:
            err = fields.get("err", "").strip()
            status = fields.get("status", "").strip()

            if err:
                error_count += 1
                if "no_model_loaded" in err.lower():
                    no_model_count += 1
                else:
                    other_errors[err] = other_errors.get(err, 0) + 1

        print("\nСтатистика ошибок:")
        print(f"   Всего записей с ошибками: {error_count}/{len(messages)}")
        print(f"   no_model_loaded: {no_model_count}")

        if other_errors:
            print("   Другие ошибки:")
            for err, count in sorted(other_errors.items(), key=lambda x: x[1], reverse=True)[:5]:
                print(f"     - {err}: {count}")

        if no_model_count > 0:
            print(f"\n⚠️  Найдено {no_model_count} записей с ошибкой 'no_model_loaded'")
            print("   Это подтверждает проблему загрузки модели")

    except Exception as e:
        print(f"⚠️  Ошибка при проверке логов: {e}")


def main() -> None:
    """Главная функция диагностики."""
    redis_url = os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0")
    champion_key = os.getenv("ML_CFG_CHAMPION_KEY", "cfg:ml_confirm:champion")
    metrics_stream = os.getenv("ML_CONFIRM_METRICS_STREAM", "metrics:ml_confirm")

    print("=" * 60)
    print("Диагностика загрузки ML модели")
    print("=" * 60)
    print(f"Redis URL: {redis_url}")
    print(f"Champion key: {champion_key}")
    print()

    # 1. Проверка конфигурации
    cfg_ok, cfg, cfg_source = check_redis_config(redis_url, champion_key)

    if not cfg_ok:
        print("\n⚠️  Конфигурация не найдена в Redis")
        print("   Продолжаем диагностику других аспектов...")
        model_path = None
    else:
        model_path = cfg.get("model_path", "") if cfg else ""

    # 2. Проверка файла модели
    if model_path:
        file_ok, verified_path = check_model_file(model_path)
    else:
        print("\n" + "=" * 60)
        print("2. Проверка файла модели")
        print("=" * 60)
        print("⚠️  Путь к модели неизвестен (конфигурация не найдена)")
        print("   Проверяем стандартные пути...")
        file_ok = False
        verified_path = None

        # Проверка стандартных путей
        standard_paths = [
            "/var/lib/trade/of_reports/models/model.joblib",
            "/var/lib/trade/ml_models/model.joblib",
            "./models/model.joblib",
        ]

        for path in standard_paths:
            if os.path.exists(path):
                print(f"✅ Найден файл модели: {path}")
                file_ok, verified_path = check_model_file(path)
                if file_ok:
                    break

    # 3. Проверка joblib
    joblib_ok, joblib_version = check_joblib()

    # 4. Попытка загрузки модели
    if file_ok and joblib_ok:
        load_ok, model, error = try_load_model(verified_path or model_path)
    else:
        load_ok, model, error = False, None, "prerequisites_not_met"
        print("\n⚠️  Пропуск загрузки модели (не выполнены предварительные условия)")

    # 5. Проверка логов
    check_logs_for_errors(redis_url, metrics_stream)

    # Итоговый отчет
    print("\n" + "=" * 60)
    print("ИТОГОВЫЙ ОТЧЕТ")
    print("=" * 60)

    all_ok = cfg_ok and file_ok and joblib_ok and (load_ok if file_ok and joblib_ok else False)

    if all_ok:
        print("✅ Все проверки пройдены успешно")
        print("   Модель должна загружаться корректно")
    else:
        print("❌ Обнаружены проблемы:")
        if not cfg_ok:
            print("   - Конфигурация не найдена в Redis")
        if not file_ok:
            print(f"   - Файл модели не найден или недоступен: {model_path}")
        if not joblib_ok:
            print("   - joblib не установлен или недоступен")
        if file_ok and joblib_ok and not load_ok:
            print(f"   - Ошибка при загрузке модели: {error}")

        print("\nРекомендации по исправлению:")
        if not cfg_ok:
            print("1. Установите конфигурацию в Redis:")
            print(f"   redis-cli SET {champion_key} '{{\"model_path\": \"/path/to/model.joblib\", ...}}'")
        if not file_ok:
            print("2. Проверьте путь к модели и права доступа")
            print("3. Используйте update_ml_model.sh для обучения и деплоя модели")
        if not joblib_ok:
            print("4. Установите joblib: pip install joblib")
        if file_ok and joblib_ok and not load_ok:
            print("5. Проверьте формат модели (должен быть UtilMHModelV1)")
            print("6. Проверьте логи worker'а на детали ошибки")


if __name__ == "__main__":
    main()

