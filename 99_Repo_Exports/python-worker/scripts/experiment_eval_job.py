#!/usr/bin/env python3
# scripts/experiment_eval_job.py

""",
Оффлайн-джоб для расчёта метрик экспериментов

Запускать cron-ом каждые 5-15 минут или как отдельный сервис.
""",

import os
import json
import time
import logging
from typing import Dict, Any, List

import psycopg2
from psycopg2.extras import RealDictCursor

from handlers.experiment_metrics import calculate_experiment_metrics

# Настройки
PG_DSN = (os.getenv("ANALYTICS_DB_DSN") or os.getenv("PG_DSN"))
SUCCESS_THRESHOLD_R = float(os.getenv("EXPERIMENT_SUCCESS_R", "0.2"))
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")

logging.basicConfig(level=getattr(logging, LOG_LEVEL))
logger = logging.getLogger(__name__)


def run_experiment_eval() -> None:
    """,
    Основная функция джоба
    """,
    logger.info("Starting experiment evaluation job")

    conn = psycopg2.connect(PG_DSN)
    try:
        # Получаем список активных экспериментов
        experiments = get_active_experiments(conn)

        for exp_id in experiments:
            logger.info("Evaluating experiment: %s", exp_id)
            eval_experiment(conn, exp_id)

        logger.info("Experiment evaluation completed")

    finally:
        conn.close()


def get_active_experiments(conn) -> List[str]:
    """,
    Возвращает список ID активных экспериментов
    """,
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(
            """,
            select experiment_id
            from signal_experiment
            where status in ('running', 'completed')
            """,
        )
        rows = cur.fetchall()
        return [r["experiment_id"] for r in rows]


def eval_experiment(conn, experiment_id: str) -> None:
    """,
    Вычисляет метрики для одного эксперимента и сохраняет в snapshot
    """,
    now_ts = int(time.time())
    as_of = now_ts  # seconds

    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        # Выбираем данные по эксперименту
        cur.execute(
            """,
            select
                s.experiment_variant,
                coalesce(sp.realized_r, 0.0) as pnl_r,
                case when sp.outcome in ('realized', 'stopped') then 1 else 0 end as was_traded
            from signals s
            left join signal_performance sp on s.signal_id = sp.signal_id
            where s.experiment_id = %s
            """,
            (experiment_id,),
        )
        rows = cur.fetchall()

    if not rows:
        logger.warning("No data found for experiment %s", experiment_id)
        return

    # Группируем по variant
    by_variant: Dict[str, Dict[str, Any]] = {}
    for row in rows:
        variant = row["experiment_variant"] or "control"
        v = by_variant.setdefault(
            variant, {"pnl_rs": [], "traded_flags": []}
        )
        # Берем только реально отторгованные сделки для расчёта метрик
        if row["was_traded"]:
            v["pnl_rs"].append(float(row["pnl_r"]))
            v["traded_flags"].append(True)
        else:
            # Для precision/recall учитываем все сигналы
            v["traded_flags"].append(False)

    # Вычисляем метрики для каждого варианта
    with conn.cursor() as cur:
        for variant, data in by_variant.items():
            pnl_rs = data["pnl_rs"]
            traded_flags = data["traded_flags"]

            if not pnl_rs:
                logger.warning("No traded signals for experiment %s variant %s", experiment_id, variant)
                continue

            # Вычисляем метрики
            metrics = calculate_experiment_metrics(pnl_rs, SUCCESS_THRESHOLD_R)

            # Добавляем дополнительные метрики
            signals_total = len(traded_flags)
            traded_total = len(pnl_rs)

            # Сохраняем в snapshot
            cur.execute(
                """,
                insert into signal_experiment_snapshot (
                    experiment_id, as_of, variant,
                    signals_total, traded_total, winners_total, losers_total,
                    expectancy_r, sharpe_r, max_dd_r, cl_ratio, winrate,
                    precision, recall, f1, extra
                )
                values (
                    %s, to_timestamp(%s), %s,
                    %s, %s, %s, %s,
                    %s, %s, %s, %s, %s,
                    %s, %s, %s, %s::jsonb
                )
                on conflict (experiment_id, as_of, variant) do nothing
                """,
                (
                    experiment_id,
                    as_of,
                    variant,
                    signals_total,
                    traded_total,
                    metrics["winners_total"],
                    metrics["losers_total"],
                    metrics["expectancy_r"],
                    metrics["sharpe_r"],
                    metrics["max_dd_r"],
                    metrics["cl_ratio"],
                    metrics["winrate"],
                    metrics["precision"],
                    metrics["recall"],
                    metrics["f1"],
                    json.dumps({
                        "success_threshold_r": SUCCESS_THRESHOLD_R,
                        "evaluation_timestamp": now_ts,
                    }),
                )
            )

        conn.commit()

    logger.info("Evaluated experiment %s: %d variants processed", experiment_id, len(by_variant))


if __name__ == "__main__":
    run_experiment_eval()























































