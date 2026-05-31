# Conf Score Guardrails Autopromo (world practice)

## Цель
Автоматизировать безопасное продвижение конфигурации guardrails:
- staged candidate -> canary promote -> observe -> full promote / rollback
- минимальный blast radius
- детерминизм, SLO-гейты, авто-rollback при регрессии

## Компоненты
- `conf_score_guardrails_apply_v1.py` (stage): формирует candidate bundle и `staged.json`
- `conf_score_guardrails_promote_v1.py` (promote): применяется staged -> live, умеет `--promote-canary-only`
- `conf_score_guardrails_autopromo_controller_v1.py` (этот шаг): оркестрация canary/observe/promote/rollback
- `conf_score_guardrails_autopromo_exporter_v1.py`: метрики состояния автопромо

## Состояния (phase)
- `idle`: нет staged кандидата
- `baseline`: снят baseline health snapshot
- `canary_promote`: выполнен canary-only promote (без смены pointer)
- `observing`: ожидание окна наблюдения `CONF_SCORE_GUARD_CANARY_OBSERVE_SEC`
- `evaluate`: сравнение health метрик vs baseline
- `promote_full`: full promote + pointer update + clear staged (по настройкам)
- `rollback`: rollback к `current` (re-apply current bundle)
- `promoted`: успех
- `rolled_back`: откат выполнен
- `blocked`: требуется ручное вмешательство

## Health/SLO гейты (в этом шаге)
Источник: `CONF_SCORE_GUARD_HEALTH_STATE_PATH` (JSON)

Проверки:
- freshness: `age <= CONF_SCORE_GUARD_MAX_HEALTH_AGE_SEC`
- `degrade == 0`
- если присутствует `n`: `n >= CONF_SCORE_GUARD_MIN_N`
- если присутствуют paired arm-aware поля:
  - `arm_delta_ece_cal <= CONF_SCORE_GUARD_MAX_ARM_DELTA_ECE`
  - `arm_delta_brier_cal <= CONF_SCORE_GUARD_MAX_ARM_DELTA_BRIER`
- если присутствуют cohort поля:
  - `cohort_delta_ece_cal_wmean <= CONF_SCORE_GUARD_MAX_COHORT_DELTA_ECE_WMEAN`
  - `cohort_delta_brier_cal_wmean <= CONF_SCORE_GUARD_MAX_COHORT_DELTA_BRIER_WMEAN`
  - `cohort_delta_ece_cal_max <= CONF_SCORE_GUARD_MAX_COHORT_DELTA_ECE_MAX` (worst cohort)
  - `cohort_delta_brier_cal_max <= CONF_SCORE_GUARD_MAX_COHORT_DELTA_BRIER_MAX`
- иначе fallback (legacy baseline vs current):
  - `abs_delta_ece_cal <= CONF_SCORE_GUARD_MAX_ABS_DELTA_ECE`
  - `abs_delta_brier_cal <= CONF_SCORE_GUARD_MAX_ABS_DELTA_BRIER`

## Настройка (рекомендуемо)
1) Оставить включенным STAGE timer (формирует кандидаты).
2) Отключить ручной PROMOTE timer (чтобы не было конкурирующих решений).
3) Включить AUTOPROMO timer:
   - `deploy/systemd/conf-score-guardrails-autopromo.service`
   - `deploy/systemd/conf-score-guardrails-autopromo.timer`
4) Включить exporter автопромо (порт 9117 по умолчанию).

## Как дебажить
- состояние: `cat $CONF_SCORE_GUARD_AUTOPROMO_STATE_PATH`
- логи systemd: `journalctl -u conf-score-guardrails-autopromo -n 200 --no-pager`
- принудительный прогон:
  - dry-run: `CONF_SCORE_GUARD_AUTOPROMO_APPLY=0 systemctl start conf-score-guardrails-autopromo`
  - apply:   `CONF_SCORE_GUARD_AUTOPROMO_APPLY=1 systemctl start conf-score-guardrails-autopromo`

## Типовые инциденты
### phase=blocked после canary_promote
Причины:
- promote tool вернул rc!=0 (ошибка доступа к Redis, bundle не найден, health gate не прошёл)
Действия:
- запустить promote вручную с `--apply 0` и посмотреть stderr из state/history
- проверить наличие `staged.json` и файла bundle

### Частые rollback
Причины:
- слишком строгие `MAX_DELTA_*`
- health state шумный / окно наблюдения слишком короткое
Действия:
- увеличить observe window (например 1800s)
- добавить сглаживание health метрик в генераторе health-state
- ослабить margins или перейти на регрессионный тест по статистике сделок (более стабильный)

## Recommended next enhancement
- добавить последовательный тест (SPRT/always-valid p-value) для автопромо, чтобы уменьшить ложные блокировки на шуме.
- добавить time-decay weights (recent windows) и явную стратификацию по ликвидности/волатильности.
