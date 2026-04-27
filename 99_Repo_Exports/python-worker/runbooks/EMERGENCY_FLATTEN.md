# Runbook — Emergency Flatten

## Когда применять
- protection not confirmed after `ENTRY_FILLED`
- reconcile не может доказать фактический статус exit order
- position state в Redis расходится с exchange state
- high-severity incident с потерей determinism

## Цель
Снизить экспозицию как можно быстрее. Приоритет — закрыть риск, а не сохранить maker-fee экономию.

## Алгоритм
1. Отключить новые maker exits:
   ```env
   EXEC_MAKER_TP_ENABLE=0
   EXEC_FORCE_SAFETY_FIRST=1
   ```
2. Убедиться, что executor работает с plain high-priority reduce path.
3. Для affected `sid` / `symbol`:
   - query current position
   - query open plain orders
   - query open algo orders
   - отменить orphan algo/plain exits
   - отправить market reduce-exposure close
4. Записать факт в `orders:exec` и SQL journal.

## Redis quick checks
```bash
redis-cli XREVRANGE orders:exec + - COUNT 20
redis-cli GET orders:state:<SID>
redis-cli KEYS 'orders:user_stream:*'
```

## Post-check
- positionAmt == 0
- open algo orders for symbol == 0
- state in Redis updated to `EMERGENCY_FLATTENED` or `EXIT_FILLED`
