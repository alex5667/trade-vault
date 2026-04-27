# Orchestration Workflows

Добавлены три orchestration-workflow для делегирования между ролями:

- `/trade-parallel-investigation` — параллельное расследование неочевидной проблемы
- `/trade-sequential-review` — последовательный review, где каждый шаг зависит от предыдущего
- `/trade-release-gate` — формальный gate для merge / canary / prod

Когда использовать:
- проблема неясна и может быть в нескольких подсистемах -> `parallel`
- change понятен, но нужен ordered validation chain -> `sequential`
- нужно решение go / no-go перед выпуском -> `release-gate`
