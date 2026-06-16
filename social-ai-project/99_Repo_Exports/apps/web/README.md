# apps/web — Next.js Operator Dashboard

Operator UI (plan §13). Каждое действие агента видно: что решил, почему, на каких данных, чем закончилось.

## Стек
Next.js 15 (App Router) · **Tailwind CSS** (operator-консоль, dark-first) · **framer-motion**
(переходы навигации, списки, score-бары) · **lucide-react** (иконки) · **SWR** (live-поллинг
control plane) · clsx + tailwind-merge.

## Запуск
```bash
npm install
npm run dev      # http://localhost:3002  (API: NEXT_PUBLIC_API_URL, по умолчанию http://localhost:3000)
npm run build    # прод-сборка
```

## Глобальное разделение по платформам
Верхняя панель содержит переключатель **Все / YouTube / TikTok / Instagram**
(`components/platform.tsx`, контекст + localStorage). Выбор скоупит все живые экраны
(обзор, тренды, ревью, публикация) через `matchPlatform()`, потому что правила публикации,
квоты, метрики и политика различаются по платформам.

## Экраны (§13.1)
| Маршрут | Статус | Источник |
|---|---|---|
| `/` Обзор | ✅ live | `/trends`, `/review/queue`, `/publish/status` |
| `/trends`, `/trends/[id]` | ✅ live | `GET /trends`, `GET /trends/:id` |
| `/review` | ✅ live | `GET /review/queue` + approve/reject; три ветки риска (platform/brand/commercial) |
| `/publish` | ✅ live | `GET /publish/status` |
| `/outcomes` `/experiments` `/governors` `/alerts` `/replay` `/settings/platforms` | ⏳ доменный каркас | ожидают эндпоинтов |

## Структура
- `app/` — экраны (App Router)
- `components/` — `ui.tsx` (Card/Badge/Button/Stat/ScoreBar/ReasonCodes/EmptyState), `nav.tsx`,
  `shell.tsx`, `platform.tsx`, `motion.tsx`
- `lib/` — `api.ts` (типизированный клиент + мутации), `types.ts` (view-типы под /schemas),
  `format.ts` (epoch_ms, проценты, risk-банды), `cn.ts`

Skills: `nextjs-turbopack`, `react-patterns`, `react-performance`.
