# 👤 User-Level Systemd Units для XAUUSD

**Запуск сервисов без sudo, под вашим пользователем**

---

## ⚡ Quick Start

```bash
# 1. Включить linger (чтобы сервисы работали после logout)
loginctl enable-linger "$USER"

# 2. Создать директорию для user units
mkdir -p ~/.config/systemd/user

# 3. Скопировать unit files
cp deploy/systemd/user/*.service ~/.config/systemd/user/

# 4. Перезагрузить systemd
systemctl --user daemon-reload

# 5. Запустить сервисы
systemctl --user enable --now xau-atr.service
systemctl --user enable --now xau-labeler.service

# 6. Проверить статус
systemctl --user status xau-atr.service
systemctl --user status xau-labeler.service
```

---

## 📁 Unit Files

### xau-atr.service

**Описание:** ATR Calculator from candles:data

**Что делает:**

- Читает из `candles:data` stream
- Вычисляет ATR(14) по Wilder
- Публикует в `atr:val:{symbol}:{tf}`

**Конфигурация:**

```ini
Environment=ATR_SYMBOLS=XAUUSD
Environment=ATR_TFS=1m,5m,15m,1d
Environment=ATR_PERIOD=14
```

---

### xau-labeler.service

**Описание:** Telegram Callback Labeler

**Что делает:**

- Читает из `bot:callbacks` stream
- Парсит callback события
- Публикует в `labels:trades`

**Конфигурация:**

```ini
Environment=CALLBACKS_STREAM=bot:callbacks
Environment=LABELS_STREAM=labels:trades
```

---

## 🔧 Управление

### Основные команды

```bash
# Запуск
systemctl --user start xau-atr.service
systemctl --user start xau-labeler.service

# Остановка
systemctl --user stop xau-atr.service
systemctl --user stop xau-labeler.service

# Перезапуск
systemctl --user restart xau-atr.service
systemctl --user restart xau-labeler.service

# Статус
systemctl --user status xau-atr.service
systemctl --user status xau-labeler.service

# Логи (live)
journalctl --user -u xau-atr.service -f
journalctl --user -u xau-labeler.service -f

# Логи (last 100 lines)
journalctl --user -u xau-atr.service -n 100
journalctl --user -u xau-labeler.service -n 100
```

---

### Auto-start

```bash
# Включить автозапуск
systemctl --user enable xau-atr.service
systemctl --user enable xau-labeler.service

# Выключить автозапуск
systemctl --user disable xau-atr.service
systemctl --user disable xau-labeler.service

# Проверить enabled/disabled
systemctl --user is-enabled xau-atr.service
systemctl --user is-enabled xau-labeler.service
```

---

## ⚙️ Кастомизация

### Изменить пути

Отредактируйте WorkingDirectory в unit файлах:

```ini
# Для нестандартного пути
WorkingDirectory=/custom/path/to/python-worker
ExecStart=/usr/bin/python3 /custom/path/to/python-worker/services/atr_from_candles.py
```

---

### Изменить переменные окружения

Отредактируйте Environment в unit файлах или создайте env file:

```bash
# Создать env file
cat > ~/.config/xauusd.env << 'EOF'
REDIS_URL=redis://localhost:6379/0
ATR_SYMBOLS=XAUUSD,BTCUSD
ATR_TFS=1m,5m,15m,1h,1d
EOF

# Использовать в unit file
[Service]
EnvironmentFile=%h/.config/xauusd.env
```

---

## 🐛 Troubleshooting

### Сервис не запускается

```bash
# Проверить статус
systemctl --user status xau-atr.service

# Проверить логи
journalctl --user -u xau-atr.service -n 50

# Проверить синтаксис unit file
systemd-analyze --user verify xau-atr.service
```

---

### Сервис останавливается после logout

```bash
# Включить linger
loginctl enable-linger "$USER"

# Проверить
loginctl show-user "$USER" | grep Linger
# Должно быть: Linger=yes
```

---

### Python не находится

```bash
# Найти путь к python3
which python3

# Обновить в unit file
ExecStart=/full/path/to/python3 services/atr_from_candles.py
```

---

## 🔄 Миграция на system-wide

Если нужно запускать как system service (с sudo):

```bash
# 1. Скопировать в /etc/systemd/system
sudo cp deploy/systemd/user/xau-atr.service /etc/systemd/system/

# 2. Добавить User в [Service]
sudo nano /etc/systemd/system/xau-atr.service
# Добавить после [Service]:
# User=alex
# Group=alex

# 3. Обновить пути (убрать %h)
# WorkingDirectory=/home/alex/front/trade/scanner_infra/python-worker

# 4. Reload и start
sudo systemctl daemon-reload
sudo systemctl enable --now xau-atr.service
sudo systemctl status xau-atr.service
```

---

## ✅ Checklist

**Установка:**

- [ ] Linger включен (`loginctl enable-linger`)
- [ ] Unit files скопированы в ~/.config/systemd/user/
- [ ] Пути в unit files корректны
- [ ] systemctl --user daemon-reload выполнен

**Запуск:**

- [ ] xau-atr.service запущен
- [ ] xau-labeler.service запущен
- [ ] Логи без ошибок
- [ ] Данные пишутся в Redis

**Проверка:**

- [ ] `redis-cli GET atr:val:XAUUSD:1m` возвращает значение
- [ ] `redis-cli XLEN labels:trades` растет
- [ ] Логи показывают активность

---

## 📚 Дополнительная информация

**Systemd user services документация:**

- `man systemd.service`
- `man systemd.unit`
- `man loginctl`

**Полезные ссылки:**

- [Systemd for Users](https://wiki.archlinux.org/title/Systemd/User)
- [Systemd unit files](https://www.freedesktop.org/software/systemd/man/systemd.service.html)

---

## 🎯 Рекомендации

✅ **User-level** - для development и personal use  
✅ **System-wide** - для production servers  
✅ **Docker** - для containerized deployment

**Выбирайте подход, который подходит вашему use case!**
