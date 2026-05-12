# Telegram price monitor bot

Внутренний Telegram-бот для мониторинга цен на масла и шроты. Бот получает текущие цены из внешних источников, сохраняет историю в SQLite, показывает последние замеры, сводку, статус сборов и график за последние 7 дней.

## Возможности

- Ручной сбор цен по кнопке `Текущие цены`.
- Автоматический сбор по расписанию.
- История в SQLite.
- Просмотр последних замеров.
- Сводка по товарам с изменением к предыдущему успешному замеру.
- Статус последнего ручного и автоматического сбора.
- График цены за последние 7 дней.
- Whitelist по Telegram user id.
- Логи с ротацией в `Logs/logs.log`.

## Структура файлов

```text
C:\Bot
├── .env.example          # пример переменных окружения
├── .gitignore            # исключения для секретов, БД, логов и кэша
├── README.md             # эксплуатационная документация
├── charts.py             # генерация PNG-графиков через matplotlib
├── database.py           # SQLite-схема и запросы
├── products.py           # конфиг товаров, API и парсинга
├── requirements.txt      # зависимости проекта
└── zapusk_project.py     # Telegram-бот, handlers, scheduler, сценарии
```

## Переменные окружения

Создайте `.env` рядом с кодом:

```env
TELEGRAM_TOKEN=your_telegram_bot_token_here
SQLITE_DB_PATH=prices.db
SCHEDULED_FETCH_TIMES=09:00,18:00
SCHEDULE_TIMEZONE=Europe/Moscow
ALLOWED_TELEGRAM_USER_IDS=123456789,987654321
```

Описание:

- `TELEGRAM_TOKEN` — токен Telegram-бота. Обязателен.
- `SQLITE_DB_PATH` — путь к SQLite-файлу. По умолчанию `prices.db`.
- `SCHEDULED_FETCH_TIMES` — время автосбора через запятую в формате `HH:MM`.
- `SCHEDULE_TIMEZONE` — таймзона расписания, например `Europe/Moscow`.
- `ALLOWED_TELEGRAM_USER_IDS` — Telegram user id разрешенных пользователей через запятую.

## Локальный запуск

Windows PowerShell:

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install -r requirements.txt
Copy-Item .env.example .env
# заполните .env реальными значениями
python .\zapusk_project.py
```

Linux:

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install -r requirements.txt
cp .env.example .env
# заполните .env реальными значениями
python zapusk_project.py
```

## Запуск на сервере

Минимальный вариант:

```bash
cd /opt/price-bot
python -m venv .venv
source .venv/bin/activate
python -m pip install -r requirements.txt
cp .env.example .env
nano .env
python zapusk_project.py
```

Для постоянной эксплуатации используйте systemd, supervisor или другой процесс-менеджер. Рабочая директория должна быть каталогом проекта, чтобы относительные пути `prices.db` и `Logs/logs.log` создавались рядом с кодом.

Пример systemd unit:

```ini
[Unit]
Description=Price monitor Telegram bot
After=network-online.target

[Service]
WorkingDirectory=/opt/price-bot
ExecStart=/opt/price-bot/.venv/bin/python /opt/price-bot/zapusk_project.py
Restart=always
RestartSec=10
EnvironmentFile=/opt/price-bot/.env

[Install]
WantedBy=multi-user.target
```

После создания unit-файла:

```bash
sudo systemctl daemon-reload
sudo systemctl enable price-bot
sudo systemctl start price-bot
sudo systemctl status price-bot
```

## Расписание

Автосбор работает через `JobQueue` из `python-telegram-bot`.

По умолчанию сбор запускается в:

- `09:00`
- `18:00`

Настройка:

```env
SCHEDULED_FETCH_TIMES=09:00,18:00
SCHEDULE_TIMEZONE=Europe/Moscow
```

Для быстрой проверки можно временно поставить ближайшее время, например:

```env
SCHEDULED_FETCH_TIMES=14:05
```

Автосбор сохраняет данные в `fetch_runs` и `price_snapshots`, но не отправляет сообщения в Telegram.

## Whitelist

Источник доступа — таблица `allowed_users` в SQLite.

Первичное заполнение выполняется при старте из `.env`:

```env
ALLOWED_TELEGRAM_USER_IDS=123456789,987654321
```

Чтобы добавить пользователя вручную:

```sql
INSERT OR IGNORE INTO allowed_users (telegram_user_id, username, is_active)
VALUES (123456789, 'username', 1);
```

Чтобы отключить пользователя:

```sql
UPDATE allowed_users
SET is_active = 0
WHERE telegram_user_id = 123456789;
```

## Проверка базы

Посмотреть таблицы:

```powershell
python -c "import sqlite3; con=sqlite3.connect('prices.db'); print(con.execute(\"SELECT name FROM sqlite_master WHERE type='table'\").fetchall())"
```

Последние запуски:

```powershell
python -c "import sqlite3; con=sqlite3.connect('prices.db'); print(con.execute('SELECT id, run_type, started_at, finished_at, success_count, error_count FROM fetch_runs ORDER BY id DESC LIMIT 5').fetchall())"
```

Последние сохраненные цены:

```powershell
python -c "import sqlite3; con=sqlite3.connect('prices.db'); print(con.execute('SELECT product_key, price, status, fetched_at FROM price_snapshots ORDER BY id DESC LIMIT 10').fetchall())"
```

## Логи

Логи пишутся в:

```text
Logs/logs.log
```

Включена ротация:

- размер файла: 5 MB;
- количество архивов: 5.

Windows:

```powershell
Get-Content .\Logs\logs.log -Tail 100
```

Linux:

```bash
tail -n 100 Logs/logs.log
```

## Графики

График строится по успешным `price_snapshots` за последние 7 дней. Временный PNG создается в системной временной директории и удаляется после отправки в Telegram. Кэш matplotlib направлен в `Logs/matplotlib`, чтобы не зависеть от прав на домашнюю директорию пользователя.

## Smoke-check

1. Запуск:
   ```bash
   python zapusk_project.py
   ```
   В логах должна появиться запись `Bot started, polling is starting`.

2. Whitelist:
   - напишите боту с ID, которого нет в `allowed_users`;
   - ожидаемый ответ: `Доступ запрещен`.

3. Ручной сбор:
   - нажмите `Текущие цены`;
   - проверьте, что появились записи в `fetch_runs` с `run_type='manual'`.

4. Scheduled run:
   - временно поставьте `SCHEDULED_FETCH_TIMES` на ближайшее время;
   - перезапустите бота;
   - проверьте `fetch_runs` на `run_type='scheduled'`.

5. Последние замеры и сводка:
   - нажмите `Последние замеры`;
   - нажмите `Сводка`;
   - ответы должны строиться из SQLite без обращения к live API.

6. График:
   - убедитесь, что по товару есть минимум две успешные точки за 7 дней;
   - нажмите `График`, выберите товар;
   - бот должен отправить PNG.

## Резервная копия SQLite

Простой вариант перед обновлениями:

Windows:

```powershell
Copy-Item .\prices.db ".\prices.backup.$(Get-Date -Format yyyyMMdd-HHmmss).db"
```

Linux:

```bash
cp prices.db "prices.backup.$(date +%Y%m%d-%H%M%S).db"
```
