# Yandex Tracker MCP

Независимый MCP-сервер на Python для работы агента с задачами Яндекс Трекера.
Проект не зависит от ChatbotAI и готов к отдельному репозиторию и деплою.

## Инструменты

- `create_issue` — создать задачу;
- `get_issue` — получить задачу;
- `update_issue` — изменить поля задачи;
- `list_issue_transitions` — получить допустимые переходы статуса;
- `cancel_issue` — отменить или закрыть задачу выбранным переходом.
- `search_issues` — получить компактный список задач;
- `schedule_tracker_report` — создать once/interval/cron расписание;
- `list_scheduled_reports` — получить расписания;
- `pause_scheduled_report`, `resume_scheduled_report`, `delete_scheduled_report`;
- `run_scheduled_report_now` — немедленно собрать и отправить отчёт;
- `get_latest_tracker_report`, `get_tracker_report_history` — прочитать SQLite-историю.

Яндекс Трекер не позволяет удалять отдельные задачи. Вместо удаления задача
закрывается или отменяется через workflow-переход. Все write-инструменты требуют
`confirmed=true`, а создание поддерживает защиту от дублей через поле `unique`.

## Быстрый запуск без изменения реальных задач

```powershell
py -3.12 -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -e ".[dev]"
$env:TRACKER_BACKEND="mock"
$env:YANDEX_DEFAULT_QUEUE="TEST"
$env:MCP_API_KEY="local-test-secret"
$env:MCP_PUBLIC_URL="http://localhost:8788"
yandex-tracker-mcp
```

Endpoint: `http://localhost:8788/mcp`.

Также поддерживается прямой запуск исходного файла:

```powershell
python src/yandex_tracker_mcp/server.py
```

## Получение доступа к Яндекс Трекеру

OAuth-приложению требуется разрешение **Запись в трекер (`tracker:write`)**.
Для документированного Яндекс Трекером потока `response_type=token` используется
Client ID; Client Secret серверу не нужен.

Сформировать ссылку авторизации без сохранения Client ID в проекте:

```powershell
$env:YANDEX_CLIENT_ID="your-client-id"
python scripts/oauth_url.py
```

Откройте выведенную ссылку под аккаунтом, от имени которого MCP будет работать,
разрешите доступ и сохраните полученный OAuth-токен. Затем в Трекере откройте
**Администрирование → Организации** и скопируйте идентификатор организации.

Для Яндекс 360 используется `X-Org-ID`, для Yandex Cloud — `X-Cloud-Org-ID`.

## Настройка production

Скопируйте `.env.example` в `.env` и заполните:

```powershell
Copy-Item .env.example .env
```

Сервер автоматически загружает `.env` из корня проекта.

```dotenv
TRACKER_BACKEND=yandex
YANDEX_AUTH_TYPE=oauth
YANDEX_TRACKER_TOKEN=your-oauth-token
YANDEX_ORG_ID=your-org-id
YANDEX_ORG_HEADER=X-Org-ID
YANDEX_DEFAULT_QUEUE=YOURQUEUE

MCP_API_KEY=another-long-random-secret
MCP_PUBLIC_URL=https://tracker-mcp.example.com
```

`YANDEX_TRACKER_TOKEN` авторизует запросы к Tracker. `MCP_API_KEY` — отдельный
секрет, который защищает публичный MCP endpoint. Не добавляйте `.env` в Git или
Docker image и используйте HTTPS reverse proxy на сервере.

## Docker

```bash
cp .env.example .env
# заполните .env
docker compose up --build -d
```

Контейнер слушает порт `8788` и публикует Streamable HTTP endpoint `/mcp`.
SQLite с расписаниями, запусками и отчётами сохраняется в `data/scheduler.db`.

## День 18: планировщик и Telegram

Настройки планировщика:

```dotenv
SCHEDULER_DATABASE=data/scheduler.db
SCHEDULER_TIMEZONE=Europe/Moscow
TELEGRAM_BOT_SERVICE_URL=http://telegram-bot:8791
TELEGRAM_BOT_SERVICE_API_KEY=shared-internal-secret
```

Пример расписания: отчёт по очереди `TEST` в 09:00 по будням:

```text
/mcp yandex-tracker schedule_tracker_report {"name":"Утренняя сводка","schedule_type":"cron","cron_expression":"0 9 * * 1-5","timezone":"Europe/Moscow","queue":"TEST","confirmed":true}
```

Агент ChatbotAI умеет выбрать этот инструмент самостоятельно: он уточнит расписание,
попросит подтверждение, затем вернёт ID задания и `next_run_at`. При запуске планировщик:

1. запрашивает задачи через Tracker API;
2. считает общее число, открытые, просроченные, критические и без исполнителя;
3. сохраняет отчёт и запуск в SQLite;
4. передаёт отчёт в защищённый `/notify` Telegram bot-service.

Для совместного запуска двух независимых проектов:

```bash
docker compose -f docker-compose.stack.yml up --build -d
```

Перед запуском создайте `.env` также в соседнем `../YandexTrackerTelegramBot`.

### Логи и диагностика доставки

Уровень подробности задаётся в `.env`:

```dotenv
LOG_LEVEL=INFO
```

Для максимально подробной диагностики временно используйте `LOG_LEVEL=DEBUG`.
Логи содержат ID задания, запуска и отчёта, количество найденных задач, агрегаты,
ответ bot-service и реальное число чатов, получивших сообщение. Токены и ключи
авторизации в лог не выводятся.

```bash
docker compose -f docker-compose.stack.yml logs -f --tail=200 \
  yandex-tracker-mcp telegram-bot
```

HTTP `200` от `/notify` означает только успешную обработку запроса сервисом. Итог
доставки смотрите в `status` и `delivered_chats`: `delivered` означает хотя бы одного
получателя, `no_subscribers` — в боте нет активных подписчиков, `partial` или `failed` —
Telegram отклонил часть или все сообщения.

## Подключение к агенту

В ChatbotAI нажмите `+ Tracker`, затем укажите URL сервера и заголовок:

```json
{
  "Authorization": "Bearer YOUR_MCP_API_KEY"
}
```

Пример прямого вызова через встроенную команду приложения:

```text
/mcp yandex-tracker create_issue {"summary":"Day 17 MCP demo","queue":"TEST","confirmed":true,"unique":"day17-tracker-demo"}
```

Для отмены агент сначала вызывает `list_issue_transitions`, затем передаёт точный
ID выбранного перехода в `cancel_issue`. Это важно, потому что workflow и названия
переходов отличаются между очередями.

## День 19: композиция MCP-инструментов

Сервер предоставляет три независимых шага:

1. `search_tracker_issues` ищет задачи, сохраняет неизменяемый снимок в SQLite и
   возвращает `search_id`;
2. `summarize_tracker_issues` принимает точный `search_id`, считает агрегаты,
   формирует Markdown и возвращает `summary_id`;
3. `save_tracker_report` принимает точный `summary_id`, создаёт реальный `.md` в
   `REPORTS_DIRECTORY` и при `send_to_telegram=true` отправляет файл bot-service.

Пример запроса агенту без ручной команды `/mcp`:

```text
Найди открытые задачи очереди AI, сделай сводку по просроченным и критическим,
сохрани отчёт в Markdown и отправь файл в Telegram.
```

ChatbotAI вызывает инструменты по очереди и передаёт идентификаторы из фактических
результатов предыдущих шагов. Снимки, сводки и метаданные файлов хранятся в той же
SQLite, что и расписания. Файлы сохраняются в `data/reports/YYYY-MM-DD`; каталог
`/app/data` подключён как постоянный Docker volume.

```dotenv
REPORTS_DIRECTORY=data/reports
```

## Проверки

```bash
ruff check .
pytest -q
```

Интеграционный тест ChatbotAI поднимает сервер с mock backend, получает
`tools/list`, вызывает `create_issue` и проверяет структурированный результат.
