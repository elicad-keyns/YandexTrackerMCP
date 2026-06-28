# Yandex Tracker MCP

Независимый MCP-сервер на Python для работы агента с задачами Яндекс Трекера.
Проект не зависит от ChatbotAI и готов к отдельному репозиторию и деплою.

## Инструменты

- `create_issue` — создать задачу;
- `get_issue` — получить задачу;
- `update_issue` — изменить поля задачи;
- `list_issue_transitions` — получить допустимые переходы статуса;
- `cancel_issue` — отменить или закрыть задачу выбранным переходом.

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

## Проверки

```bash
ruff check .
pytest -q
```

Интеграционный тест ChatbotAI поднимает сервер с mock backend, получает
`tools/list`, вызывает `create_issue` и проверяет структурированный результат.
