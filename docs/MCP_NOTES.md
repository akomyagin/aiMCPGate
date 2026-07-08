# MCP_NOTES — выверка спецификации MCP (Этап 1)

Выверенные ответы по протоколу MCP, снимающие пометки
`[TODO уточнить по официальной спецификации в Этапе 1]` из
[`TECHNICAL_PLAN.md`](TECHNICAL_PLAN.md).

- **Версия спецификации:** `2025-06-18` (последняя стабильная на момент Этапа 1).
- **Источники:**
  - Base / JSON-RPC message types — <https://modelcontextprotocol.io/specification/2025-06-18/basic>
  - Transports (stdio, Streamable HTTP) — <https://modelcontextprotocol.io/specification/2025-06-18/basic/transports>
  - Lifecycle (initialize / initialized) — <https://modelcontextprotocol.io/specification/2025-06-18/basic/lifecycle>
  - Tools (`tools/list`, `tools/call`) — <https://modelcontextprotocol.io/specification/2025-06-18/server/tools>
  - Changelog (батчинг удалён) — <https://modelcontextprotocol.io/specification/2025-06-18/changelog>
  - Официальный Go SDK — <https://github.com/modelcontextprotocol/go-sdk>

---

## 1. Решение JSON-RPC вручную vs SDK — ручной слой (Вариант A)

**Итог: реализуем тонкий ручной JSON-RPC 2.0 слой (`internal/mcp`), SDK не берём.**

Официальный Go SDK MCP существует и **зрелый**: `github.com/modelcontextprotocol/go-sdk`,
версия **v1.6.1** (2026-05), ~**4.8k★**, поддерживается «in collaboration with
Google» (источник — README репозитория выше). То есть аргумент «SDK незрелый»
неактуален — выбор ручного слоя сделан **осознанно**, не из-за отсутствия SDK.

Почему всё же ручной слой:

1. **Прозрачность проксирования — решающий аргумент.** aiMCPGate — это прозрачный
   man-in-the-middle: он должен пробрасывать произвольные (в т. ч. неизвестные и
   будущие) JSON-RPC методы и поля **без потерь**. Ручной слой хранит
   `id` / `params` / `result` как `json.RawMessage`, поэтому тело сообщения
   проходит насквозь. Официальный SDK моделирует типизированные абстракции
   «клиент»/«сервер» (`CallTool`, `ListTools`, …), заточенные под «быть
   участником», а не под «прозрачно ретранслировать чужой трафик».
2. **Обучающий эффект** — главная мотивация pet-проекта.
3. **Фрейминг тривиален** (см. §2): newline-delimited JSON без батчей —
   `bufio.Scanner` + `json.Marshal`. Тащить зависимость ради этого не нужно.
4. **Нулевые внешние зависимости** для ядра протокола.

Типы сообщений при этом сверены со спецификацией `2025-06-18` (ниже) и со схемой
SDK. Миграция на SDK остаётся открытой опцией в post-MVP, если прозрачность
перестанет быть приоритетом.

---

## 2. Транспорт stdio: фрейминг

Дословно из спецификации transports:

- Клиент запускает MCP-сервер как **подпроцесс**; сервер читает JSON-RPC из
  `stdin`, пишет в `stdout`.
- **Сообщения разделены переводами строк (`\n`) и НЕ МОГУТ содержать встроенные
  переводы строк.** → одно сообщение = ровно одна строка.
- Все сообщения — **UTF-8**.
- Сервер **НЕ ДОЛЖЕН** писать в `stdout` ничего, кроме валидных MCP-сообщений.
- `stderr` — свободен для логов (клиент их может ловить/игнорировать). → наш
  операционный `slog` пишем в stderr, чтобы не засорять протокольный `stdout`.

**Следствия для кода (`internal/mcp`):**

- Чтение — построчно. `bufio.Scanner` с увеличенным буфером (дефолтный лимит
  `bufio.MaxScanTokenSize` = 64 KiB мал для крупных `inputSchema`/результатов) —
  ставим большой `Buffer` (напр. до 16–32 MiB). Альтернатива — `bufio.Reader.ReadBytes('\n')`.
- Запись — `json.Marshal(msg)` + один `\n`. Т. к. `json.Marshal` по умолчанию
  экранирует управляющие символы (включая `\n` внутри строк как `\\n`), само
  тело гарантированно однострочное; отдельный `\n` — только как разделитель кадров.
- **Батчинг НЕ поддерживается** в `2025-06-18` (удалён; ранее был в `2025-03-26`).
  → декодер принимает только одиночный JSON-объект на строку; ведущий `[` (JSON-массив
  = батч) считаем невалидным кадром и не парсим как батч.

## 3. Базовые типы JSON-RPC 2.0 (MCP-уточнения)

Из base-спецификации (важные отличия MCP от «ванильного» JSON-RPC):

- **Request:** `{jsonrpc:"2.0", id, method, params?}`. `id` — string ИЛИ integer,
  **НЕ null** (в MCP запрещён null-id), и **не переиспользуется** в рамках сессии.
- **Response:** `{jsonrpc:"2.0", id, result? | error?}`. Ровно одно из
  `result`/`error`. `id` совпадает с запросом.
- **Notification:** `{jsonrpc:"2.0", method, params?}` — **без `id`**, ответа не ждёт.
- **Error object:** `{code:int, message:string, data?:any}`.

**Следствия для кода:**

- `id` держим как `json.RawMessage` (может быть числом или строкой; не хардкодим `int`).
- `Params`/`Result` — `json.RawMessage` (прозрачный проброс, см. §1).
- Отличаем Response от Notification по наличию `id`; отличаем success от error по
  наличию `error`.
- Стандартные коды ошибок JSON-RPC, которые генерирует сам шлюз:
  `-32700` parse error, `-32600` invalid request, `-32601` method not found,
  `-32602` invalid params, `-32603` internal error. Ошибки от upstream
  проксируем **как есть** (в т. ч. server-defined коды).

## 4. Lifecycle / handshake

Порядок (спецификация lifecycle):

1. Клиент → сервер: `initialize` request
   ```json
   {"jsonrpc":"2.0","id":1,"method":"initialize","params":{
     "protocolVersion":"2025-06-18",
     "capabilities":{ "roots":{"listChanged":true}, "sampling":{}, "elicitation":{} },
     "clientInfo":{"name":"...","title":"...","version":"..."}}}
   ```
2. Сервер → клиент: `initialize` response
   ```json
   {"jsonrpc":"2.0","id":1,"result":{
     "protocolVersion":"2025-06-18",
     "capabilities":{ "tools":{"listChanged":true}, "resources":{"subscribe":true,"listChanged":true}, ... },
     "serverInfo":{"name":"...","version":"..."},
     "instructions":"optional"}}
   ```
3. Клиент → сервер: нотификация `notifications/initialized` (без id).
4. До ответа на `initialize` клиент шлёт только `ping`; сервер до `initialized` —
   только `ping`/`logging`.

**Version negotiation:** клиент шлёт желаемую версию; если сервер её поддерживает —
отвечает той же, иначе — своей. Если клиент не поддерживает ответную — отключается.

**Роль шлюза в handshake (Фаза 1):**
- Как **клиент к каждому upstream** — сам инициирует `initialize` + шлёт
  `notifications/initialized` (fan-out на старте).
- Как **сервер к клиенту** — отвечает на клиентский `initialize` собственным
  `serverInfo` (агрегированные capabilities), затем ждёт `notifications/initialized`.
  (клиент-facing сторона — Этап 4; в Этапе 1 покрыт мультиплексор к upstream.)

## 5. Каталоги и вызовы

- `tools/list`: `params.cursor?` → `result.tools[]` + `result.nextCursor?`.
  Поля тула: `name` (обяз.), `title?`, `description`, `inputSchema` (JSON Schema),
  `outputSchema?`, `annotations?`. Пагинацию обходим по `nextCursor` до пустого.
- `resources/list`: аналогично, `result.resources[]` (`uri`, `name`, `description?`,
  `mimeType?`), `nextCursor?`.
- `tools/call`: `params.name` + `params.arguments` → `result.content[]` (+ `isError`,
  `structuredContent?`). При неизвестном туле сервер отдаёт JSON-RPC error `-32602`.
- `resources/read`: `params.uri` → `result.contents[]`.
- `description` и `inputSchema` при агрегации **проксируем как есть**
  (`json.RawMessage`) — клиент должен видеть тот же контракт, что даёт upstream.

## 6. Неймспейсинг имён инструментов

- Спецификация `2025-06-18` **не задаёт** ограничения на набор символов в
  `tool.name` (ограничения символов формализованы только для ключей `_meta`).
- Разделитель `__` (двойное подчёркивание) валиден. На практике клиенты (Claude
  Code) ожидают имена вида `^[a-zA-Z0-9_-]+$`; `<upstream>__<tool>` попадает в этот
  класс, **если имя upstream** в конфиге ограничить теми же символами.
  → `internal/config` валидирует имя upstream по `^[a-zA-Z0-9_-]+$` (и требует
  уникальности) — реализуется в Этапе 2 при парсинге реального YAML.
- Таблица маршрутизации: `namespacedName → (upstream, originalName)`; при
  `tools/call` шлюз переписывает `name` обратно в оригинал перед форвардом.

## 7. Что осознанно НЕ делаем в MVP (Фаза 1)

- Пере-агрегация каталога по `notifications/*/list_changed` — post-MVP.
- Streamable HTTP / SSE транспорт и HTTP-upstream — Фаза 2 (Этап 5, **реализовано**, см. §8).
- Проксирование client-features (`sampling`, `roots`, `elicitation`) от upstream к
  клиенту — post-MVP; в MVP шлюз объявляет минимальные capabilities.
- Авто-рестарт упавших upstream — post-MVP (в MVP — изоляция, см. §4.4 плана).

## 8. Streamable HTTP — выверка и решения (Этап 5, Фаза 2)

Сверено со спецификацией `2025-06-18` (Transports → Streamable HTTP,
<https://modelcontextprotocol.io/specification/2025-06-18/basic/transports>).
Транспорт заменяет устаревший HTTP+SSE из `2024-11-05`. Один HTTP-эндпоинт
(«MCP endpoint», напр. `/mcp`) обслуживает POST и GET.

**Дословно из спецификации (то, на что опирается код):**

- Клиент шлёт **каждое** JSON-RPC сообщение отдельным HTTP **POST** на MCP endpoint.
- Клиент **ОБЯЗАН** слать `Accept: application/json, text/event-stream` (оба типа).
- Тело POST — **ровно одно** JSON-RPC сообщение (request / notification / response).
- Если вход — *notification* или *response*: сервер отвечает **`202 Accepted` без
  тела** (или HTTP-ошибку, если не принял).
- Если вход — *request*: сервер отвечает **либо** `Content-Type: application/json`
  (один JSON-объект), **либо** `Content-Type: text/event-stream` (открывает SSE-
  стрим). Клиент **ОБЯЗАН** поддерживать оба варианта.
- В SSE-стриме сервер **МОЖЕТ** слать промежуточные request/notification до
  финального *response* на исходный запрос; ответ идентифицируется по совпадению
  `id`. Полезная нагрузка события — в строках `data:`.
- `Mcp-Session-Id`: сервер **МОЖЕТ** выдать его в заголовке ответа на `initialize`;
  если выдан — клиент **ОБЯЗАН** слать его в заголовке на всех последующих запросах.
- `MCP-Protocol-Version: <version>` — заголовок на всех запросах после
  согласования версии; при отсутствии сервер предполагает `2025-03-26`.

**Решения aiMCPGate (по аналогии с развилкой §1):**

1. **Интерфейс `upstream.Conn` НЕ вводим** — «интерфейс на второй реализации» уже
   удовлетворён существующим `registry.Upstream` (Этап 3), которому `StdioConn`
   соответствует конкретным типом. Вторая реализация `HTTPConn`
   (`internal/upstream/http.go`) просто удовлетворяет **тот же** `registry.Upstream`;
   отдельный `upstream.Conn` был бы лишней абстракцией (анти-over-engineering,
   SKILL §8). Выбор stdio vs HTTP — по `config.Upstream.ResolveKind()` (url → http)
   в `registry.startUpstream`.
2. **HTTP-upstream (шлюз как HTTP-клиент):** `HTTPConn` — request/response без
   долгоживущей горутины-читателя (в отличие от `StdioConn`): HTTP round-trip сам
   демультиплексирует ответ, разделять id-пространства для этого не нужно. Ответ
   `text/event-stream` разбирается построчно (`data:`-кадры), берётся первое
   сообщение-*response* с совпадающим `id`; промежуточные server→client сообщения
   логируются на debug и пропускаются (проксирование client-features — post-MVP,
   §7). `Mcp-Session-Id` захватывается из ответа на `initialize` и эхонится далее.
3. **HTTP-сервер (шлюз как HTTP-сервер к клиенту):** `httpServer`
   (`internal/transport/http.go`) на POST отдаёт **один `application/json`** ответ
   на request и **`202`** на notification. SSE-стрим **сервер не открывает**: шлюз
   в MVP не генерирует потоковых/серверных сообщений (нет проксирования
   `sampling`/`roots`), а спецификация делает SSE на стороне сервера
   **опциональным** — поэтому GET на `/mcp` отвечает `405`. Открытие SSE
   понадобится только когда шлюз начнёт ретранслировать upstream→client трафик —
   это post-MVP.
4. **Общий диспетчер:** вся MCP-логика (`initialize`/`tools/list`/`tools/call`/
   `resources/*`) вынесена в транспорт-независимый `dispatcher`
   (`internal/transport/dispatch.go`), разделяемый `stdioServer` и `httpServer`;
   каждый транспорт держит только своё обрамление (чтение/запись кадров, SSE vs
   newline). Дублирования проксирующей логики между двумя транспортами нет.
5. **Секреты HTTP-upstream:** статические заголовки авторизации задаются в конфиге
   через env-expansion (`headers: { Authorization: "Bearer ${TOKEN}" }`), значение
   приходит из окружения, не из файла под git, и **никогда не логируется** (SKILL
   §6; покрыто тестом `TestHTTPAuthHeaderSentNotLogged`).
6. **Пропущено осознанно (post-MVP):** DELETE-терминация сессии, GET-стрим
   server→client, `Last-Event-ID`/резюмирование стрима, `Origin`-валидация и
   привязка к localhost для локального HTTP-сервера (security §Streamable HTTP) —
   актуально, когда HTTP-режим станет постоянным сетевым сервисом; помечено TODO
   в коде.
7. **Access policy (allow/deny инструментов по клиенту) — НЕ реализована.** План
   помечает её «опционально»; она требует идентификации клиента (в stdio клиент
   один и анонимен, в HTTP — нужен токен/заголовок для различения), что тянет за
   собой аутентификацию клиента — это ближе к multi-client (явно вне MVP, PLAN §6).
   Отложено в post-MVP; `CallRecord.Client` под неё уже зарезервирован.
