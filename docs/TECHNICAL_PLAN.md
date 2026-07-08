# TECHNICAL_PLAN — aiMCPGate

Технический план: стек, архитектура мультиплексора, разбивка по Этапам.
Продуктовое видение — [`PLAN.md`](PLAN.md); post-MVP — [`POST_MVP_PLAN.md`](POST_MVP_PLAN.md).
Конвенции написания кода — [`.claude/skills/go-mcp-gateway-dev/SKILL.md`](../.claude/skills/go-mcp-gateway-dev/SKILL.md).

> **Дисклеймер по протоколу — снят в Этапе 1.** Детали MCP на момент написания
> плана были взяты по памяти. В Этапе 1 они **сверены с официальной
> спецификацией** MCP версии `2025-06-18` (<https://modelcontextprotocol.io/specification/2025-06-18>)
> и зафиксированы в [`docs/MCP_NOTES.md`](MCP_NOTES.md). Оставшиеся по тексту
> пометки `[TODO уточнить по официальной спецификации в Этапе 1]` считать
> закрытыми ссылкой на `MCP_NOTES.md`; ключевые выводы продублированы по месту.

---

## 1. Предварительное условие: прочитать спецификацию MCP — ВЫПОЛНЕНО (Этап 1)

Спецификация MCP версии `2025-06-18` сверена; выверенные ответы зафиксированы в
[`docs/MCP_NOTES.md`](MCP_NOTES.md). Резюме (детали и цитаты — в MCP_NOTES.md):

- **Транспорты:** stdio — **newline-delimited UTF-8 JSON**, одно сообщение = одна
  строка, встроенные `\n` **запрещены**, батчинг **удалён** в `2025-06-18`.
  Streamable HTTP/SSE — Фаза 2.
- **Handshake:** `initialize` (`params.protocolVersion` / `capabilities` /
  `clientInfo`) → ответ (`result.protocolVersion` / `capabilities` / `serverInfo`)
  → клиент шлёт нотификацию `notifications/initialized`.
- **Каталоги:** `tools/list` → `result.tools[]` (`name`, `title?`, `description`,
  `inputSchema`, `outputSchema?`), пагинация через `params.cursor` / `nextCursor`;
  `resources/list` → `result.resources[]`, аналогично.
- **Вызовы:** `tools/call` (`params.name` + `params.arguments`) → `result.content[]`
  + `isError`; `resources/read` (`params.uri`) → `result.contents[]`. Ошибки
  протокола — JSON-RPC `error{code,message,data?}`; ошибки исполнения инструмента —
  `result.isError=true` (шлюз проксирует оба вида как есть).
- **Уведомления:** `notifications/tools/list_changed` и т. п. — в MVP шлюз
  пере-агрегацию каталога по ним **не делает** (post-MVP), см. §4.4.

**Официальный Go SDK MCP существует и зрелый** (`github.com/modelcontextprotocol/go-sdk`,
v1.6.1, ~4.8k★, коллаборация с Google). Решение — брать/не брать — закрыто в
[«JSON-RPC вручную vs SDK»](#3-решение-json-rpc-вручную-vs-sdk--закрыто-этап-1)
ниже: **выбран ручной слой** (прозрачность проксирования + обучение).

---

## 2. Стек

| Слой | Выбор | Обоснование |
|---|---|---|
| Язык | **Go 1.23** | Приоритет пользователя; конкурентность и `os/exec` — идеальны для мультиплексора |
| CLI | `spf13/cobra` + `viper` | Как в `gitl`; команды `serve`, `logs`, `version` |
| Конфиг | YAML (`gopkg.in/yaml.v3`) | Список upstream-серверов; секреты — через env-expansion, не в файле |
| Логи (операционные) | `log/slog` (JSON handler, stderr) | Стандарт; `--verbose` → debug |
| Журнал вызовов | Свой writer, JSON-lines | Добавленная ценность; отдельно от slog (см. §6) |
| JSON-RPC | **см. §3** (вручную или SDK — решить в Этапе 1) | Обучающий эффект vs скорость |
| Конкурентность | горутины + каналы, `context` | Мультиплексирование, отмена, graceful shutdown |
| Транспорт (Фаза 2 HTTP) | `net/http` + SSE вручную | Без фреймворка, тренировка |
| Тесты | стандартный `testing`, table-driven, `httptest` | Как в `gitl` |
| Релизы (Этап 6) | `goreleaser` | Кросс-платформенные бинари, как в `gitl` |

**`pkg/` намеренно нет** — всё ядро в `internal/`, чтобы никто не импортировал
незрелый API как библиотеку (правило перенято из `gitl`).

---

## 3. Решение: JSON-RPC вручную vs SDK — ЗАКРЫТО (Этап 1)

Развилка закрыта в начале Этапа 1 после сверки со спецификацией. Варианты:

- **Вариант A — вручную (`encoding/json` + свой JSON-RPC 2.0 слой).** Плюс:
  обучающий эффект (главная мотивация проекта), нулевые зависимости, полный
  контроль над фреймингом stdio и проксированием «сырых» сообщений. Минус:
  больше кода, риск разойтись со спецификацией.
- **Вариант B — официальный Go SDK MCP** (`github.com/modelcontextprotocol/go-sdk`).
  Плюс: корректность протокола из коробки, быстрее до рабочей Фазы 1. Минус:
  меньше обучения, зависимость от зрелости SDK, сложнее «прозрачно» проксировать
  неизвестные/будущие методы (SDK моделирует конкретные типизированные вызовы, а
  не «сырой» проброс произвольного JSON-RPC).

**РЕШЕНИЕ: Вариант A — тонкий ручной JSON-RPC 2.0 слой** (`internal/mcp`).

Обоснование:

1. **Обучающий эффект** — главная мотивация pet-проекта; ручной слой её реализует.
2. **Прозрачность проксирования — решающий технический аргумент.** Ключевое
   свойство шлюза: неизвестные методы и поля должны проходить **насквозь без
   потерь**. Ручной слой хранит `Params`/`Result`/`id` как `json.RawMessage`,
   поэтому шлюз пробрасывает «сырое» тело, не привязываясь к конкретному
   типизированному вызову. Официальный SDK моделирует типизированные
   client/server-абстракции (`mcp.Client`, `mcp.Server`, типизированные
   `CallTool`/`ListTools`), которые оптимизированы под «быть сервером/клиентом»,
   а не под «прозрачный man-in-the-middle мультиплексор произвольного трафика».
3. **Фрейминг тривиален** (см. §ниже и `MCP_NOTES.md`): stdio — это просто
   newline-delimited UTF-8 JSON, сообщения без встроенных `\n`, **без батчей**
   (батчинг удалён в спецификации `2025-06-18`). Реализация — `bufio.Scanner`
   (увеличенный буфер) на чтение и `json.Marshal` + `\n` на запись. Тащить
   зависимость ради этого не оправдано.
4. **Нулевые зависимости** для ядра протокола; из внешнего берём только
   `golang.org/x/sync/errgroup` (fan-out) — не сам протокол.

**Официальный Go SDK существует и зрелый** (`github.com/modelcontextprotocol/go-sdk`,
v1.6.1 на 2026-05, ~4.8k★, поддерживается в коллаборации с Google —
источник: <https://github.com/modelcontextprotocol/go-sdk>). Мы сознательно его
**не берём** по причинам 1–2 выше; но **типы сообщений сверяем** с его схемой и
со спецификацией `2025-06-18`. Если в post-MVP прозрачность перестанет быть
приоритетом, миграция на SDK остаётся открытой опцией. Полная сверка форматов
(`initialize`, `tools/list`, `tools/call`, `resources/*`, ошибки, фрейминг) —
в [`docs/MCP_NOTES.md`](MCP_NOTES.md).

---

## 4. Архитектура мультиплексора

```
   MCP-клиент (Claude Code)
            │  stdio (Фаза 1) / HTTP+SSE (Фаза 2), JSON-RPC 2.0
            ▼
   ┌───────────────────────────────────────────────┐
   │  internal/transport  (клиент-facing сервер)    │
   │   - читает запросы клиента, пишет ответы        │
   │   - для tools/list отдаёт агрегированный каталог│
   └───────────────┬───────────────────────────────┘
                   │  вызовы: resolve(tool) → upstream
                   ▼
   ┌───────────────────────────────────────────────┐
   │  internal/registry  (ядро мультиплексора)      │
   │   - агрегированный каталог tools/resources      │
   │   - таблица маршрутизации name → upstream       │
   │   - fan-out на старте (initialize + *_list)     │
   └───┬───────────────┬───────────────┬────────────┘
       │ JSON-RPC       │               │
       ▼                ▼               ▼
   upstream A       upstream B      upstream C
   (stdio proc)     (stdio proc)    (http, Фаза 2)
   os/exec          os/exec         net/http
```

### 4.1 Модель конкурентности

- **Один клиент** в Фазе 1 (stdio — по сути один pipe). Multi-client — post-MVP.
- **Каждый upstream — своя горутина-читатель** его stdout: демультиплексирует
  ответы по JSON-RPC `id` и доставляет их ожидающим вызовам через `map[id]chan`.
- **Запись в stdin upstream сериализуется** одним мьютексом/каналом на upstream
  (writer не потокобезопасен для конкурентных сообщений).
- **`context.Context`** протянут от `signal.NotifyContext` до каждого upstream:
  отмена по Ctrl-C корректно завершает дочерние процессы (`cmd.Cancel` / kill
  process group).
- Fan-out на старте: `initialize` + `tools/list` + `resources/list` ко всем
  upstream параллельно (`errgroup`), сбор в общий каталог.

### 4.2 Агрегация каталога и неймспейсинг

Проблема: у двух upstream может быть инструмент с одинаковым `name`
(например, оба зовут `search`).

- **Неймспейсинг по умолчанию:** клиент видит `<upstream>__<tool>`
  (`github__search`, `web__search`). Разделитель — `__` (двойное подчёркивание).
  Сверено (Этап 1, `MCP_NOTES.md`): спецификация `2025-06-18` **не ограничивает
  набор символов** для `tool.name` (ограничения символов есть только у ключей
  `_meta`), поэтому `__` валиден; на практике клиенты (напр. Claude Code)
  ожидают `^[a-zA-Z0-9_-]+$` — `<upstream>__<tool>` в него укладывается, если имя
  upstream в конфиге ограничить теми же символами (валидируется в `internal/config`).
- **Таблица маршрутизации** `namespacedName → (upstream, originalName)`.
  При `tools/call` шлюз переписывает `name` обратно в оригинальный перед
  форвардом в upstream.
- `inputSchema` и `description` проксируются **как есть** (клиент должен видеть
  тот же контракт, что даёт upstream).

### 4.3 Проксирование вызова

1. Клиент шлёт `tools/call { name: "github__search", arguments: {...} }`.
2. Registry резолвит `github__search` → (`github`, `search`).
3. Запрос переписывается на `{ name: "search", arguments: {...} }`, ему
   присваивается **новый upstream-side `id`** (id-пространства клиента и upstream
   разделены — шлюз ведёт своё сопоставление).
4. Форвард в stdin upstream `github`; горутина-читатель ловит ответ по id.
5. Результат оборачивается обратно клиентским `id`, пишется клиенту.
6. **Журнал:** пишется `CallRecord` (upstream, method, tool, duration, ok/err).

### 4.4 Обработка ошибок upstream

- Upstream упал/не стартовал → его инструменты **не попадают** в каталог, шлюз
  логирует и продолжает с остальными (не падает целиком).
- Вызов инструмента упавшего upstream → JSON-RPC error клиенту (не паника).
- Таймаут вызова (из конфига) → отмена ожидания, error клиенту, запись в журнал.
- Авто-рестарт упавших upstream — **post-MVP** (в MVP простая изоляция).

---

## 5. Структура репозитория (`internal/`)

| Пакет | Ответственность |
|---|---|
| `cmd/aimcpgate/main.go` | Тонкий: конфиг → логгер → registry → transport, блокировка до сигнала |
| `internal/config` | Парсинг+валидация YAML-конфига, env-expansion секретов, дефолты |
| `internal/registry` | Мультиплексор: upstream-соединения, handshake, агрегированный каталог, маршрутизация |
| `internal/transport` | Клиент-facing сервер: `stdioServer` (Фаза 1), `httpServer` (Фаза 2) за общим `Server` |
| `internal/logging` | Операционный slog + `CallLog` (журнал вызовов, JSON-lines) |
| `internal/mcp` | (Этап 1) JSON-RPC 2.0 типы/фрейминг сообщений (или обёртка над SDK) |
| `internal/upstream` | (Этап 1) реализация одного upstream-соединения: `stdioConn` (os/exec), `httpConn` (Фаза 2) за общим интерфейсом |

Правило зависимостей (перенято из `gitl`): **интерфейс появляется на второй
реализации, не на первой**. Пример: `upstream.Conn` как интерфейс вводится,
когда появляется `httpConn` (Фаза 2), а не на первом `stdioConn`.

Каждая заглушка `internal/*` в Этапе 0 помечена комментарием «Реализация —
Этап N+»; при реализации заменять содержимое, а не плодить параллельные файлы.

---

## 6. Журнал вызовов (добавленная ценность)

- Отдельно от операционного slog: свой writer, **JSON-строки** (одна запись —
  одна строка), append в файл из конфига (или stdout, если файл не задан).
- Запись `CallRecord` (см. `internal/logging/logging.go`): `time, upstream,
  method, tool, client, duration, ok, error`.
- **Потокобезопасность** — мьютекс на writer (много upstream-вызовов на разных
  горутинах).
- **Секреты не логируются** — ни `arguments` целиком (могут содержать токены),
  ни env upstream. По умолчанию логируется только имя инструмента и метаданные;
  логирование payload — отдельный opt-in флаг в post-MVP с явным предупреждением.
- Фаза 2: команда `aimcpgate logs` (чтение/фильтрация JSON-lines) и/или
  минимальный веб-вью.

---

## 7. Развёртывание: Docker Compose — нужен ли?

- **Фаза 1 — не нужен.** Шлюз запускается как локальный stdio-процесс, который
  клиент (Claude Code) стартует сам — как любой другой MCP-сервер. Оборачивать в
  контейнер противоречит модели: клиент должен уметь `exec` бинарь напрямую.
  Дистрибуция — просто бинарь (`go install` / goreleaser-артефакт).
- **Фаза 2 (HTTP-режим) — опционально.** Если пользователь захочет держать
  HTTP/SSE-шлюз постоянно запущенным, `docker-compose.yml` уместен как удобный
  способ поднять сервис (один сервис `aimcpgate serve --transport=http`,
  проброс порта, монтирование конфига и лог-файла). Но upstream stdio-серверам
  нужны их бинарники/интерпретаторы внутри образа — это усложняет образ; для
  части upstream разумнее HTTP-режим.
- **VPS vs $0:** по умолчанию **$0** (локальный процесс). VPS оправдан только
  если понадобится общий постоянный HTTP-эндпоинт (например, доступ с нескольких
  машин). Решение отложено в POST_MVP — для MVP цель $0/мес.

**Вывод:** Docker Compose **не заводим в Фазе 1**; добавляем опциональный
`docker-compose.yml` только в Этапе 5 (Фаза 2 HTTP), если HTTP-режим доведён.

---

## 8. Разбивка по Этапам

Каждый этап — отдельная ветка от `master`, PR в `master` (workflow — CLAUDE.md).

### Этап 0 — Bootstrap (готово в этом коммите)
- `go mod init github.com/akomyagin/aiMCPGate`.
- Скелет `cmd/aimcpgate/main.go` + заглушки `internal/{config,logging,registry,transport}`.
- `go build ./...`, `go vet ./...`, `gofmt` — зелёные.
- Документы `docs/`, `CLAUDE.md`, `SKILL.md`, `README.md`.
- **Готово:** сборка проходит, скелет запускается как no-op до сигнала.

### Этап 1 — Спецификация MCP + ручной JSON-RPC слой
- Прочитать спецификацию, зафиксировать `docs/MCP_NOTES.md`, закрыть развилку §3.
- `internal/mcp`: типы сообщений JSON-RPC 2.0, кодек фрейминга stdio
  (line-delimited JSON), базовые `Request/Response/Notification/Error`.
- Тесты: round-trip кодирование/декодирование, обработка невалидного JSON,
  батчи (если применимо). Table-driven.
- **Готово:** можно сериализовать/парсить MCP-сообщения; развилка §3 закрыта.

### Этап 2 — Один stdio upstream (handshake + каталог)
- `internal/upstream`: `stdioConn` на `os/exec` — запуск дочернего процесса,
  горутина-читатель stdout, демультиплекс по `id`, сериализованная запись stdin.
- `initialize` + `tools/list` + `resources/list` к одному upstream, разбор
  ответа в `[]ToolDescriptor`.
- `internal/config`: парсинг реального YAML, валидация, env-expansion.
- Тесты: fake upstream-процесс (маленький Go-хелпер-бинарь в `testdata/`,
  говорящий по MCP) — детерминированный интеграционный ярус, не только моки.
- **Готово:** шлюз стартует один upstream и получает его каталог.

### Этап 3 — Мультиплексирование 2+ upstream + агрегация (ядро Фазы 1)
- `registry.Start`: параллельный fan-out ко всем enabled upstream (`errgroup`).
- Агрегация каталогов + неймспейсинг `<upstream>__<tool>` + таблица маршрутизации.
- Изоляция ошибок upstream (упавший не роняет шлюз).
- Тесты: два fake-upstream, проверка слияния каталогов, коллизий имён, изоляции.
- **Готово:** каталоги 2+ upstream сливаются в один, коллизии разрешены.

### Этап 4 — Клиент-facing stdio transport + проксирование вызовов + журнал
- `internal/transport`: `stdioServer` — принимает клиента по stdin/stdout,
  отвечает агрегированным `tools/list`, проксирует `tools/call`/`resources/read`.
- Разделение id-пространств клиента и upstream, переписывание имён.
- `internal/logging`: реальный `CallLog` (JSON-lines в файл/stdout), запись
  каждого вызова; секреты не логируются.
- Тесты: end-to-end через fake-клиента и 2 fake-upstream; проверка отсутствия
  секретов в журнале; таймаут вызова.
- **Готово:** **Фаза 1 завершена** — 2+ stdio upstream за одним stdio-эндпоинтом
  с журналом. Реально подключается к Claude Code.

### Этап 5 — Фаза 2: HTTP/SSE transport + HTTP upstream + просмотрщик логов — ГОТОВО
- `httpServer` (клиент-facing) на `net/http` (`internal/transport/http.go`) —
  второй `Server` наравне со `stdioServer`; POST → один `application/json` ответ
  на request, `202` на notification, GET `/mcp` → `405` (SSE server→client не
  открываем в MVP, см. MCP_NOTES §8). Общая MCP-логика вынесена в
  транспорт-независимый `dispatcher` (`internal/transport/dispatch.go`),
  разделяемый обоими транспортами.
- `HTTPConn` (upstream) на Streamable HTTP (`internal/upstream/http.go`) —
  вторая реализация **того же** `registry.Upstream`; отдельный `upstream.Conn`
  осознанно не вводился (уже удовлетворён существующим интерфейсом, MCP_NOTES §8).
  Выбор stdio vs HTTP — по `config.Upstream.ResolveKind()` в `registry.startUpstream`.
  Разбор `application/json` и `text/event-stream`, `Mcp-Session-Id`, авторизация
  через заголовки из конфига (секреты не логируются).
- `internal/config`: реальный парсинг YAML + env-expansion секретов + валидация
  «ровно один из command/url на upstream». CLI переведён на cobra
  (`internal/cli`): `serve`, `logs`, `version`.
- Команда `aimcpgate logs` (`internal/cli/logs.go`) — чтение/фильтрация журнала
  (`--tail`/`--upstream`/`--tool`/`--status`). Веб-вью не делали (план допускает
  «CLI-команда истории ИЛИ веб-вью»).
- Политика доступа и `docker-compose.yml` — осознанно отложены в post-MVP
  (обоснование — MCP_NOTES §8 п.7 и TECHNICAL_PLAN §7).
- **Готово:** **Фаза 2** — альтернативный HTTP-транспорт клиент↔шлюз, HTTP
  upstream-серверы, CLI-просмотрщик журнала. Полный MVP проекта завершён.

### Этап 6 — Релиз-пайплайн ✅
- `.goreleaser.yaml` (linux/darwin/windows × amd64/arm64), `version` через
  `-ldflags`, `SHA256SUMS`; инструкция подключения к Claude Code в README.
- Кросс-компиляция вручную проверена на всех 6 целевых `GOOS/GOARCH` без CGO.
- **Готово:** воспроизводимые бинарные релизы.

---

## 9. Команды разработки

```bash
export PATH="$HOME/sdk/go/bin:$PATH"   # если go не в PATH
go build ./...                         # сборка (зелёная с Этапа 0)
go vet ./...
gofmt -l .                             # пусто = отформатировано
go test -race ./...                    # тесты (где конкурентность — обязателен -race)

go run ./cmd/aimcpgate serve --config ./config.yaml    # Этап 4+: stdio-шлюз
go run ./cmd/aimcpgate logs --tail 50                  # Этап 5+: журнал вызовов
go run ./cmd/aimcpgate version
```

Перед коммитом кода: `go build ./...`, `go test ./...` (с `-race` где есть
конкурентность) и `gofmt` должны проходить; желателен `go vet`.
