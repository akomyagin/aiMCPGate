# aiMCPGate

Шлюз / прокси для **MCP-серверов** (Model Context Protocol) на Go. Presents
себя MCP-клиенту (Claude Code, Cursor и др.) как **один** MCP-сервер, а под
капотом **мультиплексирует** вызовы к нескольким upstream MCP-серверам,
**агрегирует** их каталоги инструментов/ресурсов в один и **логирует** каждый
вызов.

> Статус: **MVP готов (Этапы 0–5)**. Фаза 1 — мультиплексирование stdio-upstream
> за stdio-эндпоинтом с журналом; Фаза 2 — HTTP/SSE-транспорт клиент↔шлюз,
> HTTP-upstream, CLI-просмотрщик журнала (`aimcpgate logs`). Осталось: Этап 6
> (релиз-пайплайн goreleaser). Разбивка по этапам —
> [`docs/TECHNICAL_PLAN.md`](docs/TECHNICAL_PLAN.md) §8.

## Зачем

У активного пользователя MCP подключено несколько серверов (filesystem, github,
поиск, кастомные), и каждый прописан в конфиге каждого клиента по отдельности.
`aiMCPGate` даёт:

- **Одну точку входа** — один MCP-эндпоинт вместо N в конфиге клиента.
- **Единый каталог** — инструменты всех upstream-серверов сведены вместе (с
  неймспейсингом `<upstream>__<tool>`, чтобы имена не сталкивались).
- **Журнал вызовов** — какой upstream, что вызвано, когда, успех/ошибка. Это и
  есть добавленная ценность поверх «просто прокси».

Соло pet-проект: приоритет — обучение Go (конкурентность, `os/exec`,
JSON-RPC 2.0, транспорты stdio и HTTP/SSE). Расходы — **$0/мес** по умолчанию
(локальный процесс), без телеметрии.

## Как это работает (кратко)

```
MCP-клиент ──stdio/HTTP──▶ aiMCPGate ──JSON-RPC──▶ upstream A (stdio)
                              │        ├─────────▶ upstream B (stdio)
                          журнал       └─────────▶ upstream C (http, Фаза 2)
                          вызовов
```

Подробнее — [`docs/TECHNICAL_PLAN.md`](docs/TECHNICAL_PLAN.md) §4.

## MVP (две фазы)

- **Фаза 1** — мультиплексирование 2+ **stdio** upstream за одним **stdio**
  эндпоинтом (тот же транспорт, что видит Claude Code) + базовое логирование.
- **Фаза 2** — **HTTP/SSE** транспорт, HTTP upstream-серверы, просмотрщик
  журнала (CLI/веб), опционально политика доступа.

Границы — [`docs/PLAN.md`](docs/PLAN.md); за пределами MVP —
[`docs/POST_MVP_PLAN.md`](docs/POST_MVP_PLAN.md).

## Сборка

```bash
export PATH="$HOME/sdk/go/bin:$PATH"   # если go не в PATH
go build ./...
go vet ./...
go test -race ./...

go run ./cmd/aimcpgate version
```

## Использование

```bash
# stdio-режим (клиент запускает шлюз как подпроцесс):
aimcpgate serve --config ./config.yaml

# http-режим (transport: http в конфиге) — эндпоинт на http://<listen_addr>/mcp:
aimcpgate serve --config ./config-http.yaml

# просмотр журнала вызовов (последние 50; фильтры по upstream/tool/статусу):
aimcpgate logs --file ./logs/calls.jsonl --tail 50
aimcpgate logs --config ./config.yaml --upstream github --status err
```

## Конфигурация

Полный пример со всеми полями — [`config.example.yaml`](config.example.yaml).
Список upstream-серверов задаётся в YAML; **секреты (токены) — через env/`.env`**
(подстановка `${VAR}` при загрузке), никогда не в конфиге под git. На upstream
указывается **ровно один** из `command` (stdio-подпроцесс) или `url`
(HTTP-сервер, Streamable HTTP) — вид соединения выводится автоматически.

```yaml
transport: stdio            # stdio (Фаза 1) | http (Фаза 2)
listen_addr: ":8080"        # только для transport: http
log_file: ./logs/calls.jsonl
upstreams:
  - name: filesystem        # stdio-upstream
    command: npx
    args: ["-y", "@modelcontextprotocol/server-filesystem", "/home/user"]
    enabled: true
  - name: github
    command: github-mcp-server
    env:
      GITHUB_TOKEN: ${GITHUB_TOKEN}   # из окружения, не хардкод
    enabled: true
  - name: remote            # http-upstream (Фаза 2)
    url: https://mcp.example.com/mcp
    headers:
      Authorization: "Bearer ${REMOTE_MCP_TOKEN}"   # секрет, не логируется
    enabled: true
```

## Документация

- [`docs/PLAN.md`](docs/PLAN.md) — продуктовое видение и границы MVP.
- [`docs/TECHNICAL_PLAN.md`](docs/TECHNICAL_PLAN.md) — стек, архитектура
  мультиплексора, разбивка по Этапам.
- [`docs/POST_MVP_PLAN.md`](docs/POST_MVP_PLAN.md) — идеи за пределами MVP.
- [`CLAUDE.md`](CLAUDE.md) — инструкции и workflow для AI-сессий.

## Лицензия

MIT — см. [`LICENSE`](LICENSE).
