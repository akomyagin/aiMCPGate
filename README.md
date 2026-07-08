# aiMCPGate

Шлюз / прокси для **MCP-серверов** (Model Context Protocol) на Go. Presents
себя MCP-клиенту (Claude Code, Cursor и др.) как **один** MCP-сервер, а под
капотом **мультиплексирует** вызовы к нескольким upstream MCP-серверам,
**агрегирует** их каталоги инструментов/ресурсов в один и **логирует** каждый
вызов.

> Статус: **Этап 0 (bootstrap)**. Скелет собирается и запускается как no-op;
> протокольная логика реализуется с Этапа 1. Разбивка по этапам —
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

## Конфигурация (проектируется, Этап 1+)

Список upstream-серверов задаётся в YAML; **секреты (токены) — через env/`.env`**,
никогда не в конфиге под git:

```yaml
transport: stdio            # stdio (Фаза 1) | http (Фаза 2)
log_file: ./logs/calls.jsonl
upstreams:
  - name: filesystem
    kind: stdio
    command: npx
    args: ["-y", "@modelcontextprotocol/server-filesystem", "/home/user"]
    enabled: true
  - name: github
    kind: stdio
    command: github-mcp-server
    env:
      GITHUB_TOKEN: ${GITHUB_TOKEN}   # из окружения, не хардкод
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
