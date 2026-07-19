# aiMCPGate

*English version — [README.md](README.md).*

Шлюз / прокси для **MCP-серверов** (Model Context Protocol) на Go. Presents
себя MCP-клиенту (Claude Code, Cursor и др.) как **один** MCP-сервер, а под
капотом **мультиплексирует** вызовы к нескольким upstream MCP-серверам,
**агрегирует** их каталоги инструментов/ресурсов в один и **логирует** каждый
вызов.

> Статус: **MVP завершён (Этапы 0–6)**. Фаза 1 — мультиплексирование
> stdio-upstream за stdio-эндпоинтом с журналом; Фаза 2 — HTTP/SSE-транспорт
> клиент↔шлюз, HTTP-upstream, CLI-просмотрщик журнала (`mcp-gate logs`);
> релиз-пайплайн (`goreleaser`, кросс-компиляция linux/darwin/windows ×
> amd64/arm64 без CGO).

## Релизы

Кросс-платформенные бинарники собираются через [`goreleaser`](https://goreleaser.com)
(`.goreleaser.yaml`): `linux`/`darwin`/`windows` × `amd64`/`arm64`, без CGO,
версия вшивается через `-ldflags -X main.version=...`, чек-суммы в `SHA256SUMS`.
Локальный прогон: `goreleaser release --snapshot --clean`.

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

## MVP (две фазы)

- **Фаза 1** — мультиплексирование 2+ **stdio** upstream за одним **stdio**
  эндпоинтом (тот же транспорт, что видит Claude Code) + базовое логирование.
- **Фаза 2** — **HTTP/SSE** транспорт, HTTP upstream-серверы, просмотрщик
  журнала (CLI/веб), опционально политика доступа.

## Сборка

```bash
export PATH="$HOME/sdk/go/bin:$PATH"   # если go не в PATH
go build ./...
go vet ./...
go test -race ./...

go run ./cmd version
```

## Использование

```bash
# stdio-режим (клиент запускает шлюз как подпроцесс):
mcp-gate serve --config ./config.yaml

# http-режим (transport: http в конфиге) — эндпоинт на http://<listen_addr>/mcp:
mcp-gate serve --config ./config-http.yaml

# просмотр журнала вызовов (последние 50; фильтры по upstream/tool/статусу):
mcp-gate logs --file ./logs/calls.jsonl --tail 50
mcp-gate logs --config ./config.yaml --upstream github --status err

# сгенерировать случайный auth-токен (для HTTP-транспорта) и подсказку, куда его вписать:
mcp-gate token --generate
# показать auth-токен, заданный в конфиге:
mcp-gate token --config ./config-http.yaml

# напечатать готовые сниппеты конфига MCP-клиента (Claude Code / Cursor); требует
# transport: http в конфиге, добавляет заголовок Bearer, если задан auth_token:
mcp-gate client-config --config ./config-http.yaml

# напечатать SKILL.md, обучающий агента работе с агрегированным каталогом
# (по умолчанию встроенный текст; переопределяется через skill_file в конфиге):
mcp-gate skill > .claude/skills/mcp-gate/SKILL.md
```

Все команды, кроме `token --generate` и `skill` (та откатывается на встроенный
текст), загружают конфиг: передайте `--config` или положите `config.yaml` рядом
с бинарём (см. «Конфигурация» ниже).

## Перезагрузка конфига (SIGHUP)

Шлюз перечитывает конфигурацию на лету по **SIGHUP** — без перезапуска и без
обрыва клиентского соединения. Отредактируйте `config.yaml` и пошлите сигнал:

```bash
kill -HUP $(pgrep -f 'mcp-gate serve')
```

При перезагрузке шлюз сравнивает новый конфиг с работающими upstream и
применяет минимально необходимое изменение: добавленные upstream запускаются,
удалённые (или переведённые в `enabled: false`) — останавливаются, upstream с
изменёнными полями запуска (`command`/`args`/`url`/`env`/`headers`) —
перезапускаются, а те, у которых поменялся только фильтр инструментов
`allow`/`deny`/`rename`, — перепроецируются вообще без перезапуска.
Неизменённые upstream продолжают работать нетронутыми. Ошибочная правка
(битый YAML, непройденная валидация) логируется и игнорируется — продолжает
действовать текущий конфиг, так что опечатка никогда не уронит шлюз.

**Поведенческое замечание:** поскольку шлюз ставит обработчик SIGHUP, этот
сигнал больше не завершает процесс, как это делал бы дефолт ОС. Для остановки
шлюза используйте Ctrl-C, SIGINT или SIGTERM. SIGHUP существует только в Unix;
на Windows его нет, и перезагрузка недоступна (процесс обслуживает конфиг, с
которым был запущен, до перезапуска).

## Конфигурация

Без `--config` шлюз ищет `config.yaml` **рядом со своим бинарём** (например,
если `mcp-gate` установлен в `/etc/gate/`, он ищет `/etc/gate/config.yaml` —
независимо от того, из какого каталога его запустили). Если такого файла нет
и `--config` не передан — явная ошибка вместо запуска пустого шлюза.
Относительные пути внутри конфига (`log_file`, `skill_file`) резолвятся
относительно каталога **самого конфига**, не текущего рабочего каталога.

> Примечание: поиск «рядом с бинарём» опирается на путь запущенного
> исполняемого файла. При `go run ./cmd ...` это одноразовая сборка во временном
> каталоге, поэтому дефолтный поиск `config.yaml` там не сработает — при `go run`
> передавайте `--config` явно либо запускайте собранный бинарь.

Полный пример со всеми полями — [`config.example.yaml`](config.example.yaml).
Список upstream-серверов задаётся в YAML; **секреты (токены) — через env/`.env`**
(подстановка `${VAR}` при загрузке), никогда не в конфиге под git. На upstream
указывается **ровно один** из `command` (stdio-подпроцесс) или `url`
(HTTP-сервер, Streamable HTTP) — вид соединения выводится автоматически.

```yaml
transport: stdio            # stdio (Фаза 1) | http (Фаза 2)
listen_addr: "127.0.0.1:28080"  # только для transport: http; по умолчанию loopback
# auth_token: ${AIMCPGATE_TOKEN}  # обязателен, если listen_addr шире loopback
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

## Лицензия

MIT — см. [`LICENSE`](LICENSE).
