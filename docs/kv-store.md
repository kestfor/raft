# KV-хранилище

Raft реплицирует команды, KV применяет committed-записи по порядку на каждом узле.
У каждого процесса свой журнал в памяти. После полного перезапуска кластер пуст. Успешный POST /query означает, что команда подтверждена большинством и применена на сервере,
который ответил клиенту.

Лидер сразу добавляет каждую команду в журнал отдельным вызовом `Append`.
При репликации несколько соседних записей могут попасть в один RPC `AppendEntries`.
Команда крупнее 1 MiB занимает отдельный RPC.

GET использует `ReadIndex`: ждёт commit записи текущего term, затем свежего
подтверждения большинства через AppendEntries и применения безопасного префикса.
GET не добавляет запись в WAL. Изолированный старый лидер не обслуживает чтения
успешно. Обычный heartbeat, отправленный до начала чтения, не подтверждает его.

## Запуск

```sh
make build
bin/raftnode --config configs/local/node1.yaml
bin/raftnode --config configs/local/node2.yaml
bin/raftnode --config configs/local/node3.yaml
```

Узлы запускаются в отдельных терминалах. REST доступен на портах 18001–18003,
Raft gRPC — 19001–19003. `docker compose up --build -d` публикует те же REST-порты,
а gRPC — 9001–9003.

При подключении файлового WAL каждому узлу нужен отдельный каталог. WAL от предыдущей версии
с произвольными командами не является KV-журналом. Для такого перехода укажите
новые директории данных или отдельные Compose volumes; старые данные не удаляются
автоматически. Некорректная committed-команда останавливает KV и сервер.

## raftctl

```sh
bin/raftctl put greeting '"Привет"'
bin/raftctl put count 42
bin/raftctl put document '{"items":[1,true,null]}'
bin/raftctl get document
bin/raftctl --json get document
bin/raftctl log
bin/raftctl status
printf '[1,null]' | bin/raftctl put array --stdin
bin/raftctl put document --file ./document.json
bin/raftctl get document --output ./copy.json
bin/raftctl completion bash
```

Значение аргумента, stdin или файла — JSON. GET без `--json` пишет JSON-значение
без перевода строки; `--json` возвращает объект `{"key": ..., "value": ...}`.
`--output` сохраняет JSON в файл с правами 0600 и несовместим с `--json`.
Команда `log` выводит JSON-массив пользовательских команд локального узла.

По умолчанию клиент обращается к `http://localhost:18001`. `--timeout` ограничивает
весь запрос вместе с redirect; по умолчанию 5 секунд. До пяти переходов 307
сохраняют метод, query и JSON-тело. Автоматического повтора записи после сетевой
ошибки нет. Ошибки выводятся в stderr с ненулевым exit code.

## REST API

Канонический контракт — [api/client_openapi.json](../api/client_openapi.json),
OpenAPI 3.1.0. Сервер встраивает именно этот файл и отдаёт его на `/openapi.json`.

| Метод | Путь | Успех |
| --- | --- | --- |
| POST | `/query` | 200 после commit и apply; реализация возвращает `{"index": N}` |
| GET | `/query?key=...` | 200, объект `{"key": ..., "value": ...}` |
| GET | `/log` | 200, массив таких объектов в порядке индекса журнала |
| GET | `/v1/status` | 200, дополнительный локальный статус узла |

POST принимает `{"key": "name", "value": ...}`. Оба поля обязательны. Значение —
строка, число, массив, объект или `null`; boolean на верхнем уровне контрактом
не разрешён, но допустим внутри массивов и объектов. JSON-типы и точность чисел
сохраняются; преобразования значений в base64 на клиентском API нет.

Ключ POST — строка, включая пустую. GET требует непустой query-параметр `key`.
Отсутствующий ключ возвращает 404. На 64-битных узлах тело POST и закодированная команда Raft ограничены 2 147 418 111 байтами.
Значение внутри команды кодируется в base64. При коротком ключе помещается почти 1,5 GiB JSON.
Размер ключа после JSON-экранирования тоже входит в лимит команды. При превышении сервер возвращает 413.
Перед первой крупной записью обновите все узлы кластера. Ошибки проверки возвращают 422 и `{"detail": [{"loc": [...],
"msg": "...", "type": "..."}]}`. Превышение размера возвращает 413.

```sh
curl -L -H 'Content-Type: application/json' \
  --data '{"key":"greeting","value":"Привет"}' http://localhost:18002/query
curl -L --get --data-urlencode 'key=greeting' http://localhost:18002/query
curl http://localhost:18002/log
curl http://localhost:18002/v1/status
```

Follower перенаправляет `/query` на известного лидера через 307. `/log` и status
обслуживаются локально. `/log` включает все пользовательские записи локального
журнала, в том числе незакоммиченный хвост; служебные `NoOp` исключены. Это
диагностический снимок, а не линейризуемое чтение состояния KV.

Неизвестный лидер или остановленный узел дают 503, таймаут — 504, внутренняя
ошибка — 500. POST может исполниться и после таймаута. Запросы ограничены
сервером пятью секундами. Поля status: `id`, `role`, `term`, `leader_id`,
`last_log_index`, `commit_index`, `applied_index`, `available`.
`available` означает, что узел участвует в протоколе, без гарантии большинства.

DELETE и прежний `/v1/kv/{key}` в клиентский контракт не входят и не обслуживаются.
При использовании дискового хранилища WAL старой KV-версии с произвольными бинарными значениями несовместим с новой
JSON state machine: при переходе используйте отдельную директорию данных.

## Адреса и конфигурация

Конфигурация разделена на три секции. У каждой свой тип и валидация:

```yaml
http:
  listen_address: :18001
  node_urls:
    node1: http://localhost:18001
    node2: http://localhost:18002
    node3: http://localhost:18003

raft:
  node_id: node1
  listen_address: :19001
  peers:
    node2: 127.0.0.1:19002
    node3: 127.0.0.1:19003
  data_dir: data/node1
  election_timeout_min: 500ms
  election_timeout_max: 1s
  heartbeat_interval: 100ms
  rpc_timeout: 200ms

log:
  level: info
  format: plain
```

`http` отвечает за клиентский REST API, `raft` — за идентичность узла, WAL
и межузловой протокол, `log` — за диагностические сообщения процесса.

Имена одинаково устроены в YAML, env и CLI:

| YAML | Environment | CLI |
| --- | --- | --- |
| `http.listen_address` | `HTTP_LISTEN_ADDRESS` | `--http-listen-address` |
| `http.node_urls` | `HTTP_NODE_URLS` | `--http-node-urls` |
| `raft.node_id` | `RAFT_NODE_ID` | `--raft-node-id` |
| `raft.listen_address` | `RAFT_LISTEN_ADDRESS` | `--raft-listen-address` |
| `raft.peers` | `RAFT_PEERS` | `--raft-peers` |
| `raft.data_dir` | `RAFT_DATA_DIR` | `--raft-data-dir` |
| `raft.election_timeout_min` | `RAFT_ELECTION_TIMEOUT_MIN` | `--raft-election-timeout-min` |
| `raft.election_timeout_max` | `RAFT_ELECTION_TIMEOUT_MAX` | `--raft-election-timeout-max` |
| `raft.heartbeat_interval` | `RAFT_HEARTBEAT_INTERVAL` | `--raft-heartbeat-interval` |
| `raft.rpc_timeout` | `RAFT_RPC_TIMEOUT` | `--raft-rpc-timeout` |
| `log.level` | `LOG_LEVEL` | `--log-level` |
| `log.format` | `LOG_FORMAT` | `--log-format` |

Приоритет: defaults → YAML → env → явно переданные flags. Путь к YAML:
`--config` или `RAFTNODE_CONFIG`. Maps в env/CLI передаются как
`node1=http://localhost:18001,node2=http://localhost:18002` и заменяются целиком.

В `http.node_urls` указываются адреса, доступные **клиенту**, а не только соседнему
контейнеру. В `raft.peers` — gRPC-адреса других узлов, доступные самому серверу.
Compose-конфигурации рассчитаны на REST-клиент на Docker host. Для другой машины
или контейнера измените публичные origins. Без URL лидера follower ответит 503.

Старый плоский YAML и короткие флаги `--listen`, `--id`, `--peers` больше не
поддерживаются; неизвестные YAML-поля и CLI-флаги вызывают ошибку. Старые env-имена
также нужно заменить по таблице выше.

## Swagger / OpenAPI

Swagger UI доступен на `http://localhost:18001/swagger/`, спецификация —
на `/openapi.json`. Оба маршрута локальны и работают также на follower.
Swagger использует канонический OpenAPI 3.1.0 из `api/client_openapi.json`.

Спецификация встроена в Go-бинарник через embed и доступна без интернета.
UI использует Swagger UI 5.33.3 с CDN jsDelivr, поэтому браузеру нужен интернет
для загрузки JS/CSS. Внешняя отправка спецификации на validator отключена.
Для Try it out открывайте UI лидера: браузер может блокировать redirect между
разными origins; это ограничение CORS не касается raftctl и curl.

gRPC остаётся транспортом Raft. Сырой gRPC Propose в KV-сервере отклоняется, чтобы
команды не обходили проверку KV. API не включает аутентификацию или TLS; примеры
предназначены для локального запуска.

## Границы компонентов

- `pkg/raft`: выборы, журнал, commit, последовательный `Do`, ожидание apply и `ReadIndex`.
- `internal/kv`: JSON-команды, map и реализация `StateMachine.Do`.
- `cmd/raftnode/raft.go`: адаптер Raft к интерфейсу KV и запрет сырого Propose.
- `internal/api/http`: HTTP, redirect и преобразование ошибок, без импорта Raft.
- `pkg/kvclient`: REST-клиент и переходы на лидера.
- `cmd/raftctl`: Cobra, ввод/вывод и оформление.
- `cmd/raftnode`: сборка компонентов и согласованная остановка.

## Ограничения и проверки

Нет snapshots, компакции, lease или дедупликации клиентских запросов. Объём журнала в памяти растёт. Ошибка или таймаут записи не означают её отмену: команда
может позже примениться. Повтор после потери ответа может создать новую запись.

`make test` запускает race detector. Тесты покрывают ожидание apply, безопасные
чтения, старые RPC-ответы, разделение сети, смену лидера, полный рестарт из WAL,
redirect JSON-тела и работу ctl с файлами и stdin/stdout. `make lint`
проверяет код линтером.

## Acceptance на pytest

Требуются Python 3.10+ и Go; для дополнительных проверок через внешние инструменты
нужны `curl` и `grpcurl`. Python-зависимости закреплены в `tests/requirements.txt`.

```sh
make python-setup       # создать .venv, установить pytest и плагины
make acceptance-all    # собрать бинарники, поднять 3 ноды, проверить, остановить

make cluster-up        # отдельно поднять управляемый тестовый кластер
make cluster-status    # адреса, PID, роли и commit/applied
make cluster-logs      # последние строки логов
make acceptance        # проверить уже работающий кластер, оставить его запущенным
make cluster-down      # остановить ноды и TCP-прокси; данные KV будут потеряны
```

В `acceptance-all` lifecycle принадлежит pytest-фикстуре: cleanup выполняется и
при падении теста. В режиме `acceptance` адреса и управление берутся из
`.artifacts/test-cluster.json` (переопределяется через `CLUSTER_STATE`).
Приватный control API доступен только на loopback и защищён случайным токеном;
state-файл доступен только владельцу. Не используйте этот файл как конфиг приложения.

Сценарии разделены по обязанностям:

- `tests/acceptance/test_api.py`: HTTP, redirect, лимиты, конкурентные запросы.
- `tests/acceptance/test_cli.py`: ввод/вывод raftctl, файлы, JSON, curl и grpcurl.
- `tests/acceptance/test_failures.py`: SIGKILL, разделение сети, кворум и запуск пустого кластера после полного перезапуска.
- `conftest.py`: подготовка, восстановление кластера между сценариями и диагностика.
- `cluster.py`: только инфраструктура процессов, TCP-прокси и lifecycle CLI.

```sh
make acceptance-all PYTEST_ARGS='-m api -v'
make acceptance PYTEST_ARGS='-m failure -v'
make acceptance-all PYTEST_ARGS='-k redirect'
```

Тесты используют отдельные временные данные и свободные локальные порты.
Сценарии выполняются последовательно: fault-тесты изменяют состояние общего
кластера, поэтому `pytest-xdist` здесь не используется. После каждого сценария
сеть восстанавливается, остановленные ноды запускаются и догоняют журнал.
Таймаут каждого теста — 60 секунд. Логи нод прикладываются к выводу падения.
Проверки `external` явно пропускаются, если curl/grpcurl не установлены.

Результаты: `.artifacts/acceptance.html` (самодостаточный HTML) и
`.artifacts/acceptance.xml` (JUnit для CI). Старый `scripts/acceptance.py` заменён
этими сценариями и Make-командами.

## raftbench

`make build` также собирает `bin/raftbench` — нагрузочный CLI на Cobra.

```sh
bin/raftbench --endpoint http://localhost:18003 -n 1000 -c 8 -d 256
bin/raftbench -t get,put,mixed --read-percent 80 --repeat 3
bin/raftbench -t mixed -c 32 --json > results.json
bin/raftbench --endpoint http://localhost:18003 -t put -n 100000 -c 512 --warmup 100
make bench ENDPOINT=http://localhost:18003 BENCH_ARGS='-n 1000 -c 8'
```

- `-n, --requests`: число измеряемых операций на профиль и повтор.
- `-c, --clients`: параллельные HTTP-клиенты (1–1024), по одному запросу в работе
  на клиента. Рост конкуренции помогает заполнять пачки, но также может
  увеличивать latency; производительность нового JSON API измеряется отдельно.
- `-d, --size`: размер содержимого JSON-строки в bytes (без кавычек); `-r, --keyspace`: число ключей.
- `-t, --tests`: `get`, `put`, `mixed`; `--read-percent`: доля GET в mixed.
- `--warmup`: число операций прогрева перед каждым профилем.
- `--repeat`: число повторов набора профилей.
- `--timeout`: таймаут одного запроса, включая redirect.
- `--json`: структурированный результат без ANSI и progress-вывода.
- `--color auto|always|never`: цвет заголовков, метрик, ошибок и progress bar.
  По умолчанию цвет включён только в терминале; `NO_COLOR` отключает его даже
  при `--color always`. JSON всегда остаётся чистым.

Создаётся уникальный префикс `raftbench/<run-id>/`; пользовательские ключи не
меняются. Ключи остаются после теста, WAL растёт. Подготовка ключей, прогрев и
запросы status исключены из измеряемого времени. GET проверяет полученное значение.
HTTP-соединения переиспользуются; размер пула соответствует числу workers.

Отчёт показывает successful requests/sec, число ошибок, p50/p95/p99/max и
полезные MiB/sec (значения, без HTTP/JSON/WAL overhead). Перцентили вычисляются
точно методом nearest-rank по успешным операциям; ошибки считаются отдельно.
В mixed дополнительно показываются метрики GET и PUT. JSON содержит mean/min,
классы ошибок, параметры запуска, среду и status до/после каждого профиля.
При ошибках запросов exit code ненулевой, но измеренный результат сохраняется.
Ошибка подготовки или прогрева прекращает дальнейшую нагрузку; уже полученные
измерения сохраняются в JSON вместе с полем `error`.

Нагрузка closed-loop: следующий запрос worker начинает только после предыдущего.
Это не open-loop тест заданного RPS и не модель задержек очереди внешних клиентов.
Для исключения redirect overhead используйте endpoint лидера. Для сравнения
результатов сохраняйте размер значения, keyspace, конкуренцию и условия запуска.
Не запускайте fault-тесты или сборку одновременно с performance-замером.

## Make-команды

`make` без аргументов показывает сгруппированную справку; `make help` делает то же.

| Задача | Команда |
| --- | --- |
| Подготовить Go-бинарники и Python venv | `make setup` |
| Собрать всё / отдельный инструмент | `make build`, `make build-node`, `make build-ctl`, `make build-bench` |
| Запустить одну ноду в foreground | `make node CONFIG=configs/local/node1.yaml` |
| Поднять / посмотреть / остановить управляемый кластер | `make cluster-up`, `make cluster-status`, `make cluster-down` |
| Перезапустить ноды с теми же конфигами и пустым KV | `make cluster-restart` |
| Найти запущенные ноды и контроллеры текущего пользователя | `make cluster-ps` |
| Узнать REST URL лидера | `make -s cluster-endpoint` |
| Логи / следить за логами | `make cluster-logs`, `make cluster-logs LOG_ARGS=--follow` |
| Вызвать ctl через лидера управляемого кластера | `make ctl CTL_ARGS='put greeting hello'` |
| Benchmark через лидера управляемого кластера | `make bench BENCH_ARGS='-t put -n 100000 -c 512'` |
| Проверки Go | `make format`, `make lint`, `make vet`, `make test`, `make check` |
| Acceptance: существующий / временный кластер | `make acceptance`, `make acceptance-all` |
| Docker Compose | `make docker-up`, `make docker-status`, `make docker-logs`, `make docker-down` |
| Protobuf | `make proto-lint`, `make proto` |
| Удалить бинарники и pytest cache | `make clean` |

`make ctl` и `make bench` находят лидера через state-файл. Для любого другого
кластера задайте `ENDPOINT=http://host:port`. `CLUSTER_STATE` выбирает конкретный
управляемый кластер; старое имя `TEST_CLUSTER_STATE` сохранено как alias.
`GO_TEST_ARGS`, `PYTEST_ARGS`, `CTL_ARGS`, `BENCH_ARGS`, `NODE_ARGS` передают
дополнительные параметры соответствующим инструментам.

Для поиска и остановки раннего acceptance-кластера, поднятого старым скриптом:

```sh
make cluster-ps
make cluster-status CLUSTER_STATE=/tmp/raft-acceptance-20261102/cluster.json
make cluster-down CLUSTER_STATE=/tmp/raft-acceptance-20261102/cluster.json
```

Legacy-остановка сначала проверяет команду контроллера, каталог и принадлежность
PID нод; случайные/повторно использованные PID не завершаются. Для нового
управляемого кластера остановка проходит через его приватный control API.
Старый пустой startup-маркер `{}` автоматически убирается при `cluster-up`
только спустя 30 секунд и при отсутствии соответствующего контроллера.

После остановки всех узлов данные KV теряются. `cluster-restart` запускает пустой кластер с прежними конфигами. Старые команды
`up/down/logs` остаются alias для Docker, `test-cluster-up/down` — для локального
менеджера; в новых сценариях лучше использовать явные имена.
