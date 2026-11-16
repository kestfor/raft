# Клиентское HTTP API

Контракт: [api/client_openapi.json](api/client_openapi.json). Описание реализации,
запуска и CLI: [docs/kv-store.md](docs/kv-store.md).

```sh
curl -L -H 'Content-Type: application/json' \
  --data '{"key":"counter","value":42}' http://localhost:18001/query
curl -L --get --data-urlencode 'key=counter' http://localhost:18001/query
curl http://localhost:18001/log
```

`POST /query` принимает обязательные `key` и `value`. JSON-значение сохраняет тип:
строка, число, массив, объект или null. `GET /query?key=counter` возвращает
`{"key":"counter","value":42}`. Успешный POST подтверждает commit и применение.

`GET /log` возвращает пользовательские записи локального Raft-журнала по порядку,
без NoOp, включая незакоммиченный хвост. Запрос работает на любой роли без redirect.
Ошибки проверки POST и query-параметров GET возвращают 422 в формате из OpenAPI.

Спецификация доступна на `/openapi.json`, Swagger UI — `/swagger/`.
Дополнительный `/v1/status` показывает локальное состояние узла.
Сырой Propose и прежние бинарные маршруты `/v1/kv/{key}` в KV-сервере недоступны.
