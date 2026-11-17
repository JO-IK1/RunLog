# RunLog

Сервис для хранения событий запуска: сборка, тесты, завершение проверки.
Источник событий — любой HTTP-клиент. Для примера есть Bash-скрипт, поэтому
подключать CppDefense для первого запуска не нужно.

## Как устроено

```text
клиент → API → Kafka → consumer → MongoDB
          ├→ PostgreSQL: проекты и ключи
          └→ MongoDB: чтение истории
```

Один Go-модуль, два долгоживущих процесса. Та же программа выполняет
административные команды. HTTP написан на net/http, PostgreSQL — pgx,
Kafka — franz-go, MongoDB — официальный Go-драйвер v2.

В маленьком сервисе хватило бы PostgreSQL. Здесь разделение намеренное:
проект позволяет разобраться с брокером, документной БД и повторной доставкой.

## Быстрый запуск

Нужны Docker с Compose v2. Для демонстрационного скрипта — Bash, curl и Python 3.

```bash
docker compose up --build -d
docker compose logs -f api consumer
```

Дождаться запуска API, затем в другом терминале:

```bash
curl --fail http://localhost:8080/health/ready
docker compose exec api runlog project-create "CppDefense"
```

Команда печатает project_id и api_key. Ключ показывается только при создании.
Скопировать его и отправить события:

```bash
export RUNLOG_KEY='rl_вставить_выданный_ключ'
bash scripts/demo.sh
```

Остановка без удаления данных: `docker compose down`.
`docker compose down -v` также удаляет все данные стенда.
Порт API привязан к 127.0.0.1. Базы и Kafka наружу не опубликованы.
Пароли в Compose предназначены только для локального учебного окружения.

## HTTP API

Для всех /v1 маршрутов нужен `Authorization: Bearer <api_key>`.
Проект определяется по ключу: передать project_id в теле нельзя.

### Отправка события

```bash
curl -i http://localhost:8080/v1/events \
  -H "Authorization: Bearer $RUNLOG_KEY" \
  -H 'Content-Type: application/json' \
  -d '{
    "event_id":"event-001",
    "run_id":"submission-1042",
    "type":"build.failed",
    "occurred_at":"2026-11-14T10:00:00Z",
    "message":"Compilation failed",
    "data":{"compiler":"g++","exit_code":1}
  }'
```

Ответ: `202 {"event_id":"event-001","status":"accepted"}`.
Это подтверждение Kafka, запись в истории появится позже.
Если ответ потерялся или пришла ошибка публикации, повторить тот же event_id:
таймаут не доказывает, что Kafka не получила сообщение.

Типы: run.queued, build.started, build.failed, tests.finished, run.finished.
Последовательность не проверяется, событие run.finished не запрещает поздние записи.
ID — 1–128 байт, message — до 8192 байт, запрос — до 64 КиБ.
Data — произвольный JSON-объект. Неизвестные верхнеуровневые поля отклоняются.
Время нормализуется до миллисекунд, поскольку такую точность хранит BSON Date.
API добавляет project_id, received_at и schema_version=1.

Ошибки: 400 — некорректные данные; 401 — неверный/отозванный ключ;
413 — слишком большой запрос; 415 — неверный Content-Type;
503 — зависимость недоступна или публикация не подтверждена.
Тело ошибки: `{"error":"описание"}`.

### История

```bash
curl -sS -H "Authorization: Bearer $RUNLOG_KEY" \
  'http://localhost:8080/v1/runs/submission-1042/events?limit=20&type=build.failed'
```

Параметры: limit (1–100, по умолчанию 50), type, from и to (RFC3339,
границы включены), cursor. Ответ: events и, если есть следующая страница,
next_cursor. Передать его как cursor с прежними фильтрами. Курсор кодируется
base64url, привязан к проекту, запуску и фильтрам; он не является секретом.
Сортировка: occurred_at ASC, event_id ASC. Отсутствующая история — пустой массив.
Это не снимок данных: позднее событие может попасть перед уже прочитанным курсором.

`GET /health/live` проверяет процесс; `/health/ready` — PostgreSQL, MongoDB и Kafka.
Недоступная MongoDB делает readiness отрицательным, но работающий API всё ещё
может принимать события в Kafka.

## Проекты и ключи

```bash
docker compose exec api runlog project-create "Another project"
docker compose exec api runlog project-revoke PROJECT_ID
```

Отзыв отключает все активные ключи проекта. Ключи состоят из 32 случайных байт,
в PostgreSQL хранится SHA-256. Проверка идёт на каждом запросе, без кеша.
Публичного API управления проектами и пользовательских аккаунтов пока нет:
административный доступ — через CLI внутри контейнера.

Миграция выполняется отдельным Compose-сервисом до запуска API.
Сервис kafka-permissions однократно подготавливает права корня Kafka-тома;
сам брокер работает от пользователя образа (UID 1000). Текущая схема
в internal/runlog/postgres.go идемпотентна; полноценного версионирования миграций
в этой версии нет. Индексы MongoDB создаёт consumer до чтения Kafka.

## Доставка и ошибки

- Kafka key — JSON-пара [project_id, run_id]. События запуска идут в одну partition.
- Producer ждёт подтверждение all ISR; идемпотентность franz-go оставлена включённой.
  Ожидание HTTP ограничено 10 секундами независимо от завершения доставки.
  Буфер producer ограничен 1000 записями и 16 МиБ; при заполнении API возвращает 503.
- Consumer читает по одному сообщению, сохраняет документ и вручную коммитит offset.
- Уникальный индекс (project_id, event_id) и upsert с setOnInsert дают first-write-wins.
  Повтор с другим содержимым не перезаписывает первую запись.
- При ошибке MongoDB consumer повторяет запись с задержкой 1–8 секунд.
  До успешной записи offset не подтверждается.
- Повреждённый JSON или неизвестная версия останавливают consumer с ошибкой.
  Compose перезапустит его, сообщение останется заблокированным до вмешательства.
  Отдельного топика ошибок пока нет.
- При SIGTERM API даёт текущим HTTP-запросам до 15 секунд. Consumer отменяет
  текущую операцию; незакоммиченная запись может прийти снова.

Гарантия — at-least-once с идемпотентной записью, не сквозная exactly-once.
Дедупликация действует, пока документ хранится в MongoDB.
Kafka хранит сообщения 7 дней: если consumer отстанет дольше retention,
данные могут быть удалены до обработки. На стенде один брокер и по одному
экземпляру БД, поэтому устойчивости к потере диска здесь нет.
Длительная недоступность MongoDB может вызвать выход из consumer group после
max poll interval; повторная обработка безопасна благодаря дедупликации.

## Проверки

Код следует [Google Go Style Guide](https://google.github.io/styleguide/go/guide),
[Google Python Style Guide](https://google.github.io/styleguide/pyguide.html)
и [Shell Style Guide](https://google.github.io/styleguide/shellguide.html).
Go форматируется через `gofmt`, Python — YAPF с профилем `google`.
Настройки отступов для редактора находятся в `.editorconfig`.

Установка форматтера и команды форматирования:

```bash
python3 -m venv .venv
. .venv/bin/activate
python3 -m pip install -r requirements-dev.txt
make fmt
make fmt-check
```

CI проверяет форматирование Go и Python перед тестами.

```bash
go test -race ./...
go vet ./...
go build ./...
docker compose config --quiet
bash scripts/smoke.sh
```

Smoke-тест выполняется на работающем локальном стенде. Проверяет доступ,
валидацию, дубли, пагинацию с одинаковыми timestamp, фильтр, повторное чтение
Kafka с начала, остановку MongoDB/Kafka/consumer и отзыв ключа.
Он создаёт тестовые проекты и останавливает зависимости — не запускать на
общем окружении. В finally зависимости запускаются обратно.

GitHub Actions запускает unit-тесты, vet, сборку и отдельный Compose smoke-тест.

## Конфигурация

Переменные перечислены в .env.example. Go-процесс сам .env не читает.
В Compose адреса зависимостей заданы внутренними именами сервисов.
Для запуска Go вне контейнера нужно отдельно обеспечить доступные адреса
PostgreSQL, MongoDB и Kafka с корректным advertised listener.

Структура:

```text
cmd/runlog/            точка входа и завершение процессов
internal/runlog/       события, API, Kafka, PostgreSQL, MongoDB
scripts/              генератор и сквозная проверка
.github/workflows/    CI
```

## Что можно добавить дальше

TTL по received_at, список запусков, вычисление итогового статуса, ротацию
ключей, метрики задержки и consumer lag, нагрузочный сценарий и интеграцию
с CppDefense. Сейчас frontend, TLS termination, rate limiting, DLQ и
продакшен-развёртывание в проект не входят.
