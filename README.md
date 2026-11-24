# Стенд репетиции обновления Zammad 3.3.0 → 7.2.3

Комплект для прогона обновления на копии боевой базы: снимаем горячий дамп
продовой PostgreSQL, разворачиваем на стенде и ведём по цепочке
3.3.0 → 4.1.0 → 5.4.1 → 6.5.0 → миграция вложений в S3 → 7.2.3 (прыжок
проверен, промежуточные 7.0/7.1 — запасной путь). Прод при этом работает
без остановки. Боевой план — в
[zammad_prod_runbook](../zammad_prod_runbook.md), здесь — порядок команд
на стенде.

## Состав

| Файл | Назначение |
|-|-|
| `docker-compose.yml` | Базовый стенд: Zammad **3.3.0**, PostgreSQL 18, Elasticsearch 7.6.1 (образ Zammad с ingest-attachment), memcached, nginx на порту **8080** |
| `stages/override-4.1.yml` | Ступень 3.3 → **4.1.0**: новые теги образов, nginx внутри переезжает на порт 8080 |
| `stages/override-5.4.yml` | Ступень 4.1 → **5.4.1**: теги + апгрейд ES до 7.17.3 (образ с ingest-attachment) |
| `stages/docker-compose.6.5.yml` | Ступень 5.4 → **6.5.0**: новый стек (единый образ `ghcr.io/zammad/zammad`, Redis, MinIO, volume `zammad-storage`) |
| `stages/docker-compose.7.yml` | Ступени **7.0 / 7.1 / 7.2**: версии задаются переменными `ZAMMAD_IMAGE` и `ES_IMAGE` |
| `stages/Dockerfile.es` | Запасной вариант сборки ES с ingest-attachment |
| `smoke_test.sh` + `Dockerfile` + `entrypoint.sh` | Смоук-тест 6/6: API, клиент, тикет с вложением, сверка вложения, поиск в ES |
| `backup/` | Дампы `pg_dump -Fc` перед каждой ступенью |

Все команды — из каталога `zammad-33-stand`.

## 0. Горячий дамп прода и развёртывание на стенде

### 0.1 Дамп на проде — без остановки сервиса

`pg_dump` — горячий бэкап: PostgreSQL открывает MVCC-снимок на момент старта
и спокойно читает его, а продолжает принимать тикеты, писать в базу и
отвечать агентам. Блокировок нет, стопить сервер не нужно:

```bash
# На прод-хосте (PG внутри контейнера старого Zammad):
docker exec <prod-pg-container> \
  pg_dump -U zammad -Fc zammad_production > prod-$(date +%F).dump
```

Нюансы при 450+ ГБ:

- Дамп идёт часами — запускать ночью. Нагрузка — чтение диска и одно ядро
  CPU; при желании смягчить: `ionice -c 3 nice -n 19 docker exec …`.
- Ускорение в разы — параллельный формат: `pg_dump -Fd -j 8 -f /backup/prod-dump`
  (каталог вместо одного файла; таблица `stores` распараллелится по страницам).
- Снимок консистентен на момент старта дампа: тикеты, созданные во время
  дампа, в него не попадут. Для репетиции это не важно; для боевого
  переключения дельту доносим отдельно — схема со срезами `stores` и
  коротким окном описана в этапе 1 ранбука.
- Elasticsearch не бэкапим: индексы пересоздаются реиндексом после каждой
  ступени.

### 0.2 Развёртывание дампа на стенде

```bash
# Чистый стенд (если поднимался раньше — сносим вместе с данными):
docker compose -f docker-compose.yml down -v

# Поднимаем только PostgreSQL:
docker compose -f docker-compose.yml up -d zammad-postgresql

# Копируем дамп в контейнер и разворачиваем:
docker cp backup/prod-<дата>.dump \
  $(docker compose -f docker-compose.yml ps -q zammad-postgresql):/tmp/prod.dump
docker compose -f docker-compose.yml exec -T zammad-postgresql \
  pg_restore -U zammad -d zammad_production --no-owner --no-privileges -j 4 /tmp/prod.dump
```

`pg_restore` из дампа старой версии PG в PostgreSQL 18 проходит штатно
(формат переносим вперёд). Проверить расширения: они должны совпасть с
продовыми (`psql -c '\dx'` там и тут).

### 0.3 Запуск 3.3.0 поверх восстановленной базы

```bash
docker compose -f docker-compose.yml up -d
docker compose -f docker-compose.yml ps zammad-init   # нужен Exited (0)
```

Пользователи, права и API-токены уже в базе — ничего создавать не нужно:
входим под продовой учёткой администратора, смоук гоняем существующим
интеграционным токеном.

## 1. Ступень 3.3 → 4.1.0

```bash
docker compose -f docker-compose.yml exec -T zammad-postgresql \
  pg_dump -U zammad -Fc zammad_production > backup/pre-4.1.dump
docker compose -f docker-compose.yml -f stages/override-4.1.yml up -d
# init Exited (0), затем обязательно:
docker compose -f docker-compose.yml -f stages/override-4.1.yml \
  restart zammad-nginx zammad-websocket
```

Рестарт нужен потому, что nginx кэширует IP апстрима: после пересоздания
railsserver nginx ходит на старый адрес и отдаёт 502.

## 2. Ступень 4.1 → 5.4.1

```bash
docker compose -f docker-compose.yml -f stages/override-4.1.yml exec -T \
  zammad-postgresql pg_dump -U zammad -Fc zammad_production > backup/pre-5.4.dump
docker compose -f docker-compose.yml -f stages/override-5.4.yml up -d
# init Exited (0) → restart nginx/websocket → реиндекс:
docker compose -f docker-compose.yml -f stages/override-5.4.yml \
  exec zammad-railsserver rake zammad:searchindex:rebuild
```

## 3. Ступень 5.4 → 6.5.0

Структура стека меняется: дальше compose один (`stages/docker-compose.6.5.yml`),
старые файлы больше не используем. Перед стартом — права на каталог MinIO
(пользователь контейнера 1001):

```bash
docker run --rm -v zammad-33-stand_minio-data:/d alpine chown -R 1001:1001 /d

docker compose -f docker-compose.yml -f stages/override-5.4.yml exec -T \
  zammad-postgresql pg_dump -U zammad -Fc zammad_production > backup/pre-6.5.dump
docker compose -f docker-compose.yml -f stages/override-5.4.yml down
docker compose -f stages/docker-compose.6.5.yml up -d
# init Exited (0) → реиндекс (DATABASE_URL нужен, чтобы рейк не спорил с pool):
docker compose -f stages/docker-compose.6.5.yml exec zammad-railsserver \
  bash -lc 'export DATABASE_URL="postgres://zammad:zammad@zammad-postgresql:5432/zammad_production?pool=50"; rake zammad:searchindex:rebuild'
```

Volume `postgresql-data` и `elasticsearch-data` общие с прошлыми шагами —
данные подхватываются сами.

## 4. Миграция вложений в S3 (на 6.5)

```bash
docker compose -f stages/docker-compose.6.5.yml exec -T zammad-railsserver \
  bash -lc 'export DATABASE_URL="postgres://zammad:zammad@zammad-postgresql:5432/zammad_production?pool=50"; \
  cd /opt/zammad && bundle exec rails runner " \
  pp Store::Provider::S3.ping?; \
  pp Store::File.group(:provider).count; \
  Setting.set(%q{storage_provider}, %q{S3}); \
  Store::File.move(%q{DB}, %q{S3}, 1); \
  pp Store::File.group(:provider).count"'
```

Ожидаемый итог: счётчик `{"S3"=>N}` — все вложения уехали в бакет `zammad`,
таблица `stores` в БД сдувается до сотен килобайт. Третий аргумент `move` —
пауза между файлами в секундах: 1 с, чтобы миграция шла фоном и не грузила
сервис (на 450 ГБ это дни — и это нормально, см. ранбук). Консоль MinIO —
http://localhost:9001 (zammad / zammadsecret123).

## 5. Прыжок 6.5 → 7.2.3 (проверен как прямой)

```bash
docker compose -f stages/docker-compose.6.5.yml exec -T zammad-postgresql \
  pg_dump -U zammad -Fc zammad_production > backup/pre-7.2.dump
docker compose -f stages/docker-compose.6.5.yml down
ZAMMAD_IMAGE=ghcr.io/zammad/zammad:7.2.3-0020 \
ES_IMAGE=docker.elastic.co/elasticsearch/elasticsearch:8.15.5 \
docker compose -f stages/docker-compose.7.yml up -d
# init Exited (0) → реиндекс → смоук
```

ES 8.15.5 читает данные 7.17.3 штатно. Если захочется пойти ступенями —
теги `7.0.2-0000` и `7.1.3-0018` в том же файле, без смены ES.

## 6. Смоук-тест после каждой ступени

```bash
docker build -t zammad-smoke .
docker run --rm -e ZAMMAD_TOKEN=<интеграционный токен из базы> zammad-smoke
```

Адрес стенда по умолчанию — `http://host.docker.internal:8080`,
переопределение: `-e BASE_URL=...`. UI стенда: http://localhost:8080,
вход под продовой учёткой из восстановленной базы.

## Откат

Любая ступень откатывается парой «поднять прошлый compose + развернуть дамп»:

```bash
docker cp backup/<имя>.dump \
  $(docker compose -f <compose ступени> ps -q zammad-postgresql):/tmp/rollback.dump
docker compose -f <compose ступени> exec -T zammad-postgresql \
  pg_restore -U zammad -d zammad_production --clean --if-exists -j 4 /tmp/rollback.dump
```

Прод при всём этом не трогаем: до финального боевого переключения он
остаётся источником истины и запасным вариантом.
