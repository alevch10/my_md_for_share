# Цель

Автоматизировать сбор и визуализацию 4 ключевых метрик года, чтобы мониторинг был живым процессом, а не разовой выгрузкой, и служил источником сигналов для продуктовых целей других стримов.

**Из чего состояла цель**

| Квартал | KR |
|---|---|
| Q2 | Определить перечень ключевых метрик и правила их расчёта |
| Q2 | Построить процесс автоматического сбора, очистки и расчёта метрик |
| Q3 | Автоматизировать сбор и визуализацию метрик |

## Что считается

| # | Метрика | Годовой KPI | Статус автоматизации |
|---|---|---|---|
| 1 | Выручка от пользователей цифровых каналов (г/г) | +20 млн ₽ | автоматизирована |
| 2 | Доля записей через онлайн-канал (г/г) | +5 п.п. | автоматизирована |
| 3 | Трафик в ОЗ, Web + МП (г/г) | +10 % | автоматизирована |
| 4 | MAU ЛК, Web + МП | > 45 тыс. в среднем | автоматизирована |

## Как устроен этот документ

Сначала — **архитектура**: какие процессы что считают, по какому расписанию и в каком окне. Дальше каждая метрика описана по одному шаблону: **описание** (что измеряем и как читать цифру) и **техническая реализация** (SQL, источники, пошаговый разбор, бизнес-правила, ограничения). После метрик — **контроль качества**: перечень автоматических проверок и что каждая ловит.

Метрики 3 и 4 считаются одним пайплайном, поэтому общий слой данных для них вынесен в отдельный раздел перед ними.

---

# Архитектура

## Четыре процесса

```
[ESB]   01:00 UTC   patientnet_online_appointments_etl
                    → kpi.online_appointments

[МИС]   02:00 UTC   patientnet_etl
                    → kpi.revenue_kpi
                    → kpi.appointments
                    → kpi.rate_kpi  (call_center + источники ESB)
                    → kpi.ob_to_call_centre_kpi
                    → kpi.kpis_summary  (revenue, ratio)

[веб]   03:00 UTC   yandex_metrika_data_and_mau_kpis
                    → kpi.user_identity, kpi.user_presence
                    → kpi.identity_coverage
                    → kpi.mau_ob_kpi, kpi.mau_lk_kpi
                    → kpi.kpis_summary  (mau_ob, mau_lk_avg)
```

Порядок важен: ESB грузится за час до МИС, поэтому `patientnet_etl` собирает итоговую долю в один проход — числитель, знаменатель и витрина считаются подряд из уже свежих данных.

## Окна пересчёта

| Процесс | Ежедневное окно | Ручной бэкфилл |
|---|---|---|
| `patientnet_online_appointments_etl` | последние 45 дней | `{"backfill_from": "2025-01-01"}` |
| `patientnet_etl` | `RECALC_MONTHS = 3` месяца от текущего | `{"backfill_from": "2025-01-01"}` |
| `yandex_metrika_data_and_mau_kpis` | вчерашние сутки (сырьё), 3 месяца (витрины) | `{"backfill_from": "2025-01-01"}` |
| `kpi_rebuild_derivatives` | — | `{"from": "2025-01-01", "to": "2026-10-01"}` |

**Почему окно, а не один день.** Данные дозревают: услуга может быть выполнена или отменена после окончания месяца, цена и скидки уточняются задним числом, запись из ESB может приехать с более ранним `created_at`. Скользящее окно даёт им доехать.

## Переменные Airflow

Идентификаторы вынесены из текста запросов. У всех четырёх заданы значения по умолчанию, поэтому отсутствие переменной не ломает DAG — он продолжит работать по-старому.

| Имя | Значение | Тип |
|---|---|---|
| `kpi_self_booking_sysuser` | `5e95e526-907f-4eef-9093-ac0524a39f5b` | строка |
| `kpi_call_center_posts` | `["26a45943-…", "48064f8f-…"]` | JSON |
| `kpi_web_online_sources` | `["ONLINE_BOOKING", "LK"]` | JSON |
| `kpi_known_sources` | `["ONLINE_BOOKING", "LK", "MOBILE_LK", "MOBILE_ONLINE_BOOKING", "EXTERNAL", "call_center"]` | JSON |

## Итоговые витрины

| Таблица | Что хранит | Кто пишет |
|---|---|---|
| `kpi.revenue_kpi` | выручка помесячно | `patientnet_etl` |
| `kpi.appointments` | записи «Самозапись С.С.» из МИС | `patientnet_etl` |
| `kpi.online_appointments` | записи из ESB | `patientnet_online_appointments_etl` |
| `kpi.rate_kpi` | месяц × источник × количество | `patientnet_etl` |
| `kpi.ob_to_call_centre_kpi` | помесячная доля онлайн-записей | `patientnet_etl` |
| `kpi.user_identity` | склейка внешних id с `ehr_id` | веб-аналитика, пересчёт |
| `kpi.user_presence` | факт присутствия: пользователь × продукт × день | веб-аналитика, пересчёт |
| `kpi.identity_coverage` | доля пользователей без `ehr_id` по источникам | веб-аналитика, пересчёт |
| `kpi.mau_ob_kpi`, `kpi.mau_lk_kpi` | MAU помесячно | веб-аналитика, пересчёт |
| `kpi.kpis_summary` | годовые итоги: `revenue`, `ratio`, `mau_ob`, `mau_lk_avg` | оба основных DAG |

---

# Метрики

## Метрика 1. Выручка от цифровых каналов

### Описание

**Годовой KPI**
Прирастить выручку по пациентам, использовавшим цифровые продукты (ОЗ, ЛК, агрегаторы), на 20 млн ₽

**Что считаем**
Сумму выручки по услугам, назначения на которые **созданы через цифровой канал** — учётной записью «Самозапись». Учитываются подписанные и выполненные назначения, применяются скидки и правило округления из МИС. Бюджетные услуги исключены, ДМС, депозит и наличные — входят.

**Важно при чтении цифры**
Это метрика **прямого вклада продукта**, а не всей выручки цифровых пациентов. Если пациент записался через ОЗ, а врач на приёме назначил ему УЗИ и анализы — эти назначения создал врач, и в метрику они не входят. Так сделано намеренно: иначе метрика мерила бы работу врача, а не качество продукта.

**Отчёты (2 шт.)**

1. *Разница выручки от записей через цифровые каналы, YoY* — накопительный итог разницы год к году
2. *Сравнение выручки от пользователей цифровых каналов (г/г)* — динамика выручки год к году

### Техническая реализация

#### Схема для быстрого погружения

```
ЕЖЕДНЕВНО, для окна = последние 3 месяца:

1. ОТОБРАТЬ назначенные услуги за период
     где назначение подписано/выполнено
       и услуга не бюджетная (ДМС, депозит, кеш — входят)
       и автор назначения = учётка «Самозапись»
2. ОПРЕДЕЛИТЬ услуги с зафиксированной ценой
     (финплан, действующий НА КОНЕЦ ОТЧЁТНОГО ПЕРИОДА, наличная явка)
3. СОБРАТЬ скидки по услугам из шага 1 (проценты + фиксированные суммы)
4. ПОГАСИТЬ скидки для услуг с зафиксированной ценой
5. ПОЛУЧИТЬ актуальную прейскурантную цену каждой услуги
6. ПРОЧИТАТЬ правило округления из настроек МИС (ровно одну строку)
7. ДЛЯ КАЖДОЙ услуги:
       если есть скидка — процентная ИЛИ фиксированная →
           округлить(цена − процент − фикс.сумма)
       иначе → цена
   ПРОСУММИРОВАТЬ → выручка цифровых каналов за период
8. СРАВНИТЬ с тем же периодом прошлого года,
     обрезав обе стороны последним завершённым месяцем
```

#### SQL-запрос

Границы периода, UUID учётки и вызовы вынесены в параметры; ниже — запрос с подставленными значениями для чтения.

```sql
WITH
  base_services AS (
    SELECT
      people.oid AS people_oid,
      services.pay_type,
      presc.id AS presc_id,
      ps.oid AS presc_service_oid
    FROM
      mir.people people
      JOIN mir.mdoc mdoc ON people.oid = mdoc.people_id
      JOIN mir.presc presc ON mdoc.id = presc.mdoc_id
      JOIN mir.presc_service ps ON ps.presc = presc.id
      JOIN mir.service_presctype sp ON ps.service_presctype = sp.oid
      JOIN mir.services services ON sp.service = services.oid
    WHERE
      presc.presc_state_id IN ('sign', 'done_lab', 'done', 'done_other_lpu')
      AND presc.upd_dt >= '{start_month}'
      AND presc.upd_dt <  '{end_month}'
      AND services.pay_type NOT IN ('sp_budget', 'budget')
      AND EXISTS (
        SELECT 1
        FROM mir.sotr sotr
          JOIN mir.sysuser sysuser ON sotr.sysuser = sysuser.oid
        WHERE sotr.oid = presc.creator_id
          AND sysuser.oid = '{self_booking_sysuser}'
      )
  ),
  finance_plans AS (
    SELECT DISTINCT
      fpps.presc_service AS presc_service_oid
    FROM
      pay.finance_plan_presc_service fpps
      JOIN pay.finance_plan plan ON plan.oid = fpps.finance_plan
      JOIN mir.visit vis ON vis.id = CAST(plan.visit AS bpchar (36))
    WHERE
      vis.pay_type_id = 'cash'
      AND plan.fixed = TRUE
      AND '{end_month}'::date - INTERVAL '1 day'
          BETWEEN plan.date_begin AND plan.date_end
  ),
  discount_agg AS (
    SELECT
      dp.presc_service AS presc_service_oid,
      SUM(COALESCE(dp.percent, d.percent)) AS sum_percent,
      SUM(dp.fix_sum) AS fix_sum_discount
    FROM
      mir.discount_presc dp
      LEFT JOIN mir.discount d ON d.oid = dp.discount
    WHERE dp.presc_service IN (SELECT presc_service_oid FROM base_services)
    GROUP BY dp.presc_service
  ),
  discounts AS (
    SELECT
      bs.presc_service_oid,
      CASE WHEN fp.presc_service_oid IS NULL THEN da.sum_percent END
        AS sum_percent,
      CASE WHEN fp.presc_service_oid IS NULL THEN da.fix_sum_discount END
        AS fix_sum_discount
    FROM
      base_services bs
      LEFT JOIN finance_plans fp ON fp.presc_service_oid = bs.presc_service_oid
      LEFT JOIN discount_agg da ON da.presc_service_oid = bs.presc_service_oid
  ),
  prices AS (
    SELECT
      bs.presc_service_oid,
      mir.get_price_by_presc (bs.presc_id, bs.presc_service_oid) AS base_price
    FROM base_services bs
  ),
  round_setting AS (
    SELECT COALESCE(
      (SELECT valuepar
         FROM mir.systemsettings
        WHERE param = 'DiscountAmountRounding'
        LIMIT 1),
      ''
    ) AS valuepar
  )
SELECT
  SUM(
    CASE
      WHEN d.sum_percent IS NOT NULL OR d.fix_sum_discount IS NOT NULL THEN
        CASE rs.valuepar
          WHEN '1' THEN CEILING(
            p.base_price
            - (p.base_price * COALESCE(d.sum_percent, 0) / 100)
            - COALESCE(d.fix_sum_discount, 0))
          WHEN '2' THEN TRUNC(
            p.base_price
            - (p.base_price * COALESCE(d.sum_percent, 0) / 100)
            - COALESCE(d.fix_sum_discount, 0))
          WHEN '3' THEN ROUND(
            p.base_price
            - (p.base_price * COALESCE(d.sum_percent, 0) / 100)
            - COALESCE(d.fix_sum_discount, 0))
          ELSE ROUND(
            p.base_price
            - (p.base_price * COALESCE(d.sum_percent, 0) / 100)
            - COALESCE(d.fix_sum_discount, 0), 2)
        END
      ELSE p.base_price
    END
  ) AS revenue
FROM
  prices p
  JOIN discounts d ON d.presc_service_oid = p.presc_service_oid
  CROSS JOIN round_setting rs;
```

#### Источники данных

**Основная цепочка (schema `mir`)**

| Таблица | Что хранит | Роль в расчёте |
|---|---|---|
| `people` | люди; один человек может быть одновременно пациентом и сотрудником | точка входа цепочки |
| `mdoc` | медицинские карты; у одного человека их может быть несколько | связка человек → карта |
| `presc` | назначения (назначенные и выполненные) | **основная сущность периода**: статус, дата, автор |
| `presc_service` | связь назначения с услугами (many-to-many) | **зерно расчёта** — одна строка = одна назначенная услуга |
| `service_presctype` | связь услуги с типом назначения (many-to-many) | мост к справочнику услуг |
| `services` | справочник услуг | тип оплаты (`pay_type`) |
| `sotr` | сотрудники | автор назначения (`presc.creator_id`) |
| `sysuser` | пользователи системы (МИС) | **идентификация цифрового канала** |
| `discount_presc` | скидки, привязанные к назначенной услуге | процент и фиксированная сумма скидки |
| `discount` | справочник скидок | процент по умолчанию |
| `visit` | явки (посещения) | тип оплаты явки |
| `systemsettings` | системные настройки МИС | правило округления суммы скидки |

**Финансовые планы (schema `pay`)**

| Таблица | Что хранит | Роль в расчёте |
|---|---|---|
| `finance_plan` | финансовый план — фиксация цен на услуги | признак `fixed`, период действия, ссылка на явку |
| `finance_plan_presc_service` | связь финплана с назначенной услугой | признак «цена зафиксирована → скидки не применять» |

**Функции и настройки**

| Объект | Назначение |
|---|---|
| `mir.get_price_by_presc(presc_id, presc_service_oid)` | возвращает **актуальную** прейскурантную цену назначенной услуги (не цену на дату назначения) |
| `mir.systemsettings.param = 'DiscountAmountRounding'` | правило округления цены со скидкой |

#### Пошаговый разбор расчёта

**Шаг 1. `base_services` — отобрать назначенные услуги отчётного периода**

Строит зерно всего расчёта: одна строка = одна назначенная услуга (`presc_service.oid`).

Цепочка соединений:

```
people → mdoc → presc → presc_service → service_presctype → services
```

Фильтры отбора:

| Условие | Смысл |
|---|---|
| `presc.presc_state_id IN ('sign','done_lab','done','done_other_lpu')` | берём только назначения, доведённые до подписания или выполнения |
| `presc.upd_dt >= … < …` | границы отчётного периода, подставляются оркестратором. Полуинтервал — первое число следующего месяца не захватывается |
| `services.pay_type NOT IN ('sp_budget','budget')` | исключаем ОМС. Остаются коммерческие типы оплаты: **ДМС, депозит, наличные** — все входят в KPI |
| `EXISTS (... sysuser = kpi_self_booking_sysuser)` | **ключевой фильтр атрибуции**: автор назначения связан с технической учёткой цифровых каналов «Самозапись» |

На выход отдаются `people_oid`, `pay_type`, `presc_id`, `presc_service_oid`. В расчёте выручки используются только два последних; первые два — наследие версии с разбивкой суммы по типам платежа (Огр-1.3).

**Шаг 2. `finance_plans` — найти услуги с зафиксированной ценой**

Условия:

- `plan.fixed = TRUE` — план зафиксирован;
- `visit.pay_type_id = 'cash'` — явка оплачивается наличными;
- план действует **на последний день отчётного периода**.

Бизнес-смысл: если цена услуги зафиксирована финпланом, скидки к ней не применяются — цена уже согласована.

**Шаг 3. `discount_agg` — агрегировать скидки**

По каждой назначенной услуге суммирует:

- `sum_percent` = `SUM(COALESCE(dp.percent, d.percent))` — процент берётся из привязки скидки к назначению, а если не задан — из справочника. Несколько скидок **складываются** по процентам, а не применяются каскадом;
- `fix_sum_discount` = `SUM(dp.fix_sum)` — суммарная фиксированная скидка в рублях.

CTE ограничен услугами отчётного периода. 

**Шаг 4. `discounts` — погасить скидки там, где цена зафиксирована**

Присоединяет к каждой услуге из шага 1 её скидки, обнуляя их (в `NULL`), если услуга нашлась в финплане из шага 2. `ELSE` не задан, поэтому для услуг из финплана оба поля становятся `NULL` → в финале применится базовая цена.

**Шаг 5. `prices` — получить базовую цену**

Для каждой назначенной услуги вызывается `mir.get_price_by_presc`, которая возвращает актуальную прейскурантную цену.

> ⚠️ Актуальная, а не историческая цена означает: при изменении прейскуранта пересчёт прошлого месяца даст сумму по новым ценам. Это остаётся так — функция МИС, менять её мы не можем.

**Шаг 6. `round_setting` — прочитать правило округления**

| Значение | Правило | Функция |
|---|---|---|
| `1` | округление вверх до целого | `CEILING` |
| `2` | отбрасывание дробной части | `TRUNC` |
| `3` | арифметическое округление до целого | `ROUND` |
| иное / не задано | округление до 2 знаков | `ROUND(x, 2)` |

**Фактическое состояние: `valuepar` в МИС пуст.** Значит применяется ветка `ELSE` — округление до копеек, а три первые ветки недостижимы. `CASE` оставлен как защита на случай, если настройку однажды заполнят. Вопрос к команде МИС, должно ли поле быть пустым, остаётся открытым (Приложение В).

**Шаг 7. Финальная агрегация**

```
revenue = SUM( цена_услуги )
```

где для каждой услуги: если есть скидка — процентная **или** фиксированная — цена считается по формуле со скидкой и округляется по правилу из шага 6; иначе берётся базовая цена.

**Шаг 8. Сравнение год к году**

Обе стороны сравнения обрезаются последним завершённым месяцем. Общая функция:

```python
def resolve_yoy_period(max_month):
    end_current    = min(max_month, last_complete_month())
    year           = end_current.year          # год берётся ПОСЛЕ обрезки
    start_current  = date(year, 1, 1)
    start_previous = date(year - 1, 1, 1)
    end_previous   = date(year - 1, end_current.month, 1)
    ...
```
Сначала обрезаем, потом берём год — тогда в январе посчитается полный прошлый год против позапрошлого.

#### Бизнес-правила расчёта (сводка)

1. Услуга попадает в выручку, если её назначение **подписано или выполнено**.
2. Период определяется по `presc.upd_dt` — дате последнего изменения назначения.
3. Бюджетные услуги (`budget`, `sp_budget`) в выручку не входят. ДМС, депозит и наличные — входят.
4. К цифровому каналу услуга относится, если **автор назначения** — учётная запись «Самозапись».
5. Цена = актуальная прейскурантная цена назначения из `get_price_by_presc`.
6. Скидки применяются, кроме услуг с ценой, зафиксированной финпланом, действующим на конец отчётного периода, по наличной явке.
7. Несколько скидок на одну услугу складываются по процентам, фиксированные суммы — по рублям. Скидка применяется, если задана хотя бы одна из двух.
8. Правило округления берётся из системной настройки МИС. Фактически настройка пуста → округление до копеек.
9. Отменённые и перенесённые услуги выпадают из выборки естественным образом — через статус назначения и скользящее окно пересчёта.
10. Сравнение год к году идёт по отрезкам, обрезанным последним завершённым месяцем.

#### Известные ограничения (приняты осознанно)

| # | Ограничение | Комментарий |
|---|---|---|
| **Огр-1.1** | Считается только прямой вклад продукта, без врачебных дозначений | осознанное решение, см. «Важно при чтении цифры» |
| **Огр-1.2** | Внутри «Самозаписи» нет разделения по каналу | агрегаторы, ЛК и веб-ОЗ входят в KPI, но неразличимы в этом расчёте |
| **Огр-1.3** | `people_oid` и `pay_type` отбираются, но не используются | наследие версии с разбивкой по типам платежа; можно вернуть разрез |
| **Огр-1.4** | Значение месяца нестабильно ~3 месяца после его окончания | плата за полноту данных; см. «Окна пересчёта» |
| **Огр-1.5** | Период определяется по дате изменения, а не по бизнес-дате услуги | выборка почти правильная (99,9 % совпадений с `prunbeg_dt`). Оставил этот источник, поскольку он был в формуле, которую мне дала Анна Сердитова |
| **Огр-1.6** | Цена берётся актуальная, а не на дату назначения | так устроена функция МИС |

---

## Метрика 2. Доля онлайн-записей

### Описание

**Годовой KPI**
Вырастить долю записавшихся онлайн на 5 п.п.

**Что считаем**

$$\frac{\text{записи через веб-ОЗ и веб-ЛК}}{\text{записи через веб-ОЗ и веб-ЛК} + \text{записи через колл-центр}}$$

**Важно при чтении цифры**
Знаменатель — не весь поток записей клиники, а только веб + колл-центр. Мобильное приложение из расчёта исключено: его удаляли из сторов, и повлиять на этот канал команда не могла. Метрика сознательно ограничена тем, на что продукт влияет.

Сравниваются только записи **на услуги, доступные для записи через онлайн-запись**: колл-центр умеет записывать на всё, что есть в расписании, и без этого ограничения знаменатель включал бы то, что онлайн-запись в принципе не может забрать.

**Что входит в числитель.** Источники `ONLINE_BOOKING` (веб-ОЗ) и `LK` (запись из веб-личного кабинета). Второй появился в ESB в апреле 2026 и вырос с 521 до 9 364 записей в месяц — к августу он давал больше половины числителя. Состав источников вынесен в переменную `kpi_web_online_sources`, а появление незнакомого источника роняет пайплайн (см. `check_known_sources`).

### Техническая реализация

#### Схема для быстрого погружения

```
01:00  patientnet_online_appointments_etl
       [ESB] → источник записи (ONLINE_BOOKING, LK, …)
            → kpi.online_appointments        (DISTINCT ON, DO UPDATE, окно 45 дней)

02:00  patientnet_etl
       [МИС] → записи колл-центра            → kpi.rate_kpi (source='call_center')
       [МИС] → id записей «Самозаписи»       → kpi.appointments
                                                      │
       kpi.appointments ⋈ kpi.online_appointments      ▼
                                              kpi.rate_kpi (источники ESB)
                                                      │
                                                      ▼
                          kpi.ob_to_call_centre_kpi  (считается ИЗ rate_kpi)
                                                      │
                                                      ▼
                          kpi.kpis_summary, kpi='ratio'  (AVG за завершённые месяцы)
```

#### SQL-запросы

**1) Записи, созданные контактным центром (знаменатель)**

Принадлежность к колл-центру фиксируется **на момент создания записи**.

```sql
WITH
  call_center AS (
    SELECT
      s2."oid" AS sotr_oid,
      COALESCE(s2.date_post_begin, '2000-01-01'::timestamp) AS post_begin,
      COALESCE(s2.date_post_end,   '2999-12-31'::timestamp) AS post_end
    FROM mir.sotr s2
      JOIN mir.post p ON p."oid" = s2.post
    WHERE p."oid" IN ({call_center_posts})
  )
SELECT
  COUNT(*) AS count
FROM
  mir.schedule s
  JOIN mir.presc_schedule ps ON s.oid = ps.shedule_id
  JOIN mir.presc p ON ps.presc_id = p.id
  JOIN mir.presctype p2 ON ps.presctype = p2."oid"
  JOIN mir.schedule_work_time sw ON s.work_time = sw.oid
  -- ПРАВКА 4: принадлежность к КЦ на момент создания записи
  JOIN call_center cc
    ON cc.sotr_oid = p.creator_id
   AND p.create_dt >= cc.post_begin
   AND p.create_dt <  cc.post_end
WHERE
  s.islocked = 0
  AND ps.presc_id IS NOT NULL
  AND date (p.create_dt) >= '{start_month}'
  AND date (p.create_dt) <  '{end_month}'
  AND p.presctype_id IN (
    SELECT DISTINCT (UNNEST(STRING_TO_ARRAY(sw.presctype, ',')))
  )
  AND p2.time_cells IS NOT NULL
  AND (
    'CHILD'::VARCHAR = ANY (p2.tags)
    OR 'ADULT'::VARCHAR = ANY (p2.tags)
  )
  AND sw.insite = 1
```

`COALESCE` здесь обязателен: у 35 сотрудников из 101 дата начала работы в должности пуста. Без него треть операторов выпала бы из знаменателя.

**2) Записи, созданные УЗ «Самозапись С.С» (числитель)**

```sql
WITH
  cte AS (
    SELECT so.oid AS oid
    FROM mir.sotr so
      JOIN mir.sysuser sys ON so.sysuser = sys.oid
    WHERE sys.oid = '{self_booking_sysuser}'
  )
SELECT
  p.create_dt,
  p.id
FROM
  mir.schedule s
  JOIN mir.presc_schedule ps ON s.oid = ps.shedule_id
  JOIN mir.presc p ON ps.presc_id = p.id
  JOIN mir.presctype p2 ON ps.presctype = p2."oid"
  JOIN mir.schedule_work_time sw ON s.work_time = sw.oid
WHERE
  ps.presc_id IS NOT NULL
  AND p.creator_id IN (SELECT oid FROM cte)
  AND date (p.create_dt) >= '{start_month}'
  AND date (p.create_dt) <  '{end_month}'
ORDER BY 2 ASC
```

Результат вставляется с `ON CONFLICT (appointment_id) DO UPDATE` — чтобы ошибка в уже загруженной записи исправлялась повторным прогоном.

**3) Источник записи из ESB**

```sql
SELECT DISTINCT ON (appointment_oid)
    created_at      AS create_dt,
    appointment_oid AS appointment_id,
    source
FROM sgm_schedule.appointment_record
WHERE created_at >= '{start}'
  AND created_at <  '{end}'
ORDER BY appointment_oid, updated_at DESC
```

**4) Сборка витрины `kpi.rate_kpi` — источники из ESB**

```sql
INSERT INTO kpi.rate_kpi ("month", "source", count)
SELECT
  DATE_TRUNC('month', a.create_dt)::date AS month,
  oa."source",
  COUNT(DISTINCT a.appointment_id)       AS count
FROM kpi.appointments a
  JOIN kpi.online_appointments oa ON a.appointment_id = oa.appointment_id
WHERE oa."source" IS NOT NULL
  AND a.create_dt >= %(start_month)s::date
  AND a.create_dt <  %(end_month)s::date
GROUP BY 1, 2
ON CONFLICT ("month", "source") DO UPDATE
SET count = EXCLUDED.count;
```

Строку `call_center` этот запрос не трогает — её пишет отдельный шаг выше по данным МИС.

`COUNT(DISTINCT)` вместо `COUNT(*)`: соединение сейчас строго 1:1, результат тот же, но если уникальность `appointment_id` однажды потеряется, `COUNT(*)` тихо завысит метрику.

**5) Витрина `kpi.ob_to_call_centre_kpi` — считается ИЗ `rate_kpi`**

```sql
INSERT INTO kpi.ob_to_call_centre_kpi
    (month, online_count, online_and_callcenter, ratio_online_to_total)
SELECT
    r.month,
    COALESCE(SUM(r.count) FILTER (WHERE r.source IN ({web_sources})), 0)
        AS online_count,
    COALESCE(SUM(r.count) FILTER (
        WHERE r.source IN ({web_sources}) OR r.source = '{call_center_source}'
    ), 0) AS online_and_callcenter,
    COALESCE(SUM(r.count) FILTER (WHERE r.source IN ({web_sources})), 0)::numeric
    / NULLIF(COALESCE(SUM(r.count) FILTER (
        WHERE r.source IN ({web_sources}) OR r.source = '{call_center_source}'
      ), 0), 0) AS ratio_online_to_total
FROM kpi.rate_kpi r
WHERE r.month >= %(start_month)s::date
  AND r.month <  %(end_month)s::date
GROUP BY r.month
HAVING COALESCE(SUM(r.count) FILTER (
    WHERE r.source IN ({web_sources}) OR r.source = '{call_center_source}'
), 0) > 0
ON CONFLICT (month) DO UPDATE
SET online_count          = EXCLUDED.online_count,
    online_and_callcenter = EXCLUDED.online_and_callcenter,
    ratio_online_to_total = EXCLUDED.ratio_online_to_total;
```

`HAVING` отсекает месяцы с нулевым знаменателем, чтобы в витрину не попали `NULL` и деление на ноль.

**6) Итоговое значение метрики**

```sql
SELECT COALESCE(AVG(ratio_online_to_total), 0)
FROM kpi.ob_to_call_centre_kpi
WHERE month >= %(start)s::date
  AND month <  %(end)s::date
```

Границы приходят из `resolve_yoy_period()` — той же функции, что и в метрике 1, с обрезкой по последнему завершённому месяцу. Раньше в сравнение попадал битый и неполный август 2026.

#### Источники данных

**МИС (schema `mir`)**

| Таблица | Что хранит | Роль в расчёте |
|---|---|---|
| `schedule` | слоты расписания | признак блокировки слота, связь с рабочим временем |
| `presc_schedule` | связь назначения со слотом расписания | соединяет запись и слот |
| `presc` | назначения | дата создания записи, автор (`creator_id`) |
| `presctype` | типы назначений | сетка слотов (`time_cells`), теги (`CHILD` / `ADULT`) |
| `schedule_work_time` | рабочее время приёма | список допустимых типов назначений, признак очного приёма |
| `sotr` | сотрудники | авторы записей, **периоды работы в должности** |
| `post` | должности сотрудников | **идентификация колл-центра** |
| `sysuser` | пользователи системы | **идентификация цифрового канала** |

**ESB (schema `sgm_schedule`)**

| Таблица | Что хранит | Роль в расчёте |
|---|---|---|
| `appointment_record` | записи на приём с указанием источника | **разрез по каналу**: ONLINE_BOOKING, LK и другие |

**Хранилище (VS-BI-DWH, schema `kpi`)**

| Таблица | Что хранит |
|---|---|
| `appointments` | записи «Самозаписи» из МИС |
| `online_appointments` | источник записи из ESB |
| `rate_kpi` | агрегат: месяц × источник × количество |
| `ob_to_call_centre_kpi` | помесячная доля `ratio_online_to_total` |

#### Пошаговый разбор расчёта

**Шаг 1. Записи колл-центра (знаменатель)**

Колл-центр определяется через должность сотрудника — два UUID из переменной `kpi_call_center_posts` — **и через период работы в этой должности**.

Фильтры:

| Условие | Смысл |
|---|---|
| `s.islocked = 0` | слот расписания не заблокирован |
| `ps.presc_id IS NOT NULL` | связка с назначением есть |
| `date(p.create_dt) >= … < …` | месяц по дате создания назначения |
| `JOIN call_center … p.create_dt BETWEEN post_begin AND post_end` | автор был сотрудником КЦ **в момент создания записи** |
| `p.presctype_id IN (UNNEST(STRING_TO_ARRAY(sw.presctype, ',')))` | тип назначения разрешён для этого рабочего времени |
| `p2.time_cells IS NOT NULL` | тип назначения имеет сетку слотов — это запись в расписание, а не бумажное назначение |
| `'CHILD' = ANY(p2.tags) OR 'ADULT' = ANY(p2.tags)` | только детский или взрослый приём |
| `sw.insite = 1` | рабочее время относится к очному приёму в клинике |

**Почему фильтров больше, чем в скрипте числителя.** Пять последних условий вместе описывают «услугу, на которую можно записаться через онлайн-запись». Колл-центр записывает и на то, что онлайн-записи недоступно, — без этих фильтров знаменатель включал бы поток, который продукт в принципе не может забрать, и доля была бы занижена. В числителе фильтры не нужны: всё, что создано учёткой «Самозапись», по определению прошло через онлайн-запись. **Это осознанная асимметрия, а не дефект.**

**Почему важна дата должности.** Когда оператор переходил на другую позицию, его прошлые записи задним числом выпадали из знаменателя, и доля онлайна росла сама собой, без улучшения продукта. Эффект измерен: июль 2026 потерял 745 записей — 5,8 % знаменателя — между двумя прогонами с интервалом в несколько месяцев.

**Шаг 2. Записи онлайн-канала (числитель)**

Отбирает `presc.id` записей, созданных учётной записью «Самозапись» — той же технической учёткой, что и в метрике 1.

**Шаг 3. Источник записи из ESB**

Из `sgm_schedule.appointment_record` берутся `appointment_id` и `source`. Это единственное место, где различаются каналы внутри «Самозаписи». МИС такого разреза не даёт.

Загрузка работает скользящим окном в 45 дней с `ON CONFLICT DO UPDATE`, поэтому **чинится сама**: если день загрузился частично, следующий прогон его перезапишет. 

**Шаг 4. Сборка `rate_kpi`**

Соединяет записи из МИС с источником из ESB, агрегирует по месяцу и источнику, в пределах окна пересчёта. 

**Шаг 5. Витрина доли**

Считается из `rate_kpi` — см. запрос 5 выше.

**Шаг 6. Итоговое значение**

Помесячные доли усредняются за период, обрезанный последним завершённым месяцем, и сравниваются с тем же периодом прошлого года. Прирост в процентных пунктах и есть измеряемая величина.

#### Бизнес-правила расчёта (сводка)

1. Числитель — записи, созданные учёткой «Самозапись» с источниками `ONLINE_BOOKING` и `LK`.
2. Знаменатель — числитель плюс записи колл-центра.
3. Период определяется по `presc.create_dt` — дате создания записи.
4. В знаменатель попадают только записи на услуги, доступные для онлайн-записи: незаблокированный слот, тип назначения с сеткой слотов, теги `CHILD` / `ADULT`, очный приём, разрешённый тип назначения для рабочего времени.
5. Принадлежность автора к колл-центру определяется на момент создания записи.
6. Разрез по каналам внутри «Самозаписи» берётся из ESB, МИС его не различает.
7. Числитель и знаменатель витрины берутся из одной таблицы `rate_kpi`.
8. Итоговое значение за период — среднее помесячных долей за завершённые месяцы.

#### Известные ограничения (приняты осознанно)

| # | Ограничение | Комментарий |
|---|---|---|
| **Огр-2.1** | Знаменатель — только веб + колл-центр, не весь поток записей | осознанное решение, см. «Важно при чтении цифры» |
| **Огр-2.2** | Мобильное приложение исключено | приложение удаляли из сторов, влиять на канал команда не могла |
| **Огр-2.3** | Числитель и знаменатель отбираются разными наборами фильтров | осознанно: фильтры знаменателя описывают услуги, доступные для онлайн-записи |
| **Огр-2.4** | Разрез по каналам зависит от ESB | без ESB метрика различает только «Самозапись против колл-центра» |

---

## Общий слой данных для метрик 3 и 4

Метрики «Трафик в ОЗ» и «MAU ЛК» считаются одним пайплайном: у них общие источники и два общих слоя данных. Ниже описана общая часть, дальше — витрина и итоговый расчёт каждой метрики.

### Источники данных и смена трекера

| Период | Мобильное приложение | Веб-ОЗ | Веб-ЛК |
|---|---|---|---|
| до 01.02.2026 | Amplitude (`amplitude_mob.events`) | Amplitude Web | Amplitude Web |
| с 01.02.2026 | AppMetrica (`appmetrica.events`) | Яндекс.Метрика, счётчик Веб ОЗ | Яндекс.Метрика, счётчик Веб ЛК |

| Источник | Схема | Идентификатор пользователя |
|---|---|---|
| AppMetrica | `appmetrica.events` | `appmetrica_device_id`, `profile_id` |
| Метрика Веб ОЗ | `yandex_metrika_booking.events` + `event_params` | `client_id`, id из параметров события |
| Метрика Веб ЛК | `yandex_metrika_web_lk.events` + `event_params` | `client_id`, id из параметров события |

Внутренний идентификатор пользователя во всех источниках — **`ehr_id`**, системный идентификатор пациента. В AppMetrica он приходит как `profile_id`, в Метрике — в пользовательских параметрах (`__ymu → user_id`). Это одно пространство идентификаторов, поэтому кросс-платформенная склейка работает для всех авторизованных пользователей.

**Насколько трекеры сходятся.** 1 февраля 2026 работали оба, и это единственный день перекрытия. Замеры:

| Канал | Amplitude | Новый трекер | Разница |
|---|---|---|---|
| ЛК мобильный, устройств | 7 643 | 6 070 | **−20,6 %** |
| ОЗ мобильный, устройств | 3 267 | 2 835 | −13,2 % |
| ОЗ веб, устройств | 1 149 | 1 411 | **+22,8 %** |

Разница по людям, а не устройствам: из 7 305 человек, которых в этот день увидел хотя бы один трекер, AppMetrica показала 5 867 — 80 % объединения.

Это объясняет январский скачок MAU ЛК: 56 776 в январе против 45 949 в феврале, −19,1 % — практически ровно тот же коэффициент. **Январь посчитан другим инструментом и завышен примерно на 20 %; это артефакт смены трекера, а не сезонность.**

Коэффициент по одному дню ненадёжен ещё и потому, что новый SDK раскатывается вместе с обновлением приложения: первого февраля AppMetrica стояла не у всех. Косвенное подтверждение — февраль 45 949, март 50 867 (+10,7 %), дальше ряд успокаивается: похоже на догоняющую раскатку, а не на рост аудитории. Поэтому исторические значения **не корректируются**, а ограничение фиксируется явно (Огр-3.5, Огр-4.4).

### Схема для быстрого погружения

```
ЕЖЕДНЕВНО, за предыдущие сутки:

СЛОЙ 1 — идентификация
  appmetrica.events        ─┐
  yandex_metrika_booking   ─┼──> kpi.user_identity
  yandex_metrika_web_lk    ─┘    (source, external_id) → ehr_id

СЛОЙ 2 — присутствие (факт «был в продукте в этот день»)
  appmetrica (все события)                    ──> user_presence: lk / mobile
  appmetrica ∩ kpi.oz_events_mobile           ──> user_presence: oz / mobile
  booking, только цели (goal://)              ──> user_presence: oz / web
  web_lk, только просмотры страниц            ──> user_presence: lk / web

СЛОЙ 2а — контроль
  user_presence  ──> kpi.identity_coverage  (доля без ehr_id по источникам)

СЛОЙ 3 — витрины, окно 3 месяца
  user_presence (product='oz')  ──> kpi.mau_ob_kpi     (метрика 3, помесячно)
  user_presence (product='oz')  ──> kpi.kpis_summary   (метрика 3, годовой охват)
  user_presence (product='lk')  ──> kpi.mau_lk_kpi     (метрика 4, помесячно)
  mau_lk_kpi                    ──> kpi.kpis_summary   (метрика 4, среднее)
```

### Слой 1. `kpi.user_identity` — склейка идентификаторов

**Задача.** Сопоставить внешний идентификатор трекера (device id, cookie id) с `ehr_id`, чтобы один человек не считался несколько раз с разных устройств.

Ключ таблицы — `(source, external_user_id)`. Заполняется тремя запросами, по одному на источник. Ниже — AppMetrica; остальные два устроены так же, отличаются таблицей и способом извлечения `ehr_id`.

```sql
INSERT INTO kpi.user_identity (
    source, external_user_id, internal_user_id, first_seen_at, last_seen_at
)
SELECT
  'appmetrica',
  appmetrica_device_id::TEXT,
  MAX(profile_id),
  MIN(event_datetime),
  MAX(event_datetime)
FROM appmetrica.events
WHERE event_datetime >= %(start_date)s::date
  AND event_datetime <  (%(end_date)s::date + INTERVAL '1 day')
  AND appmetrica_device_id IS NOT NULL
GROUP BY appmetrica_device_id::TEXT
ON CONFLICT (source, external_user_id) DO UPDATE
SET
  internal_user_id = COALESCE(
      EXCLUDED.internal_user_id, kpi.user_identity.internal_user_id),
  first_seen_at = LEAST( 
      kpi.user_identity.first_seen_at, EXCLUDED.first_seen_at),
  last_seen_at = GREATEST(
      kpi.user_identity.last_seen_at, EXCLUDED.last_seen_at),
  updated_at = NOW();
```

Для Метрики `internal_user_id` берётся как `MAX(kpi.safe_extract_user_id(p.params::TEXT))` из соединения с `event_params`.

**Функция извлечения `ehr_id`**

```sql
CREATE OR REPLACE FUNCTION kpi.safe_extract_user_id(p_params text)
 RETURNS integer LANGUAGE plpgsql IMMUTABLE
AS $function$
BEGIN
    RETURN (p_params::jsonb #>> '{__ymu,user_id}')::integer;
EXCEPTION
    WHEN OTHERS THEN RETURN NULL;
END;
$function$;
```

Достаёт `ehr_id` по пути `__ymu → user_id` и приводит к целому. Любая ошибка — невалидный JSON, нечисловое значение — гасится и возвращает `NULL`. Это удобно и опасно одновременно: если Метрика изменит структуру параметров, функция молча вернёт `NULL` для всех событий, MAU ЛК на вебе рухнет, охват ОЗ вырастет, и ошибки не будет. Против этого работает проверка `check_identity_coverage`.

**Логика обновления таблицы:**

- `internal_user_id` перезаписывается новым значением, если оно не пустое. Таблица хранит «последний известный пользователь этого устройства», а не историю;
- `last_seen_at` двигается только вперёд, `first_seen_at` — только назад.

`LEAST` для `first_seen_at` критичен именно для пересчёта: месяцы обрабатываются по порядку, и без него январь, обработанный после августа, не смог бы исправить дату первого визита. С ним порядок обработки перестаёт иметь значение.

### Слой 2. `kpi.user_presence` — факт присутствия

**Задача.** Зафиксировать: такой-то пользователь был в таком-то продукте в такой-то день.

**Зерно:** `(activity_date, source, product, external_user_id)`. `internal_user_id` подтягивается из `user_identity` через `LEFT JOIN`, поэтому может остаться пустым.

**Четыре определения присутствия**

| # | `product` / `platform` | Источник | Что считается присутствием |
|---|---|---|---|
| 1 | `lk` / `mobile` | AppMetrica | **любое событие в приложении** |
| 2 | `oz` / `mobile` | AppMetrica ∩ `kpi.oz_events_mobile` | событие входа в онлайн-запись из справочника |
| 3 | `oz` / `web` | Метрика Веб ОЗ | достижение цели: `is_page_view = false` и `url LIKE 'goal://booking.avaclinic.ru/%'` |
| 4 | `lk` / `web` | Метрика Веб ЛК | просмотр страницы: `is_page_view = true` |

`kpi.oz_events_mobile` — справочник событий, означающих вход в онлайн-запись в мобильном приложении. **Поддерживается вручную.** Это граница продукта ОЗ внутри приложения.

**Почему для мобильного ЛК нет фильтра событий — осознанное решение.** Личный кабинет в приложении является контейнером: онлайн-запись живёт внутри него, и любой запуск приложения — это вход в ЛК. Отдельного справочника событий ЛК мы заводить не стали.

**Почему для веб-ЛК просмотры, а не цели.** По той же причине: продукту не важно, зачем пользователь зашёл — сам факт входа и есть использование. Для ОЗ присутствие определяется строже, потому что ОЗ — конкретный сценарий, а не контейнер.

**SQL наполнения.** Все четыре запроса устроены одинаково; ниже — веб-ОЗ, остальные отличаются источником, фильтром и парой `product` / `platform`.

```sql
INSERT INTO kpi.user_presence
(activity_date, source, product, platform,
 internal_user_id, external_user_id, first_event_time)
SELECT
    DATE(e.date_time), 'booking', 'oz', 'web',
    MAX(ui.internal_user_id), e.client_id::text, MIN(e.date_time)
FROM yandex_metrika_booking.events e
LEFT JOIN kpi.user_identity ui
    ON ui.source = 'booking'
   AND ui.external_user_id::TEXT = e.client_id::text
WHERE e.is_page_view = false
  AND e.url LIKE 'goal://booking.avaclinic.ru/%'
  AND e.date_time >= %(day)s::date
  AND e.date_time <  (%(day)s::date + interval '1 day')
GROUP BY DATE(e.date_time), e.client_id::text
ON CONFLICT (activity_date, source, product, external_user_id) DO UPDATE
SET internal_user_id = COALESCE(
        EXCLUDED.internal_user_id, kpi.user_presence.internal_user_id),
    updated_at = now();
```

Повторный прогон дня безопасен. `platform` и `first_event_time` при конфликте не обновляются.

### Слой 2а. `kpi.identity_coverage` — контроль покрытия

Доля пользователей без `ehr_id` — это верхняя граница погрешности метрик 3 и 4. 

**Ориентиры по августу 2026:**

| source / product / platform | доля без `ehr_id` |
|---|---|
| `appmetrica / oz / mobile` | 2,1 % |
| `appmetrica / lk / mobile` | 4,0 % |
| `web_lk / lk / web` | 16,9 % |
| `booking / oz / web` | **76,5 %** |

### Слой 3. Пересчёт производных слоёв

DAG `kpi_rebuild_derivatives` пересобирает всю цепочку `identity → presence → coverage → витрины → годовые итоги` за произвольный период, **не обращаясь к API трекеров**: сырые таблицы только читаются.

Порядок между шагами важен: сначала полностью пересобирается `user_identity` по всей истории, только потом `user_presence`. Иначе присутствие за январь считалось бы по таблице идентификаторов, знающей только про январь.

---

## Метрика 3. Трафик в ОЗ

### Описание

**Годовой KPI**
Увеличить входящий трафик в ОЗ на 10 %

**Что считаем**
Уникальных пользователей, попавших в онлайн-запись на любой шаг, накопительно с 1 января. Веб — по достижению цели входа в ОЗ, мобильное приложение — по событиям входа в ОЗ из справочника.

**Как считаются уникальные**
Пользователи склеиваются между вебом и приложением по `ehr_id`. Авторизованный пользователь, зашедший и с телефона, и с сайта, считается одним.

**Сравнение год к году**
Периоды берутся сопоставимыми: 1 января — вчерашний день, для 2026 и для 2025. Именно вчерашний, а не сегодняшний: данные грузятся с суточным лагом.

### Техническая реализация

Общие слои — см. раздел «Общий слой данных для метрик 3 и 4». Ниже специфичная часть.

#### SQL-запросы

**Витрина `kpi.mau_ob_kpi` — MAU онлайн-записи помесячно**

```sql
INSERT INTO kpi.mau_ob_kpi (month, count)
SELECT
    %(month)s::date,
    COUNT(DISTINCT COALESCE(
        internal_user_id::text,
        source || ':' || external_user_id
    ))
FROM kpi.user_presence
WHERE product = 'oz'
  AND activity_date >= %(month)s::date
  AND activity_date <  %(next_month)s::date
ON CONFLICT (month) DO UPDATE
SET count = excluded.count;
```

Пересобирается за то же скользящее окно в три месяца, что и остальные шаги. 

**Итоговое значение — годовой охват в `kpi.kpis_summary`**

```sql
INSERT INTO kpi.kpis_summary (year, kpi, value)
SELECT
    %(year)s,
    'mau_ob',
    COUNT(DISTINCT COALESCE(
        internal_user_id::text,
        source || ':' || external_user_id
    ))
FROM kpi.user_presence
WHERE product = 'oz'
  AND activity_date >= %(start_date)s::date
  AND activity_date <  %(end_date)s::date
ON CONFLICT (year, kpi) DO UPDATE
SET value = EXCLUDED.value;
```

Запускается дважды — за 2025 и за 2026, с сопоставимыми YTD-границами, верхняя из которых равна вчерашнему дню соответствующего года.

#### Пошаговый разбор расчёта

**Шаг 1. Ключ уникальности**

`COALESCE(internal_user_id::text, source || ':' || external_user_id)` — считаем по `ehr_id`, а при его отсутствии по паре «источник + внешний id». Для авторизованного пользователя склейка веб и мобайла работает; анонимный считается отдельно в каждом источнике.

**Шаг 2. Помесячная витрина**

Скользящее окно три месяца, при бэкфилле — вся история.

**Шаг 3. Годовой охват**

Пересчитывается целиком по всей истории каждый день за оба года, полуинтервалом, с верхней границей на вчерашнем дне.

#### Бизнес-правила расчёта (сводка)

1. Присутствие в ОЗ на вебе — достижение цели в Яндекс.Метрике; в приложении — событие из справочника `kpi.oz_events_mobile`.
2. Уникальность считается по `ehr_id`, для неопознанных — по паре «источник + внешний id».
3. Годовой итог — уникальные пользователи с 1 января по вчерашний день.
4. Сравнение год к году идёт по одинаковым по длине отрезкам года.

#### Известные ограничения (приняты осознанно)

| # | Ограничение | Комментарий |
|---|---|---|
| **Огр-3.1** | Анонимные пользователи не склеиваются между платформами | нечем склеивать; авторизация в ОЗ не обязательна |
| **Огр-3.2** | Годовой охват считается по cookie / device id | очистка кук, новый браузер, переустановка приложения создают нового «уникального» пользователя |
| **Огр-3.3** | Метрика хранится под именем `'mau_ob'` | исторический нейминг; фактически это годовой охват |
| **Огр-3.4** | `kpi.oz_events_mobile` поддерживается вручную | новый экран входа в ОЗ без заведённого события не попадёт в трафик |
| **Огр-3.5** | Годы посчитаны разными трекерами | с 01.02.2026 Amplitude → AppMetrica и Метрика. В день перекрытия мобильный разошёлся на −13,2 %, веб на +22,8 % — в разные стороны. Поправка по одному дню ненадёжна, значения не корректируются |
| **Огр-3.6** | Определение присутствия в вебе в 2025 и 2026 разное | Amplitude Web писал любое событие, Метрика — только достижения целей. Для Метрики цели и просмотры почти совпадают (44 516 против 44 930, разница 0,9 %); насколько полно Amplitude покрывал веб — неизвестно |

---

## Метрика 4. MAU ЛК

### Описание

**Годовой KPI**
Обеспечить средний MAU ЛК > 45 тыс. пользователей

**Что считаем**
Помесячную уникальную аудиторию личного кабинета, затем — среднее по **завершённым** месяцам текущего года.

**Что считается использованием ЛК**
Сам факт входа. Онлайн-запись живёт внутри личного кабинета, и нам не важно, зачем пользователь зашёл — важно, что зашёл. Поэтому в приложении присутствием считается любое событие, а в вебе — просмотр страницы, а не достижение цели. Это осознанное решение, см. «Слой 2».

**Как учитываются анонимы**
В мобильной части — по `device_id`, то есть неавторизованные пользователи входят. В веб-части — не учитываются вовсе. **Это тоже осознанное решение**, зафиксированное после замера альтернатив: три возможных правила давали 47 784 / 53 187 / 43 576 — разброс 20 %, и от выбора зависит, достигнута цель или нет. Правило принято и не пересматривается задним числом.

**Важно при чтении цифры**
Целевое значение 45 тыс. задано руководителем. Базовое значение 2025 года по этой метрике самостоятельно не рассчитывалось и текущим пайплайном не воспроизводится. Метрика абсолютная, поэтому для её расчёта база не нужна, но для сравнения с 2025 годом та цифра непригодна.

Январь 2026 посчитан Amplitude и завышен примерно на 20 % относительно остальных месяцев (см. «Смена трекера»).

### Техническая реализация

Общие слои — см. раздел «Общий слой данных для метрик 3 и 4». Ниже специфичная часть.

#### SQL-запросы

**Витрина `kpi.mau_lk_kpi` — MAU личного кабинета помесячно**

```sql
INSERT INTO kpi.mau_lk_kpi (month, count)
WITH lk_users AS (
    SELECT COALESCE(internal_user_id::text, external_user_id) AS user_id
    FROM kpi.user_presence
    WHERE product = 'lk'
      AND platform = 'mobile'
      AND activity_date >= %(month)s::date
      AND activity_date <  %(next_month)s::date
    UNION
    SELECT internal_user_id::text
    FROM kpi.user_presence
    WHERE product = 'lk'
      AND platform = 'web'
      AND internal_user_id IS NOT NULL
      AND activity_date >= %(month)s::date
      AND activity_date <  %(next_month)s::date
)
SELECT %(month)s::date, COUNT(DISTINCT user_id)
FROM lk_users
ON CONFLICT (month) DO UPDATE
SET count = excluded.count;
```

**Итоговое значение — `kpi.kpis_summary`, метрика `mau_lk_avg`**

```sql
INSERT INTO kpi.kpis_summary (year, kpi, value)
SELECT
    %(year)s,
    'mau_lk_avg',
    ROUND(AVG(count))
FROM kpi.mau_lk_kpi
WHERE month >= %(year_start)s::date
  AND month <  DATE_TRUNC('month', CURRENT_DATE)
ON CONFLICT (year, kpi) DO UPDATE
SET value = EXCLUDED.value;
```

#### Пошаговый разбор расчёта

**Шаг 1. Мобильная часть.** Пользователь считается по `ehr_id`, при его отсутствии — по `device_id`.

**Шаг 2. Веб-часть.** Считаются только строки с заполненным `ehr_id`.

**Шаг 3. Объединение.** `UNION` (не `UNION ALL`) схлопывает дубликаты. Пересечение мобильной и веб-частей возможно только по `ehr_id`.

**Шаг 4. Итоговое значение.** Среднее помесячных значений за завершённые месяцы текущего года.

#### Бизнес-правила расчёта (сводка)

1. Присутствие в ЛК на вебе — просмотр страницы; в приложении — любое событие, включая `dashboard_open`.
2. В мобильной части неавторизованные пользователи считаются по `device_id`, в веб-части — не считаются.
3. Помесячные значения считаются независимо друг от друга.
4. Итог года — среднее помесячных значений по завершённым месяцам.

#### Известные ограничения (приняты осознанно)

| # | Ограничение | Комментарий |
|---|---|---|
| **Огр-4.1** | Определение использования ЛК самое широкое из четырёх метрик | осознанно: ЛК — контейнер, а не отдельный сценарий. 60,1 % аудитории ЛК имеет события ОЗ |
| **Огр-4.2** | База 2025 года не воспроизводится пайплайном | цель задана извне; метрика абсолютная |
| **Огр-4.3** | Анонимы учитываются на мобильном и не учитываются на вебе | осознанно принятое правило; альтернативы дают разброс до 20 % |
| **Огр-4.4** | Январь 2026 посчитан Amplitude | завышен примерно на 20 % относительно остальных месяцев ряда |
| **Огр-4.5** | Пересчёт с полной таблицей идентификаторов снижает значения | схлопывание анонимных устройств одного человека; сдвиг всего ряда, не отдельных месяцев |

---

# Контроль качества

Алертинга в контуре нет, поэтому механизм такой: **проверка нашла проблему → `raise AirflowException` → таск красный в Airflow.** Это и есть уведомление, и оно не даёт битым данным доехать до витрины.

У проверочных тасков отдельные `default_args` с `retries: 0`. У основных стоит `retries: 2` с паузой 30 минут, и для проверок это вредно: данные за полтора часа сами не починятся, а проблема станет видна на 90 минут позже.

## Проверки в `patientnet_etl`

| Таск | Что ловит | Порог |
|---|---|---|
| `check_appointments_volume` | объём `kpi.appointments` за последний завершённый месяц против медианы шести предыдущих | падение > 40 % |
| `check_rate_kpi_volume` | то же в разрезе источников `rate_kpi` | падение > 40 % |
| `check_known_sources` | появление источника, которого нет в `kpi_known_sources` | любой |
| `check_ratio_range` | доля вне 0–1, пустая доля, нулевой знаменатель | любое нарушение |
| встроенные | нулевая выручка, ноль записей КЦ, ноль онлайн-записей — только за завершённые месяцы | ноль |

`check_known_sources` — не формальность. Источник `LK` появился в ESB в апреле 2026 и к августу давал больше половины числителя. Если бы его не добавили в расчёт, метрика тихо недосчитывала бы самый быстрорастущий канал, и заметить это было бы нечем — число просто росло бы медленнее.

## Проверки в `patientnet_online_appointments_etl`

| Таск | Что ловит | Порог |
|---|---|---|
| `reconcile_with_esb` | расхождение числа записей за последний завершённый месяц между ESB и хранилищем | > 2 % |
| `check_source_not_null` | доля записей с пустым `source` (они отбрасываются при сборке `rate_kpi`) | > 5 % |

## Проверки в `yandex_metrika_data_and_mau_kpis`

| Таск | Что ловит | Порог |
|---|---|---|
| `check_gaps_appmetrica` / `_booking` / `_web_lk` | суточные пропуски в сырых событиях за 30 дней | день ниже 60 % медианы |
| `check_presence_volume` | помесячный объём `user_presence` по каждой связке источник/продукт/платформа | падение > 40 % |
| `check_identity_coverage` | сдвиг доли без `ehr_id` — признак поломки `safe_extract_user_id` | > 20 п.п. от медианы за 14 дней |
| `check_mau_sanity` | нулевые значения в витринах MAU за завершённые месяцы | ноль |

## Чего проверки не покрывают

**Идемпотентность загрузки сырых событий.** В `load_source` день грузится ровно один раз: старт берётся как `max(date_field) + 1 день`. Если API отдал часть данных или запрос оборвался на середине, а `transformer` записал половину и вернул `success` — дыра остаётся навсегда и молча. Проверки `check_gaps_*` её найдут, но закрывать придётся вручную

Ещё одна потенциальная проблема, которая возможна при ошибке в данных Яндекса: `max(date_field)` берётся по всей таблице. Одно событие с датой из будущего — и старт уедет вперёд, пропустив всё между.

---

# Как это собрано

## KR Q2. Источники данных и скрипты

**Сделано**

- Определён набор и формат данных для расчёта метрик
- Найдены источники данных по каждой метрике
- Написаны скрипты подготовки и расчёта
- Запросы задокументированы для дальнейшей автоматизации

**Источники по метрикам**

| Метрика | Источник | Доступ |
|---|---|---|
| Выручка от цифровых каналов | БД Patient.Net | получен |
| Доля онлайн-записей | БД Patient.Net + БД ESB | получен |
| Трафик в ОЗ | Yandex.Metrica, AppMetrica, исторические данные Amplitude | получен |
| MAU ЛК | Yandex.Metrica, AppMetrica, исторические данные Amplitude | получен |

## KR Q3. Автоматизация

**1. Apache Airflow — четыре процесса, ежедневно с 01:00 до 03:00 UTC**

- получение данных из БД ESB → `kpi.online_appointments`
- получение данных из БД Patient.Net → выручка, записи, доля
- получение и загрузка данных из Яндекс.Метрики и AppMetrica → идентификация, присутствие, MAU
- расчёт значений и сохранение в БД VS-BI-DWH
- плюс отдельный DAG ручного пересчёта производных слоёв

**2. Metabase** — визуализация рассчитанных значений в реальном времени

**3. Контроль качества** — 12 автоматических проверок, падающих таском при отклонении

**Что это даёт**
Цифра доступна в любой момент без участия человека. Отчёт открывается по ссылке, а не собирается по запросу. А если данные приедут неполными — это станет видно в тот же день.

---

# Доступ и документация

## Где смотреть метрики

**Инструмент:** Metabase — `http://vs-bi-dwh:3000/`
**Доступ:** выставляется по запросу к Андрею Левченко.
