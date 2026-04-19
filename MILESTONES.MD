# MILESTONES

## Цель v1

**Локальный single-user read-only companion для Crawlab, который корректно понимает состояние пауков и расписаний по `tasks + schedules + logs + optional task data`, а уже потом показывает это в минимальном UI.**

Это не клон Crawlab UI и не analytics-платформа. Это безопасный локальный инструмент наблюдения.

## Неподвижные правила

1. Никаких write-операций против Crawlab. Только `GET` через `ReadonlyCrawlabClient`.
2. Источник истины для payload shape: сохранённые фикстуры, а не публичная документация.
3. Runtime-классификация детерминированная. Никакого LLM в production logic.
4. `execution_key = spider_id + normalized cmd + normalized param`.
5. `schedule_id` живёт отдельно от `execution_key`.
6. `000000000000000000000000` = zero-id / null semantics.
7. `0001-01-01T00:00:00Z` = null time.
8. Manual run определяется по zero `schedule_id`.
9. Для `running` задач при `runtime_duration == 0` live runtime считается локально как `now - start_at`.
10. Браузер никогда не ходит в Crawlab напрямую.
11. Не трогаем `/nodes` до стабилизации task/schedule/parser logic.
12. Один milestone = один coherent thread. Параллельные агенты работают только в отдельных worktree и только при разведённых ownership boundaries.

## Что уже считаем сделанным

Нижние первые 4 задания были **выполнены по реализации раньше**, а их
verification gate теперь **закрыт по итогам `T0` + `T3a`**.

| Шаг | Что считаем сделанным | Статус gate |
| --- | --- | --- |
| 1 | Зафиксированы границы readonly companion и ADR по архитектуре | Верифицировано в `T0` |
| 2 | Есть `ReadonlyCrawlabClient` и тесты безопасности | Верифицировано в `T0` |
| 3 | Есть normalizers, snapshots и `execution_key` | Верифицировано в `T0` |
| 4 | Есть черновая реализация parser/classifier направления | Верифицировано в `T3a` |

Практический смысл: **foundation gates закрыты; следующий допустимый шаг
после этой точки — запускать `T4` и `T5` параллельно только в отдельных
worktree.**

Результат `T0` на 2026-04-18:

- `docs/adr/0001-readonly-companion.md`, `ReadonlyCrawlabClient` и domain normalizers приведены к правилам проекта.
- Foundation tests проходят.
- `T0` нашёл 2 оставшихся расхождения в collector/parser направлении (`src/tools/classify_logs.py`):
- `finished` + непустой лог без положительных сигналов всё ещё может повышаться до `success_probable`.
- manual run detection всё ещё слишком широкая для empty/missing `schedule_id` и расходится с domain rule про zero `schedule_id`.

Результат `T3a` на 2026-04-18:

- Legacy tooling в `src/tools/classify_logs.py` доведён до frozen
  project/domain contract без изменения shared runtime contract files.
- `finished` + непустой лог без реальных positive signals больше не
  повышается вслепую до `success_probable`.
- Manual detection в legacy tooling подтверждена только по zero
  `schedule_id`; empty/missing `schedule_id` не считаются manual run.
- Через безопасный project workflow собран реальный anonymized fixture с
  explicit `auto_stop` marker: `fixtures/api/task_ID_820.json`,
  `fixtures/logs/ID_820.log`, `fixtures/expected/task_ID_820_log.yaml`.
- После этого закрыты оставшиеся tails `T0` и `T2`, а foundation stage
  действительно готов к распараллеливанию `T4`/`T5`.

## Что ещё не закрыто после `T15`

После `T14` research smoke и `T15` build thread local-service gap уже закрыт:

- `python -m cl_monitoring.app` больше не является только web viewer поверх
  готовой SQLite; теперь это реальный runtime entrypoint с app-owned lifecycle.
- Runtime env/settings contract выровнен вокруг одного operator path:
  defaults -> `.env` -> process env, с единым token key `CRAWLAB_TOKEN`.
- Live mode и SQLite-only fallback mode теперь определены явно, а partial live
  env приводит к startup error вместо тихого downgrade.
- Exact live operator path больше не падает на stale/orphan
  `schedule -> spider_id -> missing /api/spiders/{id}` reference: startup
  проходит, SQLite продолжает обновляться, board/incidents/detail остаются
  usable через локальный fallback projection.

Что всё ещё не закрыто:

- Внешний marker blocker из `T11` остаётся отдельной проблемой и не считается
  закрытым только потому, что runtime service wiring уже доведён.
- Repo-wide `ruff` / `mypy` debt вне непосредственной поверхности `T12`-`T15`
  всё ещё остаётся в репозитории и не должен смешиваться с фактом, что новый
  local-service runtime path уже реализован, live-verified и покрыт targeted
  runtime tests.

## Что в ветке уже есть, но трогать пока рано

Эти части уже появились в репозитории, но это не значит, что ими нужно заниматься прямо сейчас:

- `src/cl_monitoring/db/*`
- `src/cl_monitoring/sync/poller.py`
- `src/cl_monitoring/web/*`
- `src/cl_monitoring/crawlab/client.py`

До закрытия foundation gates (`T0`-`T5`) их не расширять, кроме минимальной уборки, которая нужна для прохождения тестов или устранения явного конфликта архитектуры.

## Жёсткий порядок по тредам

Ниже порядок, который нужно соблюдать. Здесь важно не только **что** делать, но и **когда именно можно распараллеливать**.

| Порядок | Thread | Режим | Агент | Что делает | Зависимости | Параллельность |
| --- | --- | --- | --- | --- | --- | --- |
| 0 | `T0` | Build | Codex `gpt-5.4` | Верифицирует первые 4 шага и закрывает базовые gates | Нет | Только последовательно |
| 1 | `T1` | Plan | OpenCode | Пишет `status-parser` contract | `T0` | Только последовательно |
| 2 | `T2` | Build | Codex `gpt-5.4` | Доводит fixture pack и golden corpus | `T1` | Только последовательно |
| 3 | `T3` | Build | Codex `gpt-5.4` | Фиксирует shared runtime contracts (`RunSummary`, reason/evidence shape) | `T2` | Только последовательно |
| 3a | `T3a` | Build | OpenCode `gpt-5.4` | Закрывает remaining `T0`/`T2` cleanup tails без изменения shared contract | `T3` | Только последовательно |
| 4 | `T4` | Build | Codex `gpt-5.4` | Реализует schedule engine | `T3` | Можно параллельно с `T5`, но только в отдельном worktree |
| 5 | `T5` | Build | Codex `gpt-5.4` | Реализует runtime `crawllib` parser | `T3` | Можно параллельно с `T4`, но только в отдельном worktree |
| 6 | `T6` | Plan | OpenCode | Пишет ADR по local history и poller | `T4` + `T5` | Только последовательно |
| 7 | `T7` | Build | Codex `gpt-5.4` | Делает SQLite/repo/poller | `T6` | Только последовательно |
| 8 | `T8` | Plan | OpenCode | Пишет ADR по minimal dashboard | `T7` | Только последовательно |
| 9 | `T9` | Build | Codex `gpt-5.4-mini` | Делает минимальный server-rendered UI | `T8` | Только последовательно |
| 10 | `T10` | Plan | OpenCode | Планирует structured spider markers и rollout contract | `T9` | Только последовательно |
| 11 | `T11` | Ops | Manual + agent support | Пилотный shadow rollout на 2-3 `execution_key` | `T10` | Только после локальной стабилизации |
| 12 | `T12` | Plan | OpenCode | Фиксирует runtime service mode и operator flow для одного локального запуска | `T11` | Только последовательно |
| 13 | `T13` | Build | Codex `gpt-5.4` | Доводит working local service: settings, app lifespan, background poller, docs/tests | `T12` | Только последовательно |
| 14 | `T14` | Ops | Manual + agent support | Проверяет live local-service flow end-to-end против локального Crawlab scope | `T13` | Только после локального build gate |
| 15 | `T15` | Build | OpenCode `gpt-5.4` | Устраняет найденный в `T14` live startup blocker устойчиво: orphan spider metadata не валит startup и остаётся видимой в UI | `T14` | Только последовательно |

## Единственное допустимое окно параллелизации

Параллелить имеет смысл только один участок:

1. `T4` — schedule engine.
2. `T5` — runtime parser.

И то только при соблюдении всех условий:

1. `T3` уже завершён и общий контракт зафиксирован.
2. Каждый агент работает в отдельном worktree.
3. `T4` не трогает parser-файлы.
4. `T5` не трогает schedule-engine файлы.
5. Ни один из них не меняет shared contract-файлы.

Если в ходе `T4` или `T5` выясняется, что нужно менять общий контракт, оба треда останавливаются и создаётся маленький новый последовательный thread для shared changes.

## Файлы с single-owner правилом

Следующие зоны нельзя править из двух тредов одновременно:

- `AGENTS.md`
- `DECISIONS.md`
- `docs/domain/status-parser-contract.md`
- `src/cl_monitoring/domain/models.py`
- `src/cl_monitoring/settings.py`
- `src/cl_monitoring/db/*`
- `src/cl_monitoring/sync/poller.py`
- `src/cl_monitoring/web/*`
- `src/cl_monitoring/app.py`
- `.env.example`
- `README.md`

Если thread упирается в один из этих файлов, остальные ждут.

## Подробный план по тредам

### `T0` — Закрыть verification gate по первым 4 шагам

**Цель:** считать фундамент не просто “написанным”, а реально проверенным.

Что делает:

- проверяет актуальность `docs/adr/0001-readonly-companion.md`
- проверяет `ReadonlyCrawlabClient` против safety rules
- проверяет normalizers и `execution_key`
- проверяет, что parser direction не нарушает правило deterministic runtime logic
- фиксирует любые найденные расхождения, но не уходит в DB/UI

Что может править:

- `docs/adr/0001-readonly-companion.md`
- `src/integrations/crawlab/readonly_client.py`
- `src/cl_monitoring/domain/*`
- `tests/test_readonly_client.py`
- `tests/test_normalizers.py`
- `tests/test_fixture_classifier.py`

Готово, когда:

- foundation tests проходят
- safety invariants подтверждены
- нет причины возвращаться к milestone 1-4 до отдельного баг-репорта

Текущий статус:

- `T0` gate закрыт по итогам `T3a`.
- Legacy tooling больше не расходится с frozen contract по двум найденным
  foundation rules: no blind upgrade to `success_probable` и manual only
  by zero `schedule_id`.
- Возвращаться к milestones 1-4 больше не нужно, если не появится новый
  отдельный bug report.

### `T1` — Зафиксировать `status-parser` contract

**Цель:** убрать двусмысленность до начала runtime status logic.

Что делает:

- создаёт `docs/domain/status-parser-contract.md`
- определяет `RunSummary`
- определяет `ScheduleHealth` / schedule status output
- фиксирует поля `confidence`, `reason_code`, `evidence`
- описывает, как parser и schedule engine связываются между собой

Что не делает:

- не пишет runtime код
- не меняет DB/UI

Готово, когда:

- у `T4` и `T5` есть один зафиксированный контракт
- больше не нужно спорить о shape выходных структур

Текущий статус:

- `T1` выполнен и gate закрыт.
- Создан `docs/domain/status-parser-contract.md`.
- В `DECISIONS.md` добавлена decision entry про freeze shared
  status/parser contract перед параллельными `T4` и `T5`.
- Зафиксированы минимальные shapes `RunSummary` и `ScheduleHealth`.
- Зафиксированы shared поля `run_result/health`, `confidence`,
  `reason_code`, `evidence`, `counters`.
- Отдельно зафиксированы invariants для manual runs по zero
  `schedule_id`, live runtime для `running` задач и observed fire time
  вместо blind cron truth.
- До `T3` общий runtime type layer ещё не считается реализованным в коде,
  но ambiguity для `T4`/`T5` на уровне contract снята.

### `T2` — Довести fixture pack и golden corpus

**Цель:** сделать parser/status разработку полностью offline.

Что делает:

- наполняет `fixtures/api`
- наполняет `fixtures/logs`
- наполняет `fixtures/expected`
- собирает `fixtures/manifest.md`
- убеждается, что каждая parser rule имеет fixture + expected + test

Минимальные сценарии:

- running
- error
- success без summary
- success с `put_to_parser`
- partial success
- auto stop
- cancelled
- 429 ban
- manual run
- schedule history
- `results_by_tid_empty`

Готово, когда:

- parser и status engine можно разрабатывать без live token
- golden expectations лежат рядом с log fixtures

Текущий статус:

- `T2` выполнен и gate закрыт.
- Проверен существующий collector CLI: live `--dry-run` и live `--collect`
  запускались только через `python -m tools.collect_fixtures`, без ad hoc
  обходов.
- Collector-side tooling выровнен под правила проекта:
  manual detection в tooling снова опирается только на zero
  `schedule_id`, `expected/*.yaml` перешли на shared
  `run_result/confidence/reason_code/counters/evidence`, а results fixtures
  сохраняются как API fixtures по `/api/results/{col_id}`.
- Собран и выровнен offline corpus в `fixtures/api`, `fixtures/logs`,
  `fixtures/expected`, `fixtures/manifest.md`; redacted fixtures готовы для
  commit, raw live artifacts остаются только в `fixtures_raw_local/`.
- Offline corpus теперь покрывает минимальные сценарии:
  `running`, `error`, `success without summary`, `success with put_to_parser`,
  `partial_success`, `auto_stop`, `cancelled`, `429 ban`, `manual run`,
  `schedule history`, `results_by_tid_empty`.
- В `T3a` добавлен реальный anonymized `auto_stop` fixture из live scope:
  `ID_820` с явным marker `Exception: auto_stop (80) is reached`.
- Fixture-related tests и полный `pytest` проходят в актуальном состоянии:
  `./.venv/bin/pytest -q tests/test_fixture_classifier.py` -> `57 passed`,
  `./.venv/bin/pytest -q` -> `167 passed`.

### `T3` — Зафиксировать shared runtime types

**Цель:** создать безопасную границу перед параллельной работой `T4` и `T5`.

Что делает:

- доводит `RunSummary` в `src/cl_monitoring/domain/models.py`
- при необходимости добавляет shared enums / value objects
- фиксирует shape, который больше не должен меняться параллельно

Готово, когда:

- `T4` и `T5` могут работать независимо
- shared models не нужно менять “на лету”

Текущий статус:

- `T3` выполнен и gate закрыт.
- В `src/cl_monitoring/domain/models.py` зафиксирован runtime shape
  `RunSummary`: `task_id`, `execution_key`, `run_result`, `confidence`,
  `reason_code`, `evidence`, `counters`.
- Добавлен shared runtime type `ScheduleHealth` с frozen shape:
  `schedule_id`, `execution_key`, `health`, `confidence`, `reason_code`,
  `evidence`, `counters`.
- Добавлены минимальные shared enums и frozen catalogs для
  `RunResult`, `Confidence`, `ScheduleHealthStatus`, parser reason codes,
  schedule reason codes и counter keys v1.
- Shared domain models переведены в strict shape mode через запрет лишних
  полей, чтобы `T4` и `T5` не расширяли общий контракт параллельно.
- `error_family` сохранён только как legacy derived detail для
  совместимости с текущим кодом, но primary shared parser contract теперь
  опирается на `reason_code`.
- Экспорт shared runtime surface собран в
  `src/cl_monitoring/domain/__init__.py`.
- `tests/test_models.py` расширен contract-тестами на frozen fields,
  reason-code catalogs, counter keys и защиту от shape drift.
- Проверки пройдены: `./.venv/bin/pytest -q tests/test_models.py
  tests/test_normalizers.py` -> `19 passed`; `./.venv/bin/pytest -q` ->
  `163 passed`.
- Frozen shared contract из `T3` остался без изменений в `T3a`.
- После `T3` и последующего `T3a` cleanup `T4` и `T5` можно запускать
  параллельно только в отдельных worktree и без изменений в shared
  contract files.

### `T3a` — Закрыть remaining `T0`/`T2` tails перед параллельными `T4`/`T5`

**Цель:** убрать последние foundation blockers без изменения frozen shared
contract из `T3`.

Что делает:

- минимально правит `src/tools/classify_logs.py`
- доводит legacy tooling до project/domain contract
- закрывает blind `success_probable` upgrade в finished logs без positive signals
- подтверждает manual detection только по zero `schedule_id`
- добавляет реальный anonymized fixture для explicit `auto_stop`
- добавляет `expected/*.yaml` и тест, который реально проверяет этот сценарий

Что не делает:

- не меняет `docs/domain/status-parser-contract.md`
- не меняет `src/cl_monitoring/domain/models.py`
- не реализует schedule engine
- не реализует production runtime parser
- не трогает DB / poller / UI

Готово, когда:

- закрыты оставшиеся tails из `T0` и `T2`
- fixture corpus покрывает `auto_stop`
- `T4` и `T5` можно запускать параллельно без shared cleanup thread

Текущий статус:

- `T3a` выполнен и gate закрыт.
- Legacy classifier/tooling выровнен с frozen contract по правилам
  `unknown_finished_without_positive_signal` и manual-by-zero-`schedule_id`.
- Добавлен реальный `auto_stop` fixture `ID_820` и соответствующий
  `fixtures/expected/task_ID_820_log.yaml`.
- `tests/test_fixture_classifier.py` теперь проверяет реальный `auto_stop`
  scenario из corpus, а не только synthetic sample.
- Проверки пройдены: `./.venv/bin/pytest -q tests/test_fixture_classifier.py`
  -> `57 passed`; `./.venv/bin/pytest -q` -> `167 passed`.

### `T4` — Реализовать schedule engine

**Цель:** детерминированно определять состояние schedule chain.

Что делает:

- создаёт `src/cl_monitoring/status/models.py`
- создаёт `src/cl_monitoring/status/engine.py`
- добавляет `tests/test_schedule_engine.py`
- использует observed fire time, минутные окна и grace period
- учитывает manual rerun как recovery, но не смешивает его с `schedule_id`

Состояния v1:

- `on_time`
- `queued_start`
- `delayed_start`
- `running_as_expected`
- `running_long`
- `missed_schedule`
- `recovered_by_manual_rerun`

Готово, когда:

- по одной schedule chain можно ответить: missed / queued / delayed / still normal / suspicious / recovered manually

Текущий статус:

- `T4` выполнен и gate закрыт.
- Добавлены `src/cl_monitoring/status/models.py` и
  `src/cl_monitoring/status/engine.py` без изменения shared runtime contract
  files.
- Добавлен `tests/test_schedule_engine.py` с покрытием всех v1 состояний:
  `on_time`, `queued_start`, `delayed_start`, `running_as_expected`,
  `running_long`, `missed_schedule`, `recovered_by_manual_rerun`.
- Schedule engine опирается на observed scheduled `create_ts` history,
  минутные окна и grace period; `description` и blind cron truth не
  используются как источник истины.
- Manual reruns учитываются только как recovery signal по тому же
  `execution_key`; они не смешиваются с schedule chain.
- Для `running_long` baseline берётся сначала из schedule-local history, а
  при sparse history допускается explicit `execution_key` fallback через
  полезные manual runs; это отражается в `evidence` и confidence.
- Для running tasks engine использует уже нормализованный
  `TaskSnapshot.runtime`, включая upstream rule `runtime_duration == 0 ->
  now - start_ts`.
- Проверки пройдены: `./.venv/bin/pytest -q tests/test_schedule_engine.py`
  -> `9 passed`; `./.venv/bin/pytest -q` -> `176 passed`.

### `T5` — Реализовать runtime `crawllib` parser

**Цель:** получать детерминированный `RunSummary` из логов.

Что делает:

- создаёт `src/cl_monitoring/parsers/crawllib_default.py`
- добавляет `tests/test_crawllib_parser.py`
- реализует runtime parser отдельно от `src/tools/classify_logs.py`
- поддерживает paginated/incremental log input

Распознаёт:

- `item_event`
- `put_to_parser`
- `summary_event`
- `isSuccess=true`
- resume success marker
- `sku_not_found`
- `404 gone`
- cancel marker
- `auto_stop`
- `429 ban + error_auto_stop`

Классы результата:

- `success`
- `success_probable`
- `partial_success`
- `rule_stopped`
- `cancelled`
- `failed`
- `unknown`

Готово, когда:

- одинаковый log input всегда даёт одинаковый `RunSummary`
- golden tests читают только из fixtures

Текущий статус:

- `T5` выполнен и gate закрыт.
- Добавлен production runtime parser
  `src/cl_monitoring/parsers/crawllib_default.py` без изменения shared
  runtime contract files и без импорта runtime logic из
  `src/tools/classify_logs.py`.
- Parser принимает ordered log input вместе с явным сигналом полноты и
  поддерживает cumulative/paginated/incremental сценарии; он не
  предполагает, что один fixed fetch вроде `page=1,size=1000` всегда
  достаточен.
- Реализовано детерминированное распознавание `item_event`,
  `put_to_parser`, `summary_event`, `isSuccess=true`, summary success
  marker, `sku_not_found`, `404 gone`, cancel marker, `auto_stop`,
  `429 ban + error_auto_stop`.
- Добавлены `tests/test_crawllib_parser.py` и golden-проверка всего corpus
  из `fixtures/logs` + `fixtures/expected`, включая отдельную проверку
  incremental input.
- Добавлены anonymized fixtures для отсутствовавших rule paths:
  `ID_821` (`cancelled_marker_with_terminal_context`) и `ID_822`
  (`unknown_finished_without_positive_signal`).
- Existing expected fixtures выровнены под runtime truth source: отдельно
  считается `resume_success_markers`, а legacy false positives по
  `gone_404` / `ban_429_markers` и один drift по `item_events` убраны из
  golden corpus.
- Проверки пройдены: `./.venv/bin/pytest -q tests/test_crawllib_parser.py`
  -> `3 passed`; `./.venv/bin/pytest -q` -> `179 passed`.
- После закрытия `T4` и `T5` sequential dependency для `T6` снята:
  status/parser truth layer стабилизирован для следующего planning thread.

### `T6` — ADR по local history и poller

**Цель:** спроектировать persistence после того, как status/parser уже стабилизированы.

Что делает:

- создаёт `docs/adr/0002-local-history-and-poller.md`
- фиксирует минимальные таблицы v1
- фиксирует polling cadence
- описывает restart-safe инкрементальность
- отдельно запрещает появление второго Crawlab client

Минимальные таблицы:

- `spiders`
- `schedules`
- `tasks_raw` или `task_snapshots`
- `task_log_cursors`
- `run_summaries`
- `incidents`
- `spider_profiles`

Готово, когда:

- `T7` можно делать без повторного архитектурного обсуждения

Текущий статус:

- `T6` выполнен и gate закрыт.
- Добавлен `docs/adr/0002-local-history-and-poller.md`.
- Зафиксирован runtime flow `ReadonlyCrawlabClient -> normalizers -> SQLite -> parser/status -> incidents`.
- Зафиксированы минимальные v1 таблицы: `spiders`, `schedules`,
  `task_snapshots`, `task_log_cursors`, `run_summaries`, `incidents`,
  `spider_profiles`.
- Зафиксированы poll cadence, final log sync после terminal state и
  restart-safe resume через persisted cursor state.
- Отдельно запрещён второй Crawlab client в runtime package; допустим только
  thin re-export / adapter над существующим `ReadonlyCrawlabClient`.
- После `T6` реализацию `T7` можно делать без повторного архитектурного
  обсуждения persistence/poller слоя.

### `T7` — Реализовать SQLite, repo и poller

**Цель:** получить устойчивое локальное состояние, независимое от live UI polling.

Что делает:

- наполняет `src/cl_monitoring/db/engine.py`
- наполняет `src/cl_monitoring/db/tables.py`
- наполняет `src/cl_monitoring/db/repo.py`
- наполняет `src/cl_monitoring/sync/poller.py`
- делает incremental log sync
- делает final sync после terminal state
- переживает restart без reread всего лога

Специальное правило:

- `src/cl_monitoring/crawlab/client.py` не должен превратиться во второй независимый клиент; допустим только re-export / thin adapter или удаление stub

Готово, когда:

- после перезапуска локальное состояние и incidents не теряются
- UI сможет читать только из SQLite

Текущий статус:

- `T7` выполнен и gate закрыт.
- Реализован `src/cl_monitoring/db/engine.py`: SQLite connection setup с
  обязательным `PRAGMA journal_mode=WAL`, `foreign_keys=ON` и local
  concurrency-safe настройками.
- Реализован `src/cl_monitoring/db/tables.py`: полный DDL, индексы и schema
  setup для `spiders`, `schedules`, `task_snapshots`, `task_log_cursors`,
  `run_summaries`, `incidents`, `spider_profiles`.
- Реализован `src/cl_monitoring/db/repo.py`: upsert/read методы для snapshots,
  cursor state, derived summaries, incidents и local-only spider profiles.
- Реализован `src/cl_monitoring/sync/poller.py`: task/schedule/spider sync,
  incremental log sync, final sync после terminal state, reparsing в
  `RunSummary` и derived incident projection.
- Restart-safe инкрементальность идёт через persisted `task_log_cursors`:
  poller продолжает с `next_page - 1`, делает one-page overlap merge и не
  reread-ит весь лог после рестарта.
- `src/cl_monitoring/crawlab/client.py` оставлен как thin re-export
  существующего `ReadonlyCrawlabClient`, без второго независимого transport
  layer.
- Добавлены `tests/test_repo.py` и `tests/test_poller.py` с покрытием schema,
  repo lifecycle, cursor resume/final sync и incident lifecycle.
- Проверки пройдены:
  `./.venv/bin/pytest -q tests/test_repo.py tests/test_poller.py` ->
  `6 passed`; `./.venv/bin/pytest -q` -> `185 passed`.

### `T8` — ADR по minimal dashboard

**Цель:** спроектировать UI только после стабилизации backend truth layer.

Что делает:

- создаёт `docs/adr/0003-minimal-dashboard.md`
- фиксирует 3 экрана
- определяет SQLite queries и обязательные поля
- фиксирует evidence blocks
- отдельно фиксирует, что action buttons не делаем

Экраны v1:

- `Project board`
- `Spider detail`
- `Incidents`

Готово, когда:

- `T9` может строить UI без догадок о данных

Текущий статус:

- `T8` выполнен и gate закрыт.
- Создан `docs/adr/0003-minimal-dashboard.md` со scope для трёх экранов:
  `Project board`, `Spider detail`, `Incidents`.
- ADR фиксирует evidence-first UI поверх локальной truth layer и запрещает
  browser-side Crawlab calls, action buttons, dense filters и UI settings.
- Для `T9` заранее зафиксированы SQLite-backed read patterns, обязательные поля,
  evidence blocks и non-goals, так что реализация UI идёт без догадок о данных.

### `T9` — Реализовать минимальный server-rendered UI

**Цель:** дать рабочий локальный dashboard, а не копию Crawlab.

Что делает:

- наполняет `src/cl_monitoring/web/routes.py`
- наполняет `src/cl_monitoring/web/templates/*`
- наполняет `src/cl_monitoring/static/style.css`
- наполняет `src/cl_monitoring/app.py`
- использует только SQLite как источник данных

Что показывает:

- running
- warning
- critical
- stale/missed
- recovered manually
- parsed summary
- baseline runtime
- evidence

Что не делает:

- direct Crawlab calls from browser
- action buttons
- graphs ради graphs
- много фильтров
- настройку через UI

Готово, когда:

- один экран отвечает на вопросы: что крутится, что сломано, что просрочено, что восстановилось

Текущий статус:

- `T9` выполнен и gate закрыт.
- `T9` закрыл именно UI scope поверх локальной SQLite, а не полный operator
  runtime flow.
- Реализованы server-rendered routes в `src/cl_monitoring/web/routes.py`:
  `GET /`, `GET /spiders/{spider_id}`, `GET /incidents`.
- Реализованы шаблоны `project_board.html`, `spider_detail.html`,
  `incidents.html` и обновлён общий `base.html`.
- Реализован минимальный local-only UI в `src/cl_monitoring/static/style.css`
  без action buttons, без browser-side Crawlab calls и без clone-подхода к
  Crawlab UI.
- Реализован `create_app()` в `src/cl_monitoring/app.py`; app по умолчанию
  монтирует static files и запускается только на `127.0.0.1`.
- Все browser-visible данные читаются только из локальной SQLite через
  repository-backed reads над `spiders`, `schedules`, `task_snapshots`,
  `run_summaries`, `incidents`.
- Dashboard показывает `running`, `warning`, `critical`, `stale/missed`,
  `recovered manually`, `parsed summary`, `baseline runtime`, `evidence`.
- Добавлен `tests/test_web_routes.py` с проверкой трёх экранов и 404 case.
- Старт `python -m cl_monitoring.app` сейчас поднимает web server, но не
  стартует poller и не формирует единый local service flow; этот remaining
  product gap вынесен в `T12`-`T14`.
- Проверки пройдены:
  `./.venv/bin/pytest -q tests/test_web_routes.py` -> `4 passed`;
  `./.venv/bin/pytest -q` -> `189 passed`.

### `T10` — Спланировать structured spider markers и rollout contract

**Цель:** только после полезного parser/status слоя улучшать свои spider logs.

Что делает:

- фиксирует JSON markers `RUN_START`, `HEARTBEAT`, `RUN_END`
- выбирает 2-3 pilot `execution_key`
- описывает как сравнивать решение инструмента с ручной оценкой
- описывает taxonomy расхождений

Категории расхождений:

- parser bug
- bad threshold
- timezone ambiguity
- missing profile rule

Готово, когда:

- rollout можно делать без самообмана и без массового переписывания всех пауков

Текущий статус:

- `T10` выполнен и gate закрыт.
- Добавлены `docs/domain/structured-markers.md` и
  `docs/rollout/shadow-mode.md`.
- Зафиксирован минимальный additive marker contract: single-line JSON marker
  с envelope `kind=clm_spider_marker`, `v=1`, UTC `ts`, marker types
  `RUN_START` / `HEARTBEAT` / `RUN_END`, optional `profile` / `counters` и
  terminal `outcome` + `reason_code` только для `RUN_END`.
- Rollout contract привязан к pilot-first расширению по `execution_key`, а не
  к глобальному включению по `spider_id`.
- Зафиксированы три pilot bucket-а (`stable short`, `long-running`,
  `problematic anti-bot`), blind manual-vs-shadow comparison procedure,
  mismatch taxonomy и pass criteria перед любым расширением coverage.

### `T11` — Shadow rollout

**Цель:** не расширять покрытие, пока инструмент не доказал, что не врёт.

Что делает:

- берёт 2-3 pilot `execution_key`
- запускает инструмент в shadow-mode
- сравнивает автоматическое решение с ручной оценкой после terminal runs
- записывает каждое расхождение в одну из категорий `T10`

Готово, когда:

- серия пилотных запусков совпадает с ручной оценкой
- только после этого можно расширять покрытие

Текущий статус:

- `T11` pilot support выполнен, но rollout gate пока не закрыт.
- Для пилота выбраны 3 `execution_key` по bucket-правилам `T10`:
  `684c7259e944d42ba023f686:python stockfirmati.it.py:`,
  `698f03bdf186d32df354edc8:python massimodutti_de_it.py:-tags de,it -as 550`,
  `680a20d1e944d42ba023c49f:python zara.com.py:-tags de,es,hu,it,lt,pl`.
- В shadow discipline сравнено `15` terminal runs (`5` на каждый pilot key)
  с blind manual review до просмотра tool outcome.
- Во всех `15/15` сравнениях marker lines отсутствовали
  (`RUN_START = 0`, `HEARTBEAT = 0`, `RUN_END = 0`), поэтому pilot уже не
  проходит pass criteria из `docs/rollout/shadow-mode.md`.
- В `T11a-research` raw evidence подтверждён `parser bug`: для
  `69c63900a2fc400a16fad74c` и `69c62af0a2fc400a16fad73e` terminal
  `CancelledError` оказался transport wrapper над `parse.php` crash after
  positive progress, а не operator cancellation.
- В `T11b-build` parser сужен минимально: `task.status=cancelled` и plain
  cancel markers по-прежнему дают `cancelled`, но `task.status=error` +
  wrapped `CancelledError: parse.php error ...` больше не short-circuit-ится
  в `cancelled` и падает дальше в существующую deterministic logic.
- Для regression coverage добавлен anonymized fixture `ID_823`
  (`partial_success` with wrapped `CancelledError`) и сохранён guardrail на
  true cancelled через existing fixture `ID_821`.
- После `T11b` replay тех же `15` pilot runs снова совпадает с blind manual
  baseline (`15/15 match`), parser bug на frozen pilot set закрыт.
- Coverage не расширялся и rollout gate автоматически не открылся: marker
  lines по-прежнему отсутствуют, поэтому pass criteria `docs/rollout/shadow-mode.md`
  всё ещё не выполнены.
- В `T11c-research` raw evidence по тому же frozen 15-run set подтверждён
  следующий blocker: во всём доступном `full.log` и raw `logs_page_*.json`
  нет exact marker lines `kind=clm_spider_marker`, `RUN_START`, `HEARTBEAT`,
  `RUN_END` и нет marker-like near-miss envelope.
- `T11c-research` также подтвердил, что blocker уже не в parser semantics и не
  в текущем in-repo log assembly path: для всех `15/15` runs assembled
  `full.log` line count совпадает с Crawlab `total`, а raw logs сохраняют
  другой structured content вроде inline JSON и terminal traceback.
- После этого подготовлен внешний producer-side handoff: минимальный следующий
  шаг для `T11` находится outside repo и состоит в exact v1 marker emission по
  тем же 3 pilot `execution_key` внутри raw Crawlab logs.
- Новый build thread в этом repo до этого шага не открывать: сначала нужен
  внешний producer/runtime change, затем новые terminal runs с raw marker lines
  и только после этого повторный `T11` replay на том же pilot scope.
- Этот внешний marker blocker не должен смешиваться с local service gap:
  runtime service wiring и operator flow по-прежнему нужно закрыть отдельно в
  `T12`-`T14`.

### `T12` — ADR по runtime service mode и operator flow

**Цель:** после появления SQLite/poller/UI закрыть главный product gap: как
владелец проекта запускает один локальный сервис и получает auto-updating
dashboard без ad hoc shell wiring.

Что делает:

- создаёт `docs/adr/0004-runtime-service-mode.md`
- фиксирует, что именно означает “запустить сервис” для v1
- определяет default runtime mode для `python -m cl_monitoring.app`
- определяет, владеет ли web app background poller lifecycle или нужен другой
  первичный operator entrypoint
- фиксирует единый runtime env/settings contract
- фиксирует поведение при наличии и отсутствии live credentials
- описывает startup/shutdown lifecycle, initial sync policy и failure mode

Что не делает:

- не меняет parser/status semantics
- не меняет marker rollout contract
- не добавляет новые UI экраны или action buttons

Готово, когда:

- `T13` можно реализовать без спора о том, должен ли старт сервера сам
  поднимать poller
- нет двусмысленности между `CRAWLAB_TOKEN` и `CRAWLAB_API_TOKEN` на уровне
  runtime operator flow
- ясно, что происходит в live mode и в SQLite-only fallback mode

Текущий статус:

- `T12` выполнен и gate закрыт.
- Добавлен `docs/adr/0004-runtime-service-mode.md`.
- Зафиксировано, что `python -m cl_monitoring.app` является единственным v1
  operator entrypoint для обычного owner workflow.
- Зафиксировано, что lifecycle background poller принадлежит app lifespan, а не
  отдельному daemon command и не browser-side flow.
- Зафиксирован единый runtime settings contract: `CRAWLAB_BASE_URL`,
  `CRAWLAB_TOKEN`, `CL_MONITORING_DB_PATH`, `APP_PORT`, с fixed bind на
  `127.0.0.1`.
- Зафиксированы runtime mode rules: full live env -> live local-service mode,
  обе live settings absent -> SQLite-only fallback mode, partial live env ->
  explicit startup error.
- Отдельно зафиксированы initial forced sync на startup, clean shutdown order и
  runtime-safe normalization для `CRAWLAB_BASE_URL` с хвостом `/api`.
- После `T12` для `T13` больше не осталось архитектурной двусмысленности по
  operator flow, env contract и ownership poller lifecycle.

### `T13` — Реализовать working local service runtime

**Цель:** довести проект до состояния, где `python -m cl_monitoring.app` может
быть реальным локальным сервисом, а не только web viewer поверх уже готовой
SQLite.

Что делает:

- наполняет `src/cl_monitoring/settings.py`
- доводит runtime env loading и единый settings contract
- выравнивает `.env.example` под реальный runtime
- дорабатывает `src/cl_monitoring/app.py` с lifespan/startup/shutdown wiring
- интегрирует background poller по решению `T12`
- обеспечивает clean resource lifecycle для SQLite и readonly client
- документирует новый operator flow в `README.md`
- добавляет/расширяет runtime tests для app/service wiring

Жёсткие ограничения:

- никакого второго Crawlab client
- никаких write-операций против Crawlab
- браузер по-прежнему не ходит в Crawlab напрямую
- без новых UI экранов и без action buttons
- marker rollout semantics не трогать

Готово, когда:

- есть один понятный local start path для owner workflow
- при валидном live env web app поднимает poller и обновляет локальную SQLite
- dashboard продолжает читать только из SQLite
- поведение без live env тоже явно определено и покрыто тестами

Текущий статус:

- `T13` реализован по runtime/product surface.
- `src/cl_monitoring/settings.py` доведён до единого runtime settings layer на
  существующих project dependencies: defaults + `.env` + process env,
  normalized `db_path`, normalized `CRAWLAB_BASE_URL`, mode selection и явный
  runtime contract.
- `.env.example` и `README.md` выровнены под один штатный operator flow без ad
  hoc shell remapping токена.
- `src/cl_monitoring/app.py` доведён до app-managed lifespan: startup создаёт
  writer SQLite connection и `LocalRepository`, в live mode создаёт единственный
  `ReadonlyCrawlabClient`, выполняет `poller.sync_once(force=True)` до ready
  state и запускает один background poller task; shutdown делает clean resource
  cleanup в обратном порядке.
- `src/integrations/crawlab/readonly_client.py` теперь принимает явный token из
  runtime settings path; legacy `CRAWLAB_API_TOKEN` оставлен только как
  compatibility fallback, а не как нормальный operator contract.
- Добавлен `tests/test_app_runtime.py`; `tests/test_web_routes.py` выровнен под
  явный SQLite-only app startup. Runtime wiring покрыт тестами для live mode,
  SQLite-only mode, partial env failure и startup failure cleanup.
- Проверки пройдены:
  `./.venv/bin/pytest -q tests/test_poller.py tests/test_web_routes.py tests/test_app_runtime.py`
  -> `12 passed`; `./.venv/bin/pytest -q` -> `197 passed`.
- Обязательные `./.venv/bin/ruff check src tests` и `./.venv/bin/mypy src`
  запускались, но repo-wide остаются красными из-за pre-existing debt вне
  непосредственной поверхности `T13`; при этом targeted `ruff` / `mypy` checks
  для новых runtime files из `T13` проходят.
- Product-wise главный результат `T13`: теперь существует один понятный local
  start path для owner workflow, а remaining runtime verification вынесен в
  `T14`.

### `T14` — Live local-service smoke

**Цель:** не объявлять проект “рабочим сервисом” только по unit/integration
tests; отдельно подтвердить живой operator flow против локального Crawlab scope.

Что делает:

- запускает итоговый local service runtime по пути из `T13`
- проверяет, что SQLite создаётся или обновляется без ручного ad hoc wiring
- проверяет, что dashboard routes отвечают данными из локальной SQLite
- подтверждает, что live доступ остаётся readonly и идёт только через
  `ReadonlyCrawlabClient`
- фиксирует точный operator flow или точный blocker, если smoke не проходит

Что не делает:

- не открывает новые build changes посреди smoke без отдельного thread
- не смешивает runtime service smoke с marker rollout `T11`

Готово, когда:

- один локальный запуск smoke-validated на реальном scope
- или зафиксирован конкретный remaining blocker, который мешает этому

Текущий статус:

- `T14` выполнен как live operator-path smoke/research thread.
- Exact path `./.venv/bin/python -m cl_monitoring.app` был запущен против
  локального Crawlab scope.
- Smoke нашёл один конкретный startup blocker в initial
  `await poller.sync_once(force=True)`: `sync_spiders()` падал на upstream stale
  reference, где `GET /api/schedules/{id}` возвращал `spider_id`, а
  `GET /api/spiders/{spider_id}` возвращал `404 Not Found`.
- По тому же exact code path другие spider detail запросы отвечали `200`, а
  `GET /api/tasks` по problematic `schedule_id` / `spider_id` возвращал `0`
  records; это зафиксировало проблему как реальный upstream orphan reference, а
  не как local path/id wiring bug.
- Blocker был вынесен в отдельный build thread `T15`, а не замаскирован broad
  exception handling внутри smoke.

### `T15` — Устранить live startup blocker из `T14`

**Цель:** сделать local service устойчивым к upstream stale
`schedule -> spider_id` reference так, чтобы startup не падал, broken reference
не исчезал из UI и при этом реальные runtime ошибки по-прежнему оставались
fatal.

Что делает:

- обрабатывает только `httpx.HTTPStatusError` со статусом `404` для individual
  `GET /api/spiders/{id}` внутри `Poller.sync_spiders()`
- не глотает `401/403/5xx`, timeout, transport errors и другие неожиданные
  случаи
- переводит missing spider detail в явное локальное unresolved representation,
  а не в silent disappearance
- перестраивает read-model fallback в `routes.py` из union локальных `spiders`,
  `schedules`, `task_snapshots` без тяжёлой DB migration
- показывает unresolved spider на board/detail/incidents с явным badge/state

Что не делает:

- не меняет operator path из `T13`
- не добавляет новый Crawlab client
- не ослабляет readonly boundary
- не делает broad `except Exception`
- не глотает все `4xx`
- не вводит schema migration без необходимости

Готово, когда:

- exact command `./.venv/bin/python -m cl_monitoring.app` проходит live startup
- `GET /` и `GET /incidents` отвечают `200`
- unresolved spider не ломает service-level UX и detail page может рендериться
- non-404 spider detail failures по-прежнему валят startup

Текущий статус:

- `T15` выполнен и gate закрыт.
- В `src/cl_monitoring/sync/poller.py` `404` по spider detail теперь трактуется
  как unresolved metadata miss, а не как startup-fatal truth error; остальные
  ошибки по-прежнему пробрасываются вверх.
- Локальный spider read-model больше не зависит только от таблицы `spiders`:
  `src/cl_monitoring/web/routes.py` строит fallback projection из `spiders` +
  `schedules` + `task_snapshots`, поэтому unresolved spider остаётся видимой на
  board/incidents/detail даже без live spider row.
- `src/cl_monitoring/web/templates/project_board.html`,
  `src/cl_monitoring/web/templates/spider_detail.html` и
  `src/cl_monitoring/web/templates/incidents.html` теперь явно показывают
  unresolved state вместо того, чтобы выдавать missing upstream metadata за
  normal healthy spider.
- Добавлены/обновлены regression tests в `tests/test_poller.py` и
  `tests/test_web_routes.py`: `404` по одному spider detail больше не валит
  sync/startup, unresolved spider остаётся видимой в dashboard surface,
  detail route рендерится с `200`, incidents links не ведут в `404`, а non-404
  failures всё ещё surface-ятся.
- Related runtime verification через `tests/test_app_runtime.py` повторно
  подтверждён.
- Проверки пройдены:
  `./.venv/bin/pytest -q tests/test_poller.py tests/test_web_routes.py tests/test_app_runtime.py`
  -> `16 passed`.
- Live verification пройден тем же operator path:
  `./.venv/bin/python -m cl_monitoring.app` дошёл до
  `Application startup complete`; `GET /` -> `200`; `GET /incidents` -> `200`;
  `GET /spiders/66a25558116add6c8f235740` -> `200` с explicit unresolved detail
  page, построенной из локальной SQLite.
- Readonly boundary осталась неизменной: только `GET`, только через
  `ReadonlyCrawlabClient`, browser-side Crawlab calls не добавлены.

## Чего не делать раньше времени

1. Не начинать с фронта.
2. Не считать `task.status` финальной истиной.
3. Не строить baseline только по `spider_id` без `cmd + param`.
4. Не смешивать scheduled runs и manual reruns.
5. Не использовать `description` schedule как источник truth для времени.
6. Не считать, что первых 1000 строк лога всегда достаточно.
7. Не отдавать live token в контекст до fixture/test discipline.
8. Не использовать LLM как runtime classifier.
9. Не переписывать все spider logs сразу.
10. Не тащить `/nodes` в v1 раньше parser/status stability.
11. Не маскировать отсутствие runtime service wiring ad hoc shell one-liners как
    “готовый” operator flow.

## Короткая версия порядка

**`T0 verify foundation -> T1 contract -> T2 fixtures -> T3 shared runtime types -> T3a cleanup tails -> (T4 schedule engine || T5 runtime parser) -> T6 poller ADR -> T7 SQLite/poller -> T8 UI ADR -> T9 dashboard -> T10 markers plan -> T11 shadow rollout -> T12 runtime service ADR -> T13 working local service build -> T14 live service smoke -> T15 orphan spider startup hardening`**

Если хочется сократить до одного правила: **до завершения `T5` не трогать SQLite/poller/UI как самостоятельные milestones.**

Если говорить только о local-service path, smoke-validated startup blocker из
`T14` уже закрыт в **`T15`**. Главный remaining blocker вне этой поверхности —
по-прежнему внешний marker rollout gap из **`T11`**.

## Готовые Prompt

Copy-paste prompt для всех thread-ов лежат в `docs/agent-prompts.md`.
