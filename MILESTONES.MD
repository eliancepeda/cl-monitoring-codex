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
- `src/cl_monitoring/db/*`
- `src/cl_monitoring/sync/poller.py`
- `src/cl_monitoring/web/*`
- `src/cl_monitoring/app.py`

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

## Короткая версия порядка

**`T0 verify foundation -> T1 contract -> T2 fixtures -> T3 shared runtime types -> T3a cleanup tails -> (T4 schedule engine || T5 runtime parser) -> T6 poller ADR -> T7 SQLite/poller -> T8 UI ADR -> T9 dashboard -> T10 markers plan -> T11 shadow rollout`**

Если хочется сократить до одного правила: **до завершения `T5` не трогать SQLite/poller/UI как самостоятельные milestones.**

## Готовые Prompt

Copy-paste prompt для всех thread-ов лежат в `docs/agent-prompts.md`.
