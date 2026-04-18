# Agent Prompts

Готовые copy-paste prompt для thread-ов `T0`-`T11` из `MILESTONES.MD`.

## Общие правила запуска

1. Перед стартом любого thread прочитай `AGENTS.md` и актуальный `MILESTONES.MD`.
2. Соблюдай зависимости между thread-ами. Не запускай следующий раньше gate предыдущего.
3. Не делай commit, если тебя об этом явно не попросили.
4. Любой live access к Crawlab: только `GET`, только через `ReadonlyCrawlabClient`, только в рамках allowlisted endpoints.
5. Не храни live token, cookies, auth headers, raw secrets в repo.
6. Любая новая parser rule требует минимум один anonymized fixture и один test.
7. Browser/UI никогда не ходит в Crawlab напрямую.
8. Не тащи `/nodes` в v1.
9. Если prompt помечен как `Plan`, не меняй runtime-код.
10. Если prompt помечен как `Build`, делай минимальное корректное изменение и обязательно запускай проверку.

## Thread Ownership

- `T0`-`T3` запускаются строго последовательно.
- `T4` и `T5` можно запускать параллельно только после `T3` и только в разных worktree.
- `T6` и дальше снова строго последовательно.
- Файлы single-owner: `AGENTS.md`, `DECISIONS.md`, `docs/domain/status-parser-contract.md`, `src/cl_monitoring/domain/models.py`, `src/cl_monitoring/db/*`, `src/cl_monitoring/sync/poller.py`, `src/cl_monitoring/web/*`, `src/cl_monitoring/app.py`.

---

## `T0` — Foundation Verification

**Agent:** Codex `gpt-5.4`  
**Mode:** Build  
**Run when:** Сразу, это текущий стартовый thread.

```text
Работай в thread T0. Это foundation verification thread.

Сначала прочитай:
- AGENTS.md
- MILESTONES.MD
- DECISIONS.md
- docs/adr/0001-readonly-companion.md
- src/integrations/crawlab/readonly_client.py
- src/cl_monitoring/domain/models.py
- src/cl_monitoring/domain/normalizers.py
- src/tools/classify_logs.py
- tests/test_readonly_client.py
- tests/test_normalizers.py
- tests/test_fixture_classifier.py

Цель:
- закрыть verification gate по первым четырём уже реализованным шагам
- не начинать новые milestones

Жёсткие ограничения из AGENTS.md:
- never send non-GET requests to Crawlab
- all Crawlab access must go through ReadonlyCrawlabClient
- runtime classification must be deterministic; no LLM in production logic
- keep schedule_id separate from execution_key
- normalize zero ObjectId and zero time according to AGENTS.md
- do not touch UI, DB, poller, or /nodes work in this thread

Что нужно сделать:
1. Проверить, что ADR 0001 соответствует текущему коду и правилам проекта.
2. Проверить, что ReadonlyCrawlabClient реально закрывает safety invariants:
   - only GET
   - allowlisted paths
   - no absolute URL / host mismatch access
   - zero time normalization
   - results.data = null -> []
3. Проверить, что normalizers и execution_key соответствуют AGENTS.md.
4. Проверить, что текущее parser/classifier направление не нарушает базовые deterministic rules.
5. Исправить только найденные foundation gaps. Не делай полный runtime parser rewrite.

Можно менять только:
- docs/adr/0001-readonly-companion.md
- src/integrations/crawlab/readonly_client.py
- src/cl_monitoring/domain/*
- tests/test_readonly_client.py
- tests/test_normalizers.py
- tests/test_fixture_classifier.py

Не делай:
- fixtures collection
- новые runtime parser modules
- schedule engine
- SQLite/repo/poller
- web/UI

Проверка обязательна:
- ./.venv/bin/pytest -q tests/test_readonly_client.py tests/test_normalizers.py tests/test_fixture_classifier.py
- ./.venv/bin/pytest -q

В финальном отчёте дай:
1. что именно изменено
2. какие проверки запускались и их результат
3. какие foundation gaps остались и почему они не входят в T0
```

---

## `T1` — Status/Parser Contract

**Agent:** OpenCode  
**Mode:** Plan  
**Run when:** Только после успешного `T0`.

```text
Работай в thread T1. Это plan-only thread.

Сначала прочитай:
- AGENTS.md
- MILESTONES.MD
- DECISIONS.md
- docs/adr/0001-readonly-companion.md
- docs/domain/live-contract.md
- docs/domain/normalization-rules.md
- docs/domain/parser-observations.md
- src/cl_monitoring/domain/models.py
- src/cl_monitoring/domain/normalizers.py
- src/tools/classify_logs.py

Цель:
- зафиксировать единый status/parser contract до начала runtime status logic

Ограничения:
- не меняй runtime-код
- не меняй DB/poller/UI
- не придумывай новый scope сверх v1
- опирайся на AGENTS.md и реальные fixture/domain observations, а не на публичную документацию

Сделай:
1. Создай docs/domain/status-parser-contract.md.
2. Зафиксируй входы и выходы для runtime parser и schedule engine.
3. Определи минимальный рабочий shape для:
   - RunSummary
   - ScheduleHealth (или эквивалентного status object)
4. Для обоих контрактов зафиксируй:
   - run_result / health
   - confidence
   - reason_code
   - evidence
   - counters, где это уместно
5. Явно опиши, какие поля shared и не должны меняться параллельно в T4/T5.
6. Явно опиши, как учитываются:
   - manual runs by zero schedule_id
   - live runtime for running tasks
   - observed fire time вместо слепой cron-истины
7. Если это новая архитектурная decision, добавь короткую запись в DECISIONS.md.

Не делай:
- runtime implementation
- fixtures collection
- новые тесты без необходимости

Самопроверка перед финалом:
- в документе нет TODO/TBD
- нет противоречия с AGENTS.md
- T4 и T5 смогут работать, не споря о shape данных

В финальном отчёте дай:
1. путь к созданному/обновлённому документу
2. короткий список зафиксированных контрактов
3. какие файлы теперь нельзя менять параллельно
```

---

## `T2` — Fixture Pack And Golden Corpus

**Agent:** Codex `gpt-5.4`  
**Mode:** Build  
**Run when:** Только после `T1`.

```text
Работай в thread T2. Это fixtures-first build thread.

Сначала прочитай:
- AGENTS.md
- MILESTONES.MD
- docs/domain/status-parser-contract.md
- src/tools/collect_fixtures.py
- src/tools/redact.py
- src/tools/classify_logs.py
- tests/test_collector_discovery.py
- tests/test_redaction.py
- tests/test_fixture_classifier.py
- tests/test_normalizers.py

Цель:
- довести fixture pack и golden corpus до состояния, когда parser/status можно разрабатывать offline

Жёсткие ограничения из AGENTS.md:
- live Crawlab only via ReadonlyCrawlabClient
- GET-only
- raw live responses only in fixtures_raw_local/
- redacted fixtures only in fixtures/
- never store live tokens, auth headers, cookies in repo
- every new parser rule requires one anonymized fixture and one test
- no UI work before fixtures are collected

Что нужно сделать:
1. Проверить, что collector CLI используется как существующая точка входа, а не переписывается ad hoc.
2. Довести структуру fixtures:
   - fixtures/api/
   - fixtures/logs/
   - fixtures/expected/
   - fixtures/manifest.md
3. Закрыть минимальные сценарии:
   - running
   - error
   - success without summary
   - success with put_to_parser
   - partial_success
   - auto_stop
   - cancelled
   - 429 ban
   - manual run
   - schedule history
   - results_by_tid_empty
4. Для каждой log fixture сделать expected YAML с полями:
   - run_result
   - confidence
   - reason_code
   - counters
   - evidence
5. Если нужны правки collector-side classifier для подготовки fixture pack, делай их минимально и не превращай tools/* в runtime layer.
6. Если для safe collection не хватает env/config, не выдумывай fixture data. Остановись и отчитай точный blocker.

Можно менять:
- src/tools/collect_fixtures.py
- src/tools/redact.py
- src/tools/classify_logs.py
- fixtures/**
- tests/test_collector_discovery.py
- tests/test_redaction.py
- tests/test_fixture_classifier.py
- tests/test_normalizers.py

Не делай:
- runtime parser module в src/cl_monitoring/parsers/
- schedule engine
- SQLite/repo/poller
- web/UI

Если есть live env и config, сначала проверь dry-run:
- ./.venv/bin/python -m tools.collect_fixtures --dry-run -v

Если dry-run корректный и safe, выполняй collection через существующий CLI, например:
- ./.venv/bin/python -m tools.collect_fixtures --collect -v
или incremental refresh:
- ./.venv/bin/python -m tools.collect_fixtures --refresh --skip-existing -v

Проверка обязательна:
- ./.venv/bin/pytest -q tests/test_collector_discovery.py tests/test_redaction.py tests/test_fixture_classifier.py tests/test_normalizers.py
- ./.venv/bin/pytest -q

В финальном отчёте дай:
1. какие fixture scenarios добавлены/обновлены
2. какие expected files добавлены
3. запускался ли live dry-run/collect или был safe blocker
4. результаты тестов
```

---

## `T3` — Shared Runtime Types

**Agent:** Codex `gpt-5.4`  
**Mode:** Build  
**Run when:** Только после `T2`.

```text
Работай в thread T3. Это shared runtime boundary thread.

Сначала прочитай:
- AGENTS.md
- MILESTONES.MD
- docs/domain/status-parser-contract.md
- src/cl_monitoring/domain/models.py
- src/cl_monitoring/domain/normalizers.py
- tests/test_models.py
- tests/test_normalizers.py

Цель:
- зафиксировать shared runtime types перед параллельной работой T4 и T5

Жёсткие ограничения:
- не меняй schedule engine и parser implementation целиком
- не лезь в DB/poller/UI
- не используй LLM logic в runtime types
- keep execution_key and schedule_id semantics exactly as in AGENTS.md

Что нужно сделать:
1. Довести RunSummary до рабочего runtime shape по docs/domain/status-parser-contract.md.
2. При необходимости добавить минимальные shared enums / value objects в src/cl_monitoring/domain/.
3. Добавить или расширить tests/test_models.py так, чтобы shared contract был проверяем.
4. Не добавлять лишние abstraction layers.
5. Зафиксировать shape, который T4 и T5 больше не должны менять параллельно.

Можно менять:
- src/cl_monitoring/domain/models.py
- src/cl_monitoring/domain/__init__.py
- tests/test_models.py
- при необходимости минимально tests/test_normalizers.py

Не делай:
- runtime parser implementation
- schedule engine implementation
- collector logic
- SQLite/repo/poller
- UI

Проверка обязательна:
- ./.venv/bin/pytest -q tests/test_models.py tests/test_normalizers.py
- ./.venv/bin/pytest -q

В финальном отчёте дай:
1. какой shared shape теперь зафиксирован
2. какие файлы стали single-owner для следующих thread-ов
3. результаты тестов
```

---

## `T4` — Schedule Engine

**Agent:** Codex `gpt-5.4`  
**Mode:** Build  
**Run when:** После `T3`. Можно параллельно с `T5` только в отдельном worktree.

```text
Работай в thread T4. Это schedule engine thread.

Сначала прочитай:
- AGENTS.md
- MILESTONES.MD
- docs/domain/status-parser-contract.md
- docs/domain/live-contract.md
- docs/domain/normalization-rules.md
- src/cl_monitoring/domain/models.py
- src/cl_monitoring/domain/normalizers.py

Цель:
- реализовать детерминированный schedule engine для v1 без timezone-самообмана

Жёсткие ограничения:
- не менять shared runtime contract в src/cl_monitoring/domain/models.py
- не трогать parser files
- не использовать description как truth source for schedule timing
- не делать blind cron truth without observed fire time logic
- no DB/poller/UI work

Что нужно сделать:
1. Создать src/cl_monitoring/status/models.py.
2. Создать src/cl_monitoring/status/engine.py.
3. Добавить tests/test_schedule_engine.py.
4. Реализовать состояния v1:
   - on_time
   - queued_start
   - delayed_start
   - running_as_expected
   - running_long
   - missed_schedule
   - recovered_by_manual_rerun
5. Использовать:
   - observed fire time по истории задач
   - минутные окна и grace period
   - manual reruns как recovery signal
   - execution_key-aware reasoning для long-running cases
6. Для running tasks учитывать правило AGENTS.md: runtime_duration == 0 -> compute live runtime locally.

Можно менять:
- src/cl_monitoring/status/models.py
- src/cl_monitoring/status/engine.py
- tests/test_schedule_engine.py

Не делай:
- изменения в src/cl_monitoring/parsers/*
- изменения shared contract files
- collector logic
- SQLite/repo/poller
- web/UI

Проверка обязательна:
- ./.venv/bin/pytest -q tests/test_schedule_engine.py
- ./.venv/bin/pytest -q

В финальном отчёте дай:
1. какие rules реализованы
2. как engine трактует manual rerun и long-running cases
3. результаты тестов
```

---

## `T5` — Runtime Crawllib Parser

**Agent:** Codex `gpt-5.4`  
**Mode:** Build  
**Run when:** После `T3`. Можно параллельно с `T4` только в отдельном worktree.

```text
Работай в thread T5. Это runtime parser thread.

Сначала прочитай:
- AGENTS.md
- MILESTONES.MD
- docs/domain/status-parser-contract.md
- docs/domain/parser-observations.md
- src/cl_monitoring/domain/models.py
- src/tools/classify_logs.py
- fixtures/logs/**
- fixtures/expected/**

Цель:
- реализовать runtime parser для crawllib.* отдельно от collector-side logic

Жёсткие ограничения:
- не менять shared runtime contract в src/cl_monitoring/domain/models.py
- не трогать schedule-engine files
- не импортировать runtime logic из src/tools/classify_logs.py
- runtime classification must be deterministic; no LLM
- parser должен поддерживать paginated/incremental log input
- не предполагай, что page=1,size=1000 всегда достаточно

Что нужно сделать:
1. Создать src/cl_monitoring/parsers/crawllib_default.py.
2. Добавить tests/test_crawllib_parser.py.
3. Реализовать распознавание:
   - item_event
   - put_to_parser
   - summary_event
   - isSuccess=true
   - resume success marker
   - sku_not_found
   - 404 gone
   - cancel marker
   - auto_stop
   - 429 ban + error_auto_stop
4. Возвращать детерминированный RunSummary со значениями:
   - success
   - success_probable
   - partial_success
   - rule_stopped
   - cancelled
   - failed
   - unknown
5. Использовать fixtures/logs и fixtures/expected как основной truth source.
6. Если для новой rule не хватает fixture, сначала добавь anonymized fixture + expected + test.

Можно менять:
- src/cl_monitoring/parsers/crawllib_default.py
- src/cl_monitoring/parsers/__init__.py
- tests/test_crawllib_parser.py
- fixtures/logs/**
- fixtures/expected/**

Не делай:
- изменения в schedule engine
- изменения shared contract files
- collector rewrite
- SQLite/repo/poller
- web/UI

Проверка обязательна:
- ./.venv/bin/pytest -q tests/test_crawllib_parser.py
- ./.venv/bin/pytest -q

В финальном отчёте дай:
1. какие markers/rules поддерживаются
2. как parser принимает incremental input
3. какие fixtures/golden tests были добавлены или обновлены
4. результаты тестов
```

---

## `T6` — Local History And Poller ADR

**Agent:** OpenCode  
**Mode:** Plan  
**Run when:** Только после завершения `T4` и `T5`.

```text
Работай в thread T6. Это plan-only thread для persistence/poller.

Сначала прочитай:
- AGENTS.md
- MILESTONES.MD
- DECISIONS.md
- docs/domain/status-parser-contract.md
- src/cl_monitoring/status/*
- src/cl_monitoring/parsers/*
- src/cl_monitoring/db/engine.py
- src/cl_monitoring/db/tables.py
- src/cl_monitoring/db/repo.py
- src/cl_monitoring/sync/poller.py

Цель:
- зафиксировать минимальную architecture для local history и incremental poller

Ограничения:
- не меняй runtime implementation
- не проектируй второй Crawlab client
- не добавляй UI scope
- stay within v1 non-goals from AGENTS.md

Сделай:
1. Создай docs/adr/0002-local-history-and-poller.md.
2. Добавь короткую decision entry в DECISIONS.md.
3. Зафиксируй минимальные таблицы v1:
   - spiders
   - schedules
   - tasks_raw или task_snapshots
   - task_log_cursors
   - run_summaries
   - incidents
   - spider_profiles
4. Зафиксируй polling cadence:
   - spiders/schedules редко
   - tasks чаще
   - logs for running ещё чаще
   - один финальный sync after terminal state
5. Опиши restart-safe инкрементальность.
6. Явно запрети прямой live access из UI и появление второго Crawlab client.

Самопроверка перед финалом:
- в ADR нет TODO/TBD
- ясно описано, как poller связан с parser/status outputs
- ясно описано, как переживается restart

В финальном отчёте дай:
1. путь к ADR
2. краткое summary решения
3. какие файлы T7 должен будет менять
```

---

## `T7` — SQLite, Repo, Poller

**Agent:** Codex `gpt-5.4`  
**Mode:** Build  
**Run when:** Только после `T6`.

```text
Работай в thread T7. Это persistence + poller build thread.

Сначала прочитай:
- AGENTS.md
- MILESTONES.MD
- DECISIONS.md
- docs/adr/0002-local-history-and-poller.md
- docs/domain/status-parser-contract.md
- src/cl_monitoring/status/*
- src/cl_monitoring/parsers/*
- src/cl_monitoring/db/engine.py
- src/cl_monitoring/db/tables.py
- src/cl_monitoring/db/repo.py
- src/cl_monitoring/sync/poller.py
- src/integrations/crawlab/readonly_client.py
- tests/test_repo.py

Цель:
- реализовать local SQLite history и incremental poller так, чтобы UI читал только из локальной БД

Жёсткие ограничения:
- all Crawlab access only through ReadonlyCrawlabClient
- no second raw client in src/cl_monitoring/crawlab/client.py
- browser/UI still out of scope
- no write operations against Crawlab

Что нужно сделать:
1. Реализовать src/cl_monitoring/db/engine.py с учётом WAL requirement из DECISIONS.md.
2. Реализовать DDL и schema setup в src/cl_monitoring/db/tables.py.
3. Реализовать repository methods в src/cl_monitoring/db/repo.py.
4. Реализовать src/cl_monitoring/sync/poller.py.
5. Хранить минимум:
   - spiders
   - schedules
   - task snapshots/raw
   - log cursors
   - run summaries
   - incidents
   - spider profiles
6. Реализовать incremental log sync и final sync после terminal state.
7. Обеспечить restart-safe поведение без reread всего лога.
8. Не превращать src/cl_monitoring/crawlab/client.py во второй независимый клиент. Допустим только thin adapter/re-export или удаление stub.
9. Добавить/расширить tests/test_repo.py и при необходимости создать tests/test_poller.py.

Можно менять:
- src/cl_monitoring/db/*
- src/cl_monitoring/sync/poller.py
- src/cl_monitoring/crawlab/client.py
- tests/test_repo.py
- tests/test_poller.py

Не делай:
- web/UI
- новые runtime classification rules вне необходимости
- /nodes logic

Проверка обязательна:
- ./.venv/bin/pytest -q tests/test_repo.py tests/test_poller.py
- ./.venv/bin/pytest -q

В финальном отчёте дай:
1. какие таблицы и repo methods реализованы
2. как работает cursor-based sync
3. как решён вопрос со вторым client stub
4. результаты тестов
```

---

## `T8` — Minimal Dashboard ADR

**Agent:** OpenCode  
**Mode:** Plan  
**Run when:** Только после `T7`.

```text
Работай в thread T8. Это plan-only thread для minimal dashboard.

Сначала прочитай:
- AGENTS.md
- MILESTONES.MD
- DECISIONS.md
- docs/adr/0002-local-history-and-poller.md
- src/cl_monitoring/db/repo.py
- src/cl_monitoring/web/routes.py
- src/cl_monitoring/web/templates/base.html
- src/cl_monitoring/app.py

Цель:
- спроектировать минимальный v1 dashboard только после стабилизации truth layer

Ограничения:
- не меняй runtime implementation
- не проектируй action buttons
- не делай clone Crawlab UI
- browser must never call Crawlab directly

Сделай:
1. Создай docs/adr/0003-minimal-dashboard.md.
2. Добавь короткую decision entry в DECISIONS.md.
3. Зафиксируй три экрана:
   - Project board
   - Spider detail
   - Incidents
4. Для каждого экрана определи:
   - какие SQLite queries нужны
   - какие поля обязательны
   - какие evidence blocks показывать
5. Явно зафиксируй, что не делаем:
   - action buttons
   - direct Crawlab calls from browser
   - graphs ради graphs
   - много фильтров
   - настройку через UI

Самопроверка перед финалом:
- в ADR нет TODO/TBD
- ясно, какие данные UI берёт из repo
- экран отвечает на вопросы: что крутится, что сломано, что просрочено, что восстановилось

В финальном отчёте дай:
1. путь к ADR
2. краткий список экранов и их обязательных данных
3. какие файлы T9 должен будет менять
```

---

## `T9` — Minimal Server-Rendered UI

**Agent:** Codex `gpt-5.4-mini`  
**Mode:** Build  
**Run when:** Только после `T8`.

```text
Работай в thread T9. Это minimal dashboard build thread.

Сначала прочитай:
- AGENTS.md
- MILESTONES.MD
- DECISIONS.md
- docs/adr/0003-minimal-dashboard.md
- src/cl_monitoring/db/repo.py
- src/cl_monitoring/web/routes.py
- src/cl_monitoring/web/templates/base.html
- src/cl_monitoring/static/style.css
- src/cl_monitoring/app.py

Цель:
- реализовать очень маленький server-rendered dashboard для локального read-only companion

Жёсткие ограничения:
- browser must never call Crawlab directly
- app must bind only to 127.0.0.1 by default
- no action buttons
- no run/restart/cancel
- no multi-user auth
- no direct clone of Crawlab UI

Что нужно сделать:
1. Реализовать routes в src/cl_monitoring/web/routes.py.
2. Добавить/расширить templates в src/cl_monitoring/web/templates/.
3. Обновить src/cl_monitoring/static/style.css.
4. Реализовать create_app/factory в src/cl_monitoring/app.py.
5. Показывать только то, что уже есть в локальной SQLite:
   - running
   - warning
   - critical
   - stale/missed
   - recovered manually
   - parsed summary
   - baseline runtime
   - evidence
6. Использовать established minimal UI patterns, не усложнять дизайн.
7. Добавить route/template tests; при необходимости создать tests/test_web_routes.py.

Можно менять:
- src/cl_monitoring/web/routes.py
- src/cl_monitoring/web/templates/*
- src/cl_monitoring/static/style.css
- src/cl_monitoring/app.py
- tests/test_web_routes.py

Не делай:
- network calls to Crawlab from browser code
- HTMX/JS-heavy UI unless это уже строго необходимо и обосновано
- features вне трёх экранов из ADR

Проверка обязательна:
- ./.venv/bin/pytest -q tests/test_web_routes.py
- ./.venv/bin/pytest -q

В финальном отчёте дай:
1. какие экраны реализованы
2. как UI получает данные только из SQLite
3. результаты тестов
```

---

## `T10` — Structured Markers And Rollout Contract

**Agent:** OpenCode  
**Mode:** Plan  
**Run when:** Только после `T9`.

```text
Работай в thread T10. Это plan-only thread для structured spider markers и rollout contract.

Сначала прочитай:
- AGENTS.md
- MILESTONES.MD
- docs/adr/0003-minimal-dashboard.md
- docs/domain/status-parser-contract.md
- docs/domain/parser-observations.md

Цель:
- спланировать минимальные machine-readable spider markers и безопасный shadow rollout

Ограничения:
- не меняй runtime app code
- не предполагай, что spider source code точно живёт в этом repo
- не проектируй массовое переписывание всех пауков

Сделай:
1. Создай docs/domain/structured-markers.md.
2. Создай docs/rollout/shadow-mode.md.
3. Зафиксируй минимальный JSON marker contract:
   - RUN_START
   - HEARTBEAT
   - RUN_END
4. Опиши, как выбрать 2-3 pilot execution_key:
   - один стабильный короткий
   - один long-running
   - один проблемный anti-bot
5. Опиши, как сравнивать автоматическое решение инструмента с ручной оценкой.
6. Зафиксируй taxonomy расхождений:
   - parser bug
   - bad threshold
   - timezone ambiguity
   - missing profile rule

Самопроверка перед финалом:
- нет TODO/TBD
- rollout contract не зависит от UI wishful thinking
- документ можно выполнить даже если spider code находится вне этого repo

В финальном отчёте дай:
1. пути к созданным документам
2. marker contract в 3-5 строках
3. rollout criteria для pilot execution_key
```

---

## `T11` — Shadow Rollout Support

**Agent:** Manual + agent support  
**Mode:** Ops  
**Run when:** Только после `T10` и после локальной стабилизации.

```text
Работай в thread T11. Это rollout support thread, а не feature-build thread.

Сначала прочитай:
- AGENTS.md
- MILESTONES.MD
- docs/rollout/shadow-mode.md
- docs/domain/structured-markers.md

Цель:
- проверить, что инструмент не врёт, до расширения покрытия на все execution_key

Что нужно сделать:
1. Выбрать 2-3 pilot execution_key по правилам T10.
2. Запустить инструмент в shadow-mode.
3. После каждого terminal run сравнивать:
   - что решил инструмент
   - что решил бы человек вручную
4. Каждое расхождение записывать в одну из категорий:
   - parser bug
   - bad threshold
   - timezone ambiguity
   - missing profile rule
5. Не расширять coverage, пока пилотная серия не совпадает с ручной оценкой.

Не делай:
- массовый rollout на все spiders
- новые feature changes посреди pilot run без отдельного thread
- выводы вида “всё хорошо”, если нет журналируемого сравнения manual vs tool outcome

В финальном отчёте дай:
1. какие execution_key были пилотами
2. сколько terminal runs было сравнено
3. какие расхождения обнаружены
4. можно ли расширять coverage или ещё рано
```
