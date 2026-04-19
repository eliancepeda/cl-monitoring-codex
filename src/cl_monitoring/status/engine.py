"""Deterministic schedule-health engine for v1."""

from __future__ import annotations

from collections.abc import Iterable, Mapping, Sequence
from datetime import UTC, datetime, timedelta
from statistics import median_high

from cl_monitoring.domain.models import (
    Confidence,
    RunResult,
    RunSummary,
    ScheduleHealth,
    ScheduleHealthStatus,
    ScheduleSnapshot,
    TaskSnapshot,
)
from cl_monitoring.domain.normalizers import build_execution_key

from .models import ObservedFireBaseline, RuntimeBaseline, ScheduleEngineConfig

_ACTIVE_STATUSES = frozenset({"pending", "running"})
_USEFUL_RESULTS = frozenset(
    {RunResult.SUCCESS, RunResult.SUCCESS_PROBABLE, RunResult.PARTIAL_SUCCESS}
)
_FAILED_RESULTS = frozenset({RunResult.FAILED, RunResult.CANCELLED})
_ZERO_DURATION = timedelta(0)
_EPOCH = datetime.min.replace(tzinfo=UTC)


class ScheduleEngine:
    def __init__(self, config: ScheduleEngineConfig | None = None) -> None:
        self._config = config or ScheduleEngineConfig()

    def evaluate(
        self,
        schedule: ScheduleSnapshot,
        scheduled_history: Sequence[TaskSnapshot],
        manual_history: Sequence[TaskSnapshot],
        run_summaries: Mapping[str, RunSummary] | None = None,
        *,
        now: datetime | None = None,
    ) -> ScheduleHealth:
        current_time = _coerce_utc(now)
        summaries = run_summaries or {}
        execution_key = build_execution_key(
            schedule.spider_id, schedule.cmd, schedule.param
        )
        scheduled_tasks = _sorted_tasks(
            task
            for task in scheduled_history
            if task.schedule_id == schedule.id and not task.is_manual
        )
        manual_tasks = _sorted_tasks(
            task
            for task in manual_history
            if task.is_manual and task.execution_key == execution_key
        )
        latest_task = scheduled_tasks[-1] if scheduled_tasks else None
        counters = _base_counters(scheduled_tasks, manual_tasks)
        fire_baseline = _build_fire_baseline(scheduled_tasks, self._config)

        if (
            fire_baseline
            and schedule.enabled
            and current_time > fire_baseline.window_end
        ):
            counters["baseline_samples"] = fire_baseline.sample_count
            counters["lateness_ms"] = _duration_ms(
                current_time - fire_baseline.window_end
            )
            if fire_baseline.sample_count >= self._config.min_fire_interval_samples:
                recovery_task, recovery_count = _find_manual_recovery(
                    manual_tasks,
                    summaries,
                    after=fire_baseline.expected_fire_minute,
                )
                if recovery_task is not None:
                    counters["missed_windows"] = 1
                    counters["manual_recovery_runs"] = recovery_count
                    return _build_health(
                        schedule_id=schedule.id,
                        execution_key=execution_key,
                        health=ScheduleHealthStatus.RECOVERED_BY_MANUAL_RERUN,
                        confidence=Confidence.HIGH,
                        reason_code="recovered_by_manual_success",
                        evidence=[
                            _last_observed_fire_evidence(latest_task),
                            _missed_window_evidence(fire_baseline),
                            (
                                "manual recovery via task "
                                f"{recovery_task.id} after missed scheduled fire"
                            ),
                        ],
                        counters=counters,
                    )
                counters["missed_windows"] = 1
                return _build_health(
                    schedule_id=schedule.id,
                    execution_key=execution_key,
                    health=ScheduleHealthStatus.MISSED_SCHEDULE,
                    confidence=Confidence.HIGH,
                    reason_code="missed_expected_fire_window",
                    evidence=[
                        _last_observed_fire_evidence(latest_task),
                        _missed_window_evidence(fire_baseline),
                    ],
                    counters=counters,
                )

        if latest_task and _scheduled_run_failed(latest_task, summaries):
            anchor = latest_task.end_ts or latest_task.start_ts or latest_task.create_ts
            if anchor is not None:
                recovery_task, recovery_count = _find_manual_recovery(
                    manual_tasks,
                    summaries,
                    after=anchor,
                )
                if recovery_task is not None:
                    counters["manual_recovery_runs"] = recovery_count
                    summary = summaries[latest_task.id]
                    return _build_health(
                        schedule_id=schedule.id,
                        execution_key=execution_key,
                        health=ScheduleHealthStatus.RECOVERED_BY_MANUAL_RERUN,
                        confidence=Confidence.HIGH,
                        reason_code="recovered_by_manual_success",
                        evidence=[
                            _observed_fire_evidence(latest_task),
                            (
                                f"scheduled task {latest_task.id} ended as "
                                f"{summary.run_result.value}"
                            ),
                            (
                                "manual recovery via task "
                                f"{recovery_task.id} after scheduled failure"
                            ),
                        ],
                        counters=counters,
                    )

        if latest_task is None:
            return _build_health(
                schedule_id=schedule.id,
                execution_key=execution_key,
                health=ScheduleHealthStatus.ON_TIME,
                confidence=Confidence.LOW,
                reason_code="insufficient_history_for_strong_judgment",
                evidence=["no observed scheduled fire history yet"],
                counters=counters,
            )

        if (
            fire_baseline
            and schedule.enabled
            and current_time > fire_baseline.window_end
        ):
            counters["baseline_samples"] = fire_baseline.sample_count
            counters["lateness_ms"] = _duration_ms(
                current_time - fire_baseline.window_end
            )
            return _build_health(
                schedule_id=schedule.id,
                execution_key=execution_key,
                health=ScheduleHealthStatus.ON_TIME,
                confidence=Confidence.LOW,
                reason_code="insufficient_history_for_strong_judgment",
                evidence=[
                    _last_observed_fire_evidence(latest_task),
                    (
                        "expected fire window passed, but observed history is too thin "
                        f"({fire_baseline.sample_count} interval sample)"
                    ),
                ],
                counters=counters,
            )

        if latest_task.status == "pending" or (
            latest_task.status == "running" and latest_task.start_ts is None
        ):
            return self._evaluate_queue_state(
                schedule_id=schedule.id,
                execution_key=execution_key,
                latest_task=latest_task,
                current_time=current_time,
                counters=counters,
            )

        if latest_task.status == "running":
            return self._evaluate_running_state(
                schedule_id=schedule.id,
                execution_key=execution_key,
                latest_task=latest_task,
                scheduled_tasks=scheduled_tasks,
                manual_tasks=manual_tasks,
                summaries=summaries,
                counters=counters,
            )

        return self._evaluate_terminal_state(
            schedule_id=schedule.id,
            execution_key=execution_key,
            latest_task=latest_task,
            counters=counters,
            fire_baseline=fire_baseline,
        )

    def _evaluate_queue_state(
        self,
        *,
        schedule_id: str,
        execution_key: str,
        latest_task: TaskSnapshot,
        current_time: datetime,
        counters: dict[str, int],
    ) -> ScheduleHealth:
        wait_duration = _queue_wait_duration(latest_task, current_time)
        wait_ms = _duration_ms(wait_duration)
        over_grace_ms = _duration_ms(
            max(wait_duration - self._config.queue_grace, _ZERO_DURATION)
        )
        counters["lateness_ms"] = over_grace_ms
        evidence = [
            _observed_fire_evidence(latest_task),
            f"waiting {wait_ms} ms since create_ts without start_ts",
        ]
        if wait_duration > self._config.queue_grace:
            return _build_health(
                schedule_id=schedule_id,
                execution_key=execution_key,
                health=ScheduleHealthStatus.DELAYED_START,
                confidence=Confidence.HIGH,
                reason_code="delayed_start_after_observed_fire",
                evidence=evidence,
                counters=counters,
            )
        return _build_health(
            schedule_id=schedule_id,
            execution_key=execution_key,
            health=ScheduleHealthStatus.QUEUED_START,
            confidence=Confidence.HIGH,
            reason_code="queued_after_observed_fire",
            evidence=evidence,
            counters=counters,
        )

    def _evaluate_running_state(
        self,
        *,
        schedule_id: str,
        execution_key: str,
        latest_task: TaskSnapshot,
        scheduled_tasks: Sequence[TaskSnapshot],
        manual_tasks: Sequence[TaskSnapshot],
        summaries: Mapping[str, RunSummary],
        counters: dict[str, int],
    ) -> ScheduleHealth:
        baseline = _build_runtime_baseline(
            scheduled_tasks,
            manual_tasks,
            summaries,
            execution_key=execution_key,
            current_task_id=latest_task.id,
            min_runtime_baseline_samples=self._config.min_runtime_baseline_samples,
        )
        current_runtime_ms = _duration_ms(latest_task.runtime)
        counters["current_runtime_ms"] = current_runtime_ms
        evidence = [_observed_fire_evidence(latest_task)]
        if baseline is None:
            evidence.append(
                f"current runtime {current_runtime_ms} ms has no stable baseline yet"
            )
            return _build_health(
                schedule_id=schedule_id,
                execution_key=execution_key,
                health=ScheduleHealthStatus.RUNNING_AS_EXPECTED,
                confidence=Confidence.LOW,
                reason_code="insufficient_history_for_strong_judgment",
                evidence=evidence,
                counters=counters,
            )

        baseline_runtime_ms = _duration_ms(baseline.baseline_runtime)
        counters["baseline_samples"] = baseline.sample_count
        counters["baseline_runtime_ms"] = baseline_runtime_ms
        if baseline.used_execution_key_fallback:
            evidence.append("runtime baseline used execution_key history fallback")

        threshold_ms = baseline_runtime_ms + _duration_ms(self._config.runtime_grace)
        if current_runtime_ms > threshold_ms:
            evidence.append(
                "current runtime "
                f"{current_runtime_ms} ms exceeds baseline "
                f"{baseline_runtime_ms} ms"
            )
            return _build_health(
                schedule_id=schedule_id,
                execution_key=execution_key,
                health=ScheduleHealthStatus.RUNNING_LONG,
                confidence=_runtime_confidence(
                    baseline, self._config.min_runtime_baseline_samples
                ),
                reason_code="running_exceeds_baseline",
                evidence=evidence,
                counters=counters,
            )

        evidence.append(
            "current runtime "
            f"{current_runtime_ms} ms stays within baseline "
            f"{baseline_runtime_ms} ms"
        )
        return _build_health(
            schedule_id=schedule_id,
            execution_key=execution_key,
            health=ScheduleHealthStatus.RUNNING_AS_EXPECTED,
            confidence=_runtime_confidence(
                baseline, self._config.min_runtime_baseline_samples
            ),
            reason_code="running_within_baseline",
            evidence=evidence,
            counters=counters,
        )

    def _evaluate_terminal_state(
        self,
        *,
        schedule_id: str,
        execution_key: str,
        latest_task: TaskSnapshot,
        counters: dict[str, int],
        fire_baseline: ObservedFireBaseline | None,
    ) -> ScheduleHealth:
        start_delay = _start_delay(latest_task)
        counters["start_delay_ms"] = _duration_ms(start_delay)
        counters["baseline_samples"] = (
            fire_baseline.sample_count if fire_baseline else 0
        )
        evidence = [_observed_fire_evidence(latest_task)]
        if start_delay > self._config.queue_grace:
            evidence.append(
                "start delay "
                f"{_duration_ms(start_delay)} ms from create_ts to start_ts "
                f"exceeds grace {_duration_ms(self._config.queue_grace)} ms"
            )
            return _build_health(
                schedule_id=schedule_id,
                execution_key=execution_key,
                health=ScheduleHealthStatus.DELAYED_START,
                confidence=Confidence.HIGH,
                reason_code="delayed_start_after_observed_fire",
                evidence=evidence,
                counters=counters,
            )

        if start_delay > _ZERO_DURATION:
            evidence.append(
                f"start delay {_duration_ms(start_delay)} ms from create_ts to start_ts"
            )
        return _build_health(
            schedule_id=schedule_id,
            execution_key=execution_key,
            health=ScheduleHealthStatus.ON_TIME,
            confidence=_on_time_confidence(fire_baseline),
            reason_code="on_time_observed_fire",
            evidence=evidence,
            counters=counters,
        )


def _build_health(
    *,
    schedule_id: str,
    execution_key: str,
    health: ScheduleHealthStatus,
    confidence: Confidence,
    reason_code: str,
    evidence: list[str],
    counters: dict[str, int],
) -> ScheduleHealth:
    return ScheduleHealth(
        schedule_id=schedule_id,
        execution_key=execution_key,
        health=health,
        confidence=confidence,
        reason_code=reason_code,
        evidence=evidence,
        counters=counters,
    )


def _base_counters(
    scheduled_tasks: Sequence[TaskSnapshot], manual_tasks: Sequence[TaskSnapshot]
) -> dict[str, int]:
    return {
        "scheduled_tasks_seen": len(scheduled_tasks),
        "manual_tasks_seen": len(manual_tasks),
        "running_tasks_seen": sum(task.status == "running" for task in scheduled_tasks),
        "terminal_tasks_seen": sum(
            _is_terminal(task.status) for task in scheduled_tasks
        ),
        "baseline_samples": 0,
        "current_runtime_ms": 0,
        "baseline_runtime_ms": 0,
        "start_delay_ms": 0,
        "lateness_ms": 0,
        "missed_windows": 0,
        "manual_recovery_runs": 0,
    }


def _build_fire_baseline(
    scheduled_tasks: Sequence[TaskSnapshot], config: ScheduleEngineConfig
) -> ObservedFireBaseline | None:
    fire_minutes = [
        _minute_bucket(task.create_ts) for task in scheduled_tasks if task.create_ts
    ]
    if len(fire_minutes) < 2:
        return None

    intervals = [
        int((current - previous).total_seconds() // 60)
        for previous, current in zip(fire_minutes, fire_minutes[1:], strict=False)
        if current > previous
    ]
    if not intervals:
        return None

    interval_minutes = max(1, median_high(intervals))
    last_fire_minute = fire_minutes[-1]
    expected_fire_minute = last_fire_minute + timedelta(minutes=interval_minutes)
    return ObservedFireBaseline(
        interval_minutes=interval_minutes,
        sample_count=len(intervals),
        last_fire_minute=last_fire_minute,
        expected_fire_minute=expected_fire_minute,
        window_start=expected_fire_minute - config.fire_window,
        window_end=expected_fire_minute + config.fire_window,
    )


def _build_runtime_baseline(
    scheduled_tasks: Sequence[TaskSnapshot],
    manual_tasks: Sequence[TaskSnapshot],
    summaries: Mapping[str, RunSummary],
    *,
    execution_key: str,
    current_task_id: str,
    min_runtime_baseline_samples: int,
) -> RuntimeBaseline | None:
    scheduled_samples = [
        _duration_ms(task.runtime)
        for task in scheduled_tasks
        if task.id != current_task_id
        and task.execution_key == execution_key
        and _is_runtime_sample(task, summaries, require_summary=False)
    ]
    all_samples = list(scheduled_samples)
    used_execution_key_fallback = False

    manual_samples = [
        _duration_ms(task.runtime)
        for task in manual_tasks
        if task.execution_key == execution_key
        and _is_runtime_sample(task, summaries, require_summary=True)
    ]
    if len(all_samples) < min_runtime_baseline_samples and manual_samples:
        all_samples.extend(manual_samples)
        used_execution_key_fallback = True

    if not all_samples:
        return None

    return RuntimeBaseline(
        baseline_runtime=timedelta(milliseconds=median_high(all_samples)),
        sample_count=len(all_samples),
        used_execution_key_fallback=used_execution_key_fallback,
    )


def _find_manual_recovery(
    manual_tasks: Sequence[TaskSnapshot],
    summaries: Mapping[str, RunSummary],
    *,
    after: datetime,
) -> tuple[TaskSnapshot | None, int]:
    recoveries = [
        task
        for task in manual_tasks
        if (event_time := _task_event_time(task)) is not None and event_time > after
        and _is_recovery_task(task, summaries)
    ]
    if not recoveries:
        return None, 0
    return recoveries[0], len(recoveries)


def _is_recovery_task(task: TaskSnapshot, summaries: Mapping[str, RunSummary]) -> bool:
    summary = summaries.get(task.id)
    return bool(summary and summary.run_result in _USEFUL_RESULTS)


def _scheduled_run_failed(
    task: TaskSnapshot, summaries: Mapping[str, RunSummary]
) -> bool:
    summary = summaries.get(task.id)
    return bool(summary and summary.run_result in _FAILED_RESULTS)


def _is_runtime_sample(
    task: TaskSnapshot,
    summaries: Mapping[str, RunSummary],
    *,
    require_summary: bool,
) -> bool:
    if not _is_terminal(task.status) or task.runtime <= _ZERO_DURATION:
        return False

    summary = summaries.get(task.id)
    if summary is None:
        return not require_summary
    return summary.run_result in _USEFUL_RESULTS


def _runtime_confidence(
    baseline: RuntimeBaseline, min_runtime_baseline_samples: int
) -> Confidence:
    if baseline.used_execution_key_fallback:
        return Confidence.MEDIUM
    if baseline.sample_count >= min_runtime_baseline_samples:
        return Confidence.HIGH
    return Confidence.MEDIUM


def _on_time_confidence(fire_baseline: ObservedFireBaseline | None) -> Confidence:
    if fire_baseline and fire_baseline.sample_count >= 2:
        return Confidence.HIGH
    if fire_baseline:
        return Confidence.MEDIUM
    return Confidence.MEDIUM


def _sorted_tasks(tasks: Iterable[TaskSnapshot]) -> list[TaskSnapshot]:
    return sorted(tasks, key=_task_sort_key)


def _task_sort_key(task: TaskSnapshot) -> tuple[datetime, str]:
    return (_task_event_time(task) or _EPOCH, task.id)


def _task_event_time(task: TaskSnapshot) -> datetime | None:
    return task.create_ts or task.start_ts or task.end_ts


def _minute_bucket(value: datetime) -> datetime:
    return _coerce_utc(value).replace(second=0, microsecond=0)


def _queue_wait_duration(task: TaskSnapshot, now: datetime) -> timedelta:
    if task.create_ts is None:
        return _ZERO_DURATION
    return max(now - task.create_ts, _ZERO_DURATION)


def _start_delay(task: TaskSnapshot) -> timedelta:
    if task.create_ts is None or task.start_ts is None:
        return _ZERO_DURATION
    return max(task.start_ts - task.create_ts, _ZERO_DURATION)


def _is_terminal(status: str) -> bool:
    return status not in _ACTIVE_STATUSES


def _duration_ms(value: timedelta) -> int:
    return int(value.total_seconds() * 1000)


def _coerce_utc(value: datetime | None) -> datetime:
    if value is None:
        return datetime.now(UTC)
    if value.tzinfo is None:
        raise ValueError("schedule engine requires timezone-aware datetimes")
    return value.astimezone(UTC)


def _observed_fire_evidence(task: TaskSnapshot) -> str:
    if task.create_ts is None:
        return f"observed schedule task {task.id} without create_ts"
    return f"observed fire at {_isoformat(task.create_ts)} from task {task.id}"


def _last_observed_fire_evidence(task: TaskSnapshot | None) -> str:
    if task is None:
        return "no observed scheduled fire before the expected window"
    return _observed_fire_evidence(task).replace(
        "observed fire", "last observed fire", 1
    )


def _missed_window_evidence(baseline: ObservedFireBaseline) -> str:
    return (
        "expected fire window "
        f"{_isoformat(baseline.window_start)} to {_isoformat(baseline.window_end)} "
        "has no scheduled task"
    )


def _isoformat(value: datetime) -> str:
    return _coerce_utc(value).isoformat().replace("+00:00", "Z")
