"""Lease-aware CP4 execution shell; deliberately independent of processor.py."""

from __future__ import annotations

import argparse
import json
import time
import uuid
from datetime import datetime, timezone
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Mapping, Protocol

from pipeline.config import config
from pipeline.p1c_intake import (
    ReupIntakeError,
    accept_reup_envelope,
    iter_reup_envelopes,
    load_reup_envelope,
)
from pipeline.p1c_lease_client import LeaseBridgeResponse, LeaseBridgeUnavailable
from pipeline.p1c_lease_client import LeaseBridgeClient
from pipeline.p1c_processing import launch_processing_child, recover_reup_completion
from pipeline.p1c_status import (
    ReupStatusError,
    inspect_existing_accepted_status,
    load_status_event,
    publish_lease_lost_status,
    publish_started_status,
)


class HeavyWorkHandle(Protocol):
    def poll(self) -> object: ...
    def terminate(self) -> None: ...
    def wait(self, timeout: float | None = None) -> object: ...


@dataclass(frozen=True)
class WorkerResult:
    outcome: str
    lease_id: str | None = None


def run_lease_aware_work(job: Mapping[str, object], status_root, lease_client, start_work: Callable[[Mapping[str, object]], HeavyWorkHandle], *, monotonic: Callable[[], float] = time.monotonic, sleep: Callable[[float], None] = time.sleep, occurred_at_utc: str | None = None, utc_now: Callable[[], str] | None = None) -> WorkerResult:
    """Admit one accepted job, start only after a valid lease, then renew it."""
    try:
        accepted = inspect_existing_accepted_status(status_root, job)
    except ReupStatusError:
        return WorkerResult("accepted_status_unavailable")
    if accepted is None:
        return WorkerResult("accepted_status_unavailable")
    try:
        admission: LeaseBridgeResponse = lease_client.acquire(job)
    except LeaseBridgeUnavailable:
        return WorkerResult("lease_unavailable")
    if not admission.granted or admission.lease is None:
        return WorkerResult("capacity_unavailable")
    lease_id = str(admission.lease["lease_id"])
    try:
        publish_started_status(status_root, job, lease_id, occurred_at_utc=occurred_at_utc)
    except Exception:
        return _cleanup_before_work(lease_client, job, lease_id, "started_status_unavailable")
    try:
        handle = start_work(job)
    except Exception:
        return _cleanup_before_work(lease_client, job, lease_id, "work_start_failed")
    now = utc_now or _utc_now
    confirmed_expiry = str(admission.lease["expires_at"])
    next_heartbeat = monotonic() + admission.heartbeat_seconds
    while handle.poll() is None:
        if now() >= confirmed_expiry:
            handle.terminate()
            try:
                publish_lease_lost_status(status_root, job, lease_id)
            except ReupStatusError:
                pass
            return WorkerResult("lease_lost", lease_id)
        if monotonic() < next_heartbeat:
            sleep(max(0.0, next_heartbeat - monotonic()))
        try:
            renewed = lease_client.heartbeat(job, lease_id)
            if renewed.lease is None:
                raise LeaseBridgeUnavailable("missing renewed lease")
        except LeaseBridgeUnavailable:
            handle.terminate()
            try:
                publish_lease_lost_status(status_root, job, lease_id)
            except ReupStatusError:
                pass
            return WorkerResult("lease_lost", lease_id)
        confirmed_expiry = str(renewed.lease["expires_at"])
        next_heartbeat = monotonic() + renewed.heartbeat_seconds
    try:
        released = lease_client.release(job, lease_id)
    except LeaseBridgeUnavailable:
        return WorkerResult("work_completed_release_unconfirmed", lease_id)
    return WorkerResult("work_completed_lease_released" if released.lease is not None else "work_completed_release_unconfirmed", lease_id)


def _cleanup_before_work(lease_client, job: Mapping[str, object], lease_id: str, outcome: str) -> WorkerResult:
    try:
        lease_client.release(job, lease_id)
    except LeaseBridgeUnavailable:
        return WorkerResult(outcome + "_release_unconfirmed", lease_id)
    return WorkerResult(outcome, lease_id)


def _utc_now() -> str:
    value = datetime.now(timezone.utc)
    return value.strftime("%Y-%m-%dT%H:%M:%S.") + f"{value.microsecond // 1000:03d}Z"


def run_canonical_once(*, reup_job_id: str | None = None) -> dict[str, object]:
    """Run or recover one canonical Reup attempt without duplicating CP5."""

    input_root = _require_input_root()
    status_root = _require_status_configuration()
    paths = iter_reup_envelopes(input_root)
    if reup_job_id is not None:
        requested = _canonical_uuid(reup_job_id)
        paths = tuple(
            path for path in paths
            if path.name == f"dubvi-reup-job-{requested}.job.json"
        )
        if not paths:
            raise ReupIntakeError("requested Reup job envelope was not found")
        if len(paths) > 1:
            raise ReupIntakeError("requested Reup job id has multiple canonical envelopes")
    for envelope_path in paths:
        document = load_reup_envelope(envelope_path, input_root)
        accepted = accept_reup_envelope(envelope_path, input_root, status_root)
        job_id = str(document["reup_job_id"])
        events = {
            sequence: _optional_status_event(status_root, document, sequence)
            for sequence in range(2, 6)
        }
        if events[5] is not None:
            recovery = recover_reup_completion(
                document,
                status_root,
                output_root=config.output_dir,
                media_root=config.dubvi_media_dir,
            )
            if reup_job_id is None and recovery == "succeeded":
                continue
            return _reup_report(
                document,
                outcome=recovery,
                worker_outcome="recovery_only",
                terminal=_optional_status_event(status_root, document, 5),
            )
        if events[4] is not None or (
            events[3] is not None and events[3].get("event_kind") == "output_published"
        ):
            recovery = recover_reup_completion(
                document,
                status_root,
                output_root=config.output_dir,
                media_root=config.dubvi_media_dir,
            )
            return _reup_report(
                document,
                outcome=recovery,
                worker_outcome="recovery_only",
                terminal=_optional_status_event(status_root, document, 5),
            )
        if events[3] is not None:
            if reup_job_id is not None:
                return _reup_report(
                    document,
                    outcome=(
                        "failed"
                        if events[3].get("event_kind") == "failed"
                        else "reconciliation_required"
                    ),
                    worker_outcome="terminal_existing",
                    terminal=events[3],
                )
            continue
        if events[2] is not None:
            if reup_job_id is not None:
                return _reup_report(
                    document,
                    outcome="reconciliation_required",
                    worker_outcome="started_without_terminal",
                    lease_id=events[2].get("lease_id"),
                )
            continue

        bridge = LeaseBridgeClient.from_environment()
        result = run_lease_aware_work(
            accepted.document,
            status_root,
            bridge,
            lambda _job: launch_processing_child(input_root, accepted.envelope_path, status_root),
        )
        terminal = _optional_status_event(status_root, document, 5)
        output_event = _optional_status_event(status_root, document, 3)
        if terminal is None and (
            output_event is not None and output_event.get("event_kind") == "output_published"
        ):
            recovery = recover_reup_completion(
                document,
                status_root,
                output_root=config.output_dir,
                media_root=config.dubvi_media_dir,
            )
            terminal = _optional_status_event(status_root, document, 5)
            return _reup_report(
                document,
                outcome=recovery,
                worker_outcome=result.outcome,
                lease_id=result.lease_id,
                terminal=terminal,
            )
        return _reup_report(
            document,
            outcome=_reup_worker_outcome(result.outcome, terminal),
            worker_outcome=result.outcome,
            lease_id=result.lease_id,
            terminal=terminal,
        )
    return {"outcome": "no_work"}


def _require_input_root() -> Path:
    root = Path(config.input_dir)
    if root.is_symlink() or not root.is_dir():
        raise ReupIntakeError("configured Reup input root must be an existing non-symlink directory")
    return root.resolve()


def _require_status_configuration() -> Path:
    if config.engine_status_dir is None:
        raise ReupStatusError("DUBVI_ENGINE_STATUS_DIR is required for canonical execution")
    root = Path(config.engine_status_dir)
    if root.is_symlink() or (root.exists() and not root.is_dir()):
        raise ReupStatusError("DUBVI_ENGINE_STATUS_DIR must be a real directory")
    return root


def _optional_status_event(
    status_root: Path,
    job: Mapping[str, object],
    sequence: int,
) -> dict[str, object] | None:
    path = status_root / "reup" / str(job["reup_job_id"]) / f"event-{sequence:06d}.json"
    if not path.exists() and not path.is_symlink():
        return None
    try:
        return load_status_event(status_root, job, sequence)
    except ReupStatusError as error:
        raise ReupIntakeError(f"existing Reup status is invalid: {error}") from error


def _canonical_uuid(value: str) -> str:
    try:
        parsed = uuid.UUID(value)
    except (ValueError, TypeError, AttributeError) as error:
        raise ReupIntakeError("reup job id is invalid") from error
    if value != str(parsed):
        raise ReupIntakeError("reup job id is invalid")
    return value


def _reup_worker_outcome(
    worker_outcome: str,
    terminal: Mapping[str, object] | None,
) -> str:
    if terminal is not None:
        if terminal.get("event_kind") == "succeeded":
            return "succeeded"
        if terminal.get("event_kind") == "failed":
            return "failed"
        if terminal.get("event_kind") == "lease_lost":
            return "reconciliation_required"
    if worker_outcome.startswith("work_completed"):
        return "reconciliation_required"
    if worker_outcome in {"lease_lost", "lease_unavailable", "accepted_status_unavailable"}:
        return "reconciliation_required"
    return worker_outcome


def _reup_report(
    job: Mapping[str, object],
    *,
    outcome: str,
    worker_outcome: str,
    lease_id: object | None = None,
    terminal: Mapping[str, object] | None = None,
) -> dict[str, object]:
    report: dict[str, object] = {
        "reup_job_id": job["reup_job_id"],
        "outcome": outcome,
        "worker_outcome": worker_outcome,
    }
    if isinstance(lease_id, str):
        report["lease_id"] = lease_id
    if terminal is not None:
        report["terminal_state"] = terminal.get("state")
        report["terminal_event"] = terminal.get("event_kind")
        if terminal.get("event_kind") == "succeeded":
            report["canonical_output_path"] = str(
                config.output_dir / "p1c" / str(job["reup_job_id"]) / "output.mp4"
            )
    return report


def _exit_code(report: Mapping[str, object]) -> int:
    if report.get("outcome") in {"no_work", "succeeded"}:
        return 0
    return 1


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Canonical Reup P1C runner")
    subparsers = parser.add_subparsers(dest="command", required=True)
    once = subparsers.add_parser("once", help="admit, run, or recover one canonical job")
    once.add_argument("--reup-job-id")
    args = parser.parse_args(argv)
    try:
        report = run_canonical_once(reup_job_id=args.reup_job_id)
    except Exception as error:
        report = {"outcome": "error", "diagnostic_summary": str(error)[:2048]}
        print(json.dumps(report, ensure_ascii=False, sort_keys=True))
        return 1
    print(json.dumps(report, ensure_ascii=False, sort_keys=True))
    return _exit_code(report)


if __name__ == "__main__":
    raise SystemExit(main())
