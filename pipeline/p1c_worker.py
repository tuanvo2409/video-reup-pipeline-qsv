"""Lease-aware CP4 execution shell; deliberately independent of processor.py."""

from __future__ import annotations

import time
from datetime import datetime, timezone
from dataclasses import dataclass
from typing import Callable, Mapping, Protocol

from pipeline.p1c_lease_client import LeaseBridgeResponse, LeaseBridgeUnavailable
from pipeline.p1c_status import (
    ReupStatusError,
    inspect_existing_accepted_status,
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
