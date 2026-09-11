"""Offline argv-only client for the Control Plane lease bridge."""

from __future__ import annotations

import json
import os
import subprocess
import sys
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Mapping


BRIDGE_TIMEOUT_SECONDS = 10
_MAX_STDOUT = 64 * 1024


class LeaseBridgeUnavailable(RuntimeError):
    pass


@dataclass(frozen=True)
class LeaseBridgeResponse:
    action: str
    granted: bool | None
    heartbeat_seconds: int
    ttl_seconds: int
    lease: Mapping[str, object] | None


class LeaseBridgeClient:
    def __init__(self, control_plane_root: Path, *, executable: str | None = None, timeout: int = BRIDGE_TIMEOUT_SECONDS) -> None:
        if control_plane_root.is_symlink() or not control_plane_root.is_dir() or not (control_plane_root / "src" / "dubvi_control_plane").is_dir():
            raise LeaseBridgeUnavailable("DUBVI_CONTROL_PLANE_ROOT is not a valid Control Plane root")
        self._root = control_plane_root.resolve()
        self._executable = executable or sys.executable
        self._timeout = timeout

    @classmethod
    def from_environment(cls, environment: Mapping[str, str] | None = None, *, executable: str | None = None) -> "LeaseBridgeClient":
        values = os.environ if environment is None else environment
        raw = values.get("DUBVI_CONTROL_PLANE_ROOT")
        if not raw:
            raise LeaseBridgeUnavailable("DUBVI_CONTROL_PLANE_ROOT is required")
        return cls(Path(raw), executable=executable)

    def acquire(self, job: Mapping[str, object]) -> LeaseBridgeResponse:
        return self._call("acquire", job)

    def heartbeat(self, job: Mapping[str, object], lease_id: str) -> LeaseBridgeResponse:
        return self._call("heartbeat", job, lease_id)

    def release(self, job: Mapping[str, object], lease_id: str) -> LeaseBridgeResponse:
        return self._call("release", job, lease_id)

    def _call(self, action: str, job: Mapping[str, object], lease_id: str | None = None) -> LeaseBridgeResponse:
        job_id = _uuid(job.get("reup_job_id"), "reup_job_id")
        correlation = _uuid(job.get("dispatch_id"), "dispatch_id")
        argv = [self._executable, "-m", "dubvi_control_plane.engine_lease_bridge", action, "--engine-kind", "reup", "--job-id", job_id, "--correlation-id", correlation]
        if lease_id is not None:
            argv.extend(["--lease-id", _uuid(lease_id, "lease_id")])
        environment = os.environ.copy()
        source = str(self._root / "src")
        environment["PYTHONPATH"] = source + (os.pathsep + environment["PYTHONPATH"] if environment.get("PYTHONPATH") else "")
        try:
            result = subprocess.run(argv, shell=False, capture_output=True, text=True, timeout=self._timeout, env=environment)
        except (OSError, subprocess.TimeoutExpired) as error:
            raise LeaseBridgeUnavailable("lease bridge is unavailable") from error
        if result.returncode != 0 or len(result.stdout.encode("utf-8")) > _MAX_STDOUT:
            raise LeaseBridgeUnavailable("lease bridge did not return a usable response")
        try:
            value = json.loads(result.stdout)
        except (TypeError, ValueError) as error:
            raise LeaseBridgeUnavailable("lease bridge returned malformed JSON") from error
        return _validate_response(value, action, job_id)


def _validate_response(value: object, action: str, job_id: str) -> LeaseBridgeResponse:
    if not isinstance(value, dict) or set(value) - {"bridge_version", "action", "ok", "engine_kind", "timing", "lease", "granted"}:
        raise LeaseBridgeUnavailable("lease bridge response shape is unsafe")
    if value.get("bridge_version") != 1 or value.get("action") != action or value.get("ok") is not True or value.get("engine_kind") != "reup":
        raise LeaseBridgeUnavailable("lease bridge response identity is unsafe")
    timing = value.get("timing")
    if not isinstance(timing, dict):
        raise LeaseBridgeUnavailable("lease bridge timing is invalid")
    heartbeat, ttl = timing.get("heartbeat_seconds"), timing.get("ttl_seconds")
    if isinstance(heartbeat, bool) or isinstance(ttl, bool) or not isinstance(heartbeat, int) or not isinstance(ttl, int) or heartbeat <= 0 or ttl < 4 * heartbeat:
        raise LeaseBridgeUnavailable("lease bridge timing is invalid")
    lease = value.get("lease")
    if action == "acquire" and not isinstance(value.get("granted"), bool):
        raise LeaseBridgeUnavailable("lease bridge acquire response is invalid")
    if lease is not None:
        if not isinstance(lease, dict) or lease.get("resource_class") != "HEAVY_MEDIA" or lease.get("owner") != f"reup:{job_id}" or lease.get("job_id") != job_id:
            raise LeaseBridgeUnavailable("lease bridge lease identity is invalid")
        _uuid(lease.get("lease_id"), "lease_id")
        if lease.get("state") not in ({"active"} if action != "release" else {"released"}):
            raise LeaseBridgeUnavailable("lease bridge lease state is invalid")
        for key in ("acquired_at", "heartbeat_at", "expires_at"):
            if not isinstance(lease.get(key), str) or not lease[key].endswith("Z"):
                raise LeaseBridgeUnavailable("lease bridge timestamp is invalid")
    if action == "acquire" and value["granted"] != (lease is not None):
        raise LeaseBridgeUnavailable("lease bridge acquire decision is invalid")
    return LeaseBridgeResponse(action, value.get("granted"), heartbeat, ttl, lease)


def _uuid(value: object, field: str) -> str:
    if not isinstance(value, str):
        raise LeaseBridgeUnavailable(f"{field} is invalid")
    try:
        if value != str(uuid.UUID(value)):
            raise ValueError
    except (ValueError, TypeError, AttributeError) as error:
        raise LeaseBridgeUnavailable(f"{field} is invalid") from error
    return value
