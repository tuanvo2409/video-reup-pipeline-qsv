"""CP5 processing adapter and crash-safe Reup completion recovery."""

from __future__ import annotations

import os
import shutil
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Mapping

from pipeline.config import config
from pipeline.p1c_handoff import HandoffV2Error, PublishedHandoffV2, publish_reup_handoff
from pipeline.p1c_intake import AcceptedReupJob, load_reup_envelope
from pipeline.p1c_output import OutputPublicationError, PublishedReupOutput, cleanup_published_output_stage, publish_reup_output
from pipeline.p1c_status import (
    ReupStatusError, inspect_existing_accepted_status, publish_failed_status, publish_handoff_published_status,
    publish_output_published_status, publish_succeeded_status,
)
from pipeline.dubvi_engine_contract import ContractValidationError, canonical_json_bytes, parse_json_document, validate_engine_status_event


@dataclass(frozen=True)
class ProcessingResult:
    outcome: str
    output: PublishedReupOutput | None = None
    handoff: PublishedHandoffV2 | None = None


def process_reup_attempt(accepted: AcceptedReupJob, status_root: Path, *, processor: Callable[..., object], output_root: Path | None = None, media_root: Path | None = None, now_utc: str | None = None) -> ProcessingResult:
    """Run legacy processor only against an attempt-owned true copy."""
    job = accepted.document
    source = accepted.media_path
    scratch_dir = config.processing_dir / "p1c-source" / str(job["reup_job_id"])
    scratch = scratch_dir / f"source-{job['reup_job_id']}{source.suffix.lower()}"
    try:
        _verify_source(source, job)
        scratch_dir.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(source, scratch)
    except Exception as error:
        _failed(status_root, job, "PROCESSING_FAILED", str(error), now_utc)
        return ProcessingResult("failed")
    try:
        try:
            result = processor(scratch, override_profile=job["reup_profile"], target_platform=job["target_platform"], send_to_dubvi=False, force_reprocess=True)
        except Exception as error:
            _failed(status_root, job, "PROCESSING_FAILED", str(error), now_utc)
            return ProcessingResult("failed")
        if not getattr(result, "success", False):
            _failed(status_root, job, "PROCESSING_FAILED", getattr(result, "error_message", "processor returned failure"), now_utc)
            return ProcessingResult("failed")
        candidate = getattr(result, "output_file", None)
        try:
            output = publish_reup_output(Path(candidate), job, output_root or config.output_dir)
        except (OutputPublicationError, TypeError, ValueError) as error:
            _failed(status_root, job, "OUTPUT_VERIFY_FAILED" if "FINAL_PATH_CONFLICT" not in str(error) else "FINAL_PATH_CONFLICT", str(error), now_utc)
            return ProcessingResult("failed")
        try:
            publish_output_published_status(status_root, job, output.fingerprint, occurred_at_utc=now_utc)
        except ReupStatusError:
            return ProcessingResult("output_published_pending", output)
        try:
            handoff = publish_reup_handoff(output.final_path, job, media_root or config.dubvi_media_dir, output_fingerprint=output.fingerprint, created_at_utc=now_utc)
            publish_handoff_published_status(status_root, job, output.fingerprint, handoff.handoff_id, handoff.handoff_ref, occurred_at_utc=now_utc)
            publish_succeeded_status(status_root, job, output.fingerprint, handoff.handoff_id, handoff.handoff_ref, occurred_at_utc=now_utc)
        except HandoffV2Error:
            return ProcessingResult("handoff_pending", output)
        cleanup_published_output_stage(output)
        return ProcessingResult("succeeded", output, handoff)
    finally:
        scratch.unlink(missing_ok=True)


def recover_reup_completion(job: Mapping[str, object], status_root: Path, *, output_root: Path | None = None, media_root: Path | None = None) -> str:
    """Complete only durable output/handoff evidence; never rerender."""
    # Recovery deliberately observes status files and uses the canonical paths.
    event_dir = status_root / "reup" / str(job["reup_job_id"])
    final_event = _load_recovery_event(event_dir / "event-000005.json", job, 5)
    if final_event is not None:
        _cleanup_output_stage_for(job, final_event.get("output_fingerprint"), output_root or config.output_dir)
        return "succeeded"
    event = _load_recovery_event(event_dir / "event-000003.json", job, 3)
    if event is not None:
        if event.get("event_kind") == "failed": return "failed"
        if event.get("event_kind") == "lease_lost": return "reconciliation_required"
        if event.get("event_kind") != "output_published" or not isinstance(event.get("output_fingerprint"), str):
            return "reconciliation_required"
    handoff_event = _load_recovery_event(event_dir / "event-000004.json", job, 4)
    if handoff_event is not None:
        try:
            output = Path(output_root or config.output_dir) / "p1c" / str(job["reup_job_id"]) / "output.mp4"
            handoff = publish_reup_handoff(
                output, job, media_root or config.dubvi_media_dir,
                output_fingerprint=str(handoff_event["output_fingerprint"]),
            )
            if handoff.handoff_id != handoff_event.get("handoff_id") or handoff.handoff_ref != handoff_event.get("handoff_ref"):
                return "handoff_pending"
            publish_succeeded_status(
                status_root, job, str(handoff_event["output_fingerprint"]),
                str(handoff_event["handoff_id"]), str(handoff_event["handoff_ref"]),
            )
            _cleanup_output_stage_for(job, handoff_event.get("output_fingerprint"), output_root or config.output_dir)
            return "succeeded"
        except (HandoffV2Error, ReupStatusError):
            return "handoff_pending"
    if event is not None:
        output = Path(output_root or config.output_dir) / "p1c" / str(job["reup_job_id"]) / "output.mp4"
        try:
            handoff = publish_reup_handoff(output, job, media_root or config.dubvi_media_dir, output_fingerprint=event["output_fingerprint"])
            publish_handoff_published_status(status_root, job, event["output_fingerprint"], handoff.handoff_id, handoff.handoff_ref)
            publish_succeeded_status(status_root, job, event["output_fingerprint"], handoff.handoff_id, handoff.handoff_ref)
            _cleanup_output_stage_for(job, event.get("output_fingerprint"), output_root or config.output_dir)
            return "succeeded"
        except (HandoffV2Error, ReupStatusError): return "handoff_pending"
    return "reconciliation_required"


def _cleanup_output_stage_for(job: Mapping[str, object], fingerprint: object, output_root: Path) -> None:
    if not isinstance(fingerprint, str) or not fingerprint.startswith("sha256:"):
        return
    stage = Path(output_root) / "p1c" / str(job["reup_job_id"]) / f".output.{fingerprint[7:]}.part"
    if stage.is_symlink() or not stage.is_file():
        return
    try:
        stage.unlink()
    except OSError:
        pass


def _load_recovery_event(path: Path, job: Mapping[str, object], sequence: int) -> dict[str, object] | None:
    if not path.exists() and not path.is_symlink():
        return None
    if path.is_symlink() or not path.is_file() or path.stat().st_size <= 0 or path.stat().st_size > 64 * 1024:
        raise ReupStatusError("recovery status is not a bounded regular file")
    payload = path.read_bytes()
    try:
        document = validate_engine_status_event(parse_json_document(payload))
    except (ContractValidationError, ValueError) as error:
        raise ReupStatusError("recovery status is not canonical engine evidence") from error
    if payload != canonical_json_bytes(document):
        raise ReupStatusError("recovery status is not canonical bytes")
    expected_pairs = {
        3: {("output_published", "running"), ("failed", "failed"), ("lease_lost", "running")},
        4: {("handoff_published", "running")},
        5: {("succeeded", "succeeded")},
    }
    expected = {
        "engine_kind": "reup", "engine_job_id": job["reup_job_id"],
        "dispatch_id": job["dispatch_id"], "correlation_id": job["dispatch_id"],
        "attempt_number": job["attempt_number"], "sequence": sequence,
    }
    if any(document.get(field) != value for field, value in expected.items()) or (document["event_kind"], document["state"]) not in expected_pairs[sequence]:
        raise ReupStatusError("recovery status conflicts with canonical job identity")
    return document


def _verify_source(source: Path, job: Mapping[str, object]) -> None:
    from pipeline.p1c_output import _hash_file
    if source.is_symlink() or not source.is_file() or source.stat().st_size != job["media_size"] or _hash_file(source)[0] != job["source_fingerprint"]:
        raise ValueError("stable Reup source failed verification")


def _failed(status_root, job, classification: str, summary: object, now_utc: str | None) -> None:
    try: publish_failed_status(status_root, job, classification, str(summary)[:2048], occurred_at_utc=now_utc)
    except ReupStatusError: pass


class OwnedProcessingHandle:
    def __init__(self, process: subprocess.Popen[bytes]) -> None: self.process = process
    def poll(self): return self.process.poll()
    def wait(self, timeout: float | None = None): return self.process.wait(timeout=timeout)
    def terminate(self) -> None:
        if self.process.poll() is not None: return
        if os.name == "nt":
            subprocess.run(["taskkill", "/PID", str(self.process.pid), "/T", "/F"], shell=False, check=False, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        else: self.process.terminate()


def launch_processing_child(input_root: Path, envelope_path: Path, status_root: Path, *, executable: str | None = None) -> OwnedProcessingHandle:
    argv = [executable or sys.executable, "-m", "pipeline.p1c_processing", str(input_root), str(envelope_path), str(status_root)]
    environment = os.environ.copy(); result = subprocess.Popen(argv, shell=False, env=environment)
    return OwnedProcessingHandle(result)


def main(argv: list[str] | None = None) -> int:
    import argparse
    parser = argparse.ArgumentParser(); parser.add_argument("input_root"); parser.add_argument("envelope_path"); parser.add_argument("status_root")
    args = parser.parse_args(argv)
    try:
        document = load_reup_envelope(Path(args.envelope_path), Path(args.input_root))
        status_root = Path(args.status_root)
        _require_started_status(status_root, document)
        accepted = AcceptedReupJob(Path(args.envelope_path), Path(args.envelope_path).parent / str(document["media_name"]), Path(args.status_root) / "reup" / str(document["reup_job_id"]) / "event-000001.json", document)
        from pipeline.processor import process_single_video
        result = process_reup_attempt(accepted, status_root, processor=process_single_video)
        return 0 if result.outcome == "succeeded" else 1
    except Exception:
        return 2


def _require_started_status(status_root: Path, job: Mapping[str, object]) -> None:
    """Require immutable accepted and started evidence before touching a processor."""

    inspect_existing_accepted_status(status_root, job)
    path = status_root / "reup" / str(job["reup_job_id"]) / "event-000002.json"
    if path.is_symlink() or not path.is_file() or path.stat().st_size <= 0:
        raise ReupStatusError("started status is required before processing")
    payload = path.read_bytes()
    document = validate_engine_status_event(parse_json_document(payload))
    if payload != canonical_json_bytes(document):
        raise ReupStatusError("started status is not canonical bytes")
    expected = {
        "engine_kind": "reup",
        "engine_job_id": job["reup_job_id"],
        "dispatch_id": job["dispatch_id"],
        "correlation_id": job["dispatch_id"],
        "attempt_number": job["attempt_number"],
        "event_kind": "started",
        "state": "running",
        "sequence": 2,
    }
    if any(document.get(field) != value for field, value in expected.items()):
        raise ReupStatusError("started status conflicts with canonical job identity")
    if not isinstance(document.get("lease_id"), str) or not document["lease_id"]:
        raise ReupStatusError("started status requires a lease identity")


if __name__ == "__main__": raise SystemExit(main())
