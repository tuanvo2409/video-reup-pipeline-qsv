"""Atomic filesystem handoff from the reup pipeline to the DUBVI worker."""
from __future__ import annotations

from dataclasses import dataclass
import json
from pathlib import Path
import shutil
import uuid
from typing import Any, Mapping


HANDOFF_SCHEMA_VERSION = 1


class HandoffPublicationError(RuntimeError):
    """Raised when a DUBVI bridge pair could not be published safely."""


@dataclass(frozen=True)
class HandoffPublication:
    video_path: Path
    metadata_path: Path


def publish_dubvi_handoff(
    source_video: Path,
    metadata: Mapping[str, Any],
    destination_dir: Path,
) -> HandoffPublication:
    """Publish a video and a complete sidecar using the sidecar as readiness marker.

    The source production output is never moved or deleted. The final sidecar
    is published last, so a translator only sees a versioned complete handoff
    after both staged artifacts have been validated.
    """
    if not source_video.is_file() or source_video.stat().st_size <= 0:
        raise HandoffPublicationError(f"Bridge source is missing or empty: {source_video.name}")

    destination_dir.mkdir(parents=True, exist_ok=True)
    final_video = destination_dir / source_video.name
    final_metadata = destination_dir / f"{source_video.stem}.meta.json"
    if final_video.exists() or final_metadata.exists():
        raise HandoffPublicationError(
            f"Refusing to overwrite an existing DUBVI bridge artifact: {final_video.name}"
        )

    handoff_metadata = dict(metadata)
    handoff_metadata.update({
        "handoff_schema_version": HANDOFF_SCHEMA_VERSION,
        "handoff_status": "complete",
    })

    nonce = uuid.uuid4().hex
    staged_video = destination_dir / f".{final_video.name}.{nonce}.part"
    staged_metadata = destination_dir / f".{final_metadata.name}.{nonce}.part"
    published_video = False

    try:
        shutil.copy2(source_video, staged_video)
        if not staged_video.is_file() or staged_video.stat().st_size != source_video.stat().st_size:
            raise HandoffPublicationError(f"Bridge video validation failed: {source_video.name}")

        staged_metadata.write_text(
            json.dumps(handoff_metadata, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        parsed_metadata = json.loads(staged_metadata.read_text(encoding="utf-8"))
        if parsed_metadata.get("handoff_schema_version") != HANDOFF_SCHEMA_VERSION:
            raise HandoffPublicationError("Bridge metadata schema validation failed")
        if parsed_metadata.get("handoff_status") != "complete":
            raise HandoffPublicationError("Bridge metadata readiness validation failed")

        staged_video.replace(final_video)
        published_video = True
        # The completed sidecar is the readiness marker and is deliberately last.
        staged_metadata.replace(final_metadata)
        return HandoffPublication(video_path=final_video, metadata_path=final_metadata)
    except Exception as exc:
        staged_video.unlink(missing_ok=True)
        staged_metadata.unlink(missing_ok=True)
        if published_video:
            # This video was created by this publication attempt; do not leave a
            # sidecar-less partial bridge artifact for the translator to discover.
            final_video.unlink(missing_ok=True)
        if isinstance(exc, HandoffPublicationError):
            raise
        raise HandoffPublicationError(f"DUBVI bridge publication failed: {exc}") from exc
