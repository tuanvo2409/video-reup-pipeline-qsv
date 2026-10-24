"""Side-effect-free input namespace rules shared by legacy and P1C intake."""

from __future__ import annotations

from pathlib import Path


def is_legacy_media_input(path: Path, supported_extensions: tuple[str, ...]) -> bool:
    """Return whether a file belongs to the legacy watcher namespace.

    Canonical Control Plane media uses the reserved ``dubvi-`` prefix and is
    deliberately excluded from the legacy processor until a later checkpoint
    binds the canonical worker to transformation.
    """

    return (
        path.is_file()
        and not path.name.startswith("dubvi-")
        and path.suffix.lower() in supported_extensions
    )
