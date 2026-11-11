"""An fsspec file system for Azure Data Lake Storage Gen2, on fsspec-alt-strategies.

Source, documentation and releases: https://github.com/sugibuchi/fsspec-abfs
"""

from __future__ import annotations

try:
    from ._version import __version__ as __version__
except ImportError:  # A source tree never built: no version file.
    __version__ = "0.0.0"


def __getattr__(name: str) -> object:
    # Imported on first use, as the Azure SDK is slow to import.
    if name == "AbfsFileSystem":
        from .abfs import AbfsFileSystem  # noqa: PLC0415

        return AbfsFileSystem
    elif name == "AbfsFileSystems":
        from .routing import AbfsFileSystems  # noqa: PLC0415

        return AbfsFileSystems
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


__all__ = ["AbfsFileSystem", "AbfsFileSystems", "__version__"]
