"""An fsspec file system for Azure Data Lake Storage Gen2, on fsspec-alt-strategies.

Source, documentation and releases: https://github.com/sugibuchi/fsspec-abfs
"""

from __future__ import annotations

try:
    from ._version import __version__ as __version__
except ImportError:  # A source tree never built: no version file.
    __version__ = "0.0.0"
