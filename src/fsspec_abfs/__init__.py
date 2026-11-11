"""An fsspec file system for Azure Data Lake Storage Gen2, on fsspec-alt-strategies.

Source, documentation and releases: https://github.com/sugibuchi/fsspec-abfs
"""

from __future__ import annotations

try:
    from ._version import __version__ as __version__
except ImportError:  # A source tree never built: no version file.
    __version__ = "0.0.0"


def use(cross_account: bool = False, adlfs_protocols: bool = False) -> None:
    """Register a file system of this package with fsspec, for the "abfs" and "abfss" protocols.

    Installing the package registers `AbfsFileSystem` already, by its entry
    points: this chooses again, e.g. after another library registered its own.

    Parameters
    ----------
    cross_account : bool, default False
        False registers `AbfsFileSystem`, of one account, whose URLs may also
        be paths in a container. True registers `AbfsFileSystems`, of any
        account, by Hadoop's URLs only, which copies between two accounts too.
    adlfs_protocols : bool, default False
        True registers it for adlfs' "az" too, so that every protocol adlfs
        registers is this package's. `AbfsFileSystems` takes ``az://`` URLs
        naming their account only, ``az://<container>@<account>.dfs.<domain>/<path>``.

    Examples
    --------
    >>> import fsspec
    >>> import fsspec_abfs
    >>> fsspec_abfs.use(cross_account=True)
    >>> fs, path = fsspec.url_to_fs("abfss://raw@myaccount.dfs.core.windows.net/data")
    """
    from fsspec import register_implementation  # noqa: PLC0415

    if cross_account:
        from .routing import AbfsFileSystems as cls  # noqa: PLC0415
    else:
        from .abfs import AbfsFileSystem as cls  # noqa: PLC0415
    protocols = ("abfs", "abfss", "az") if adlfs_protocols else ("abfs", "abfss")
    for protocol in protocols:
        register_implementation(protocol, cls, clobber=True)


def __getattr__(name: str) -> object:
    # Imported on first use, as the Azure SDK is slow to import.
    if name == "AbfsFileSystem":
        from .abfs import AbfsFileSystem  # noqa: PLC0415

        return AbfsFileSystem
    elif name == "AbfsFileSystems":
        from .routing import AbfsFileSystems  # noqa: PLC0415

        return AbfsFileSystems
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


__all__ = ["AbfsFileSystem", "AbfsFileSystems", "__version__", "use"]
