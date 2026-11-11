"""`AbfsFileSystems`: an async fsspec filesystem of any Azure storage account, by Hadoop's ABFS URLs.

It routes each URL to the `AbfsFileSystem` of its account, made when first
needed, with the library's `AsyncRoutingFileSystem`. A copy or move within an
account is that account's own. Between two accounts, each file is streamed
through this process, or copied on the server from the source's URL with a
user delegation SAS: see ``cross_account_copy``.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, ClassVar, Literal

from ._contrib.strategy.router import AsyncRoutingFileSystem
from ._paths import (
    DEFAULT_DOMAIN,
    check_account,
    hadoop_url,
    parse_hadoop_url,
    split_stripped,
)
from .abfs import AbfsFileSystem, close_on_loop, resolve_account
from .credentials import StorageCredential

if TYPE_CHECKING:
    from fsspec.asyn import AsyncFileSystem

__all__ = ["AbfsFileSystems", "CrossAccountCopy"]

CrossAccountCopy = Literal["streaming", "server-side"]
"""How a file is copied between two accounts: through this process, or on the server."""

_Account = tuple[str, str]
"""An account's name and domain, e.g. ("myaccount", "core.windows.net")."""


class AbfsFileSystems(AsyncRoutingFileSystem):
    """An async fsspec filesystem of any Azure storage account, by Hadoop's ABFS URLs.

    It takes only Hadoop's URLs, which name their account:
    ``abfss://<container>@<account>.dfs.<domain>/<path>``, or ``abfs://``. Its
    paths are those URLs, with their protocol: ``_strip_protocol`` keeps it,
    and the names that ``ls``, ``info`` and ``find`` answer are full URLs too,
    always ``abfss://``.

    Each account is an `AbfsFileSystem` of its own, made when first needed. An
    account in ``accounts`` is made with the arguments given there. Any
    other is made with the default ``credential`` and the concurrency given
    here. A copy or move within an account is that account's own, e.g. a
    rename. Between two accounts, it is the library's copy across storages,
    each file as ``cross_account_copy`` says.

    Parameters
    ----------
    credential : AsyncTokenCredential or str, default "default"
        The default credential: a Microsoft Entra ID token credential, or a
        keyword or a class name naming one, as `StorageCredential.from_credential`
        reads it. Any other kind, e.g. a SAS token or "anonymous", is refused:
        an account needing one is given its own in ``accounts``. One
        credential object serves every account without one of its own, so
        that its tokens are asked for once. A credential named by a string is
        made here, with the options its constructor names, e.g.
        ``tenant_id``, taken from ``storage_options``, and closed by `close`.
        A credential object given is the caller's to close.
    accounts : list of dict, optional
        The arguments of `AbfsFileSystem` for some accounts, one dict per
        account, each naming it by ``account_name`` and ``account_domain``, or
        by ``account_url``, e.g.
        ``[{"account_name": "myaccount", "is_hns_enabled": False},
        {"account_name": "opendata", "credential": "anonymous"}]``. A dict
        without ``credential`` takes the default one, and one without
        ``operation_concurrency`` or ``io_concurrency`` those given here.
    operation_concurrency : int, optional
        The strategies' requests at a time, of each account.
    io_concurrency : int, optional
        The range reads and block uploads at a time, of each account's files.
    transfer_concurrency : int, default 8
        The files copied between two accounts at a time.
    cross_account_copy : {"streaming", "server-side"}, default "streaming"
        How a file is copied between two accounts:

        - "streaming" reads it through this process and writes it, a block at
          a time. It needs only reading the source and writing the
          destination, with any credentials.
        - "server-side" has the destination's account copy it from the
          source's URL, with no data through this process. The URL carries a
          user delegation SAS when the source's credential is a token one,
          see `AbfsFileSystem._get_user_delegation_key`, the source's own SAS when it is one,
          or nothing when anonymous.
    **storage_options
        The default credential's options, and those of
        `fsspec.asyn.AsyncFileSystem`, e.g. ``asynchronous=True``.

    Raises
    ------
    ValueError
        If ``credential`` gives a credential of another kind than a token
        one, ``cross_account_copy`` is none of its values, or an account of
        ``accounts`` is given twice or not at all.
    TypeError
        If ``credential`` is neither a credential object nor a string, e.g.
        None, as `StorageCredential.from_credential` refuses it.

    Examples
    --------
    >>> fs = AbfsFileSystems(
    ...     accounts=[{"account_name": "opendata", "credential": "anonymous"}],
    ...     cross_account_copy="server-side",
    ... )
    >>> fs.ls("abfss://data@myaccount.dfs.core.windows.net/raw")
    >>> fs.copy(
    ...     "abfss://public@opendata.dfs.core.windows.net/set",
    ...     "abfss://data@myaccount.dfs.core.windows.net/raw/set",
    ...     recursive=True,
    ... )
    """

    protocol: ClassVar[tuple[str, ...]] = ("abfs", "abfss")
    root_marker = ""

    transfer_concurrency: int = 8
    """The default of the files copied between two accounts at a time."""

    def __init__(
        self,
        *,
        credential: Any = "default",
        accounts: list[dict[str, Any]] | None = None,
        operation_concurrency: int | None = None,
        io_concurrency: int | None = None,
        transfer_concurrency: int | None = None,
        cross_account_copy: CrossAccountCopy = "streaming",
        **storage_options: Any,
    ) -> None:
        if cross_account_copy not in ("streaming", "server-side"):
            raise ValueError(
                "cross_account_copy must be 'streaming' or 'server-side':"
                f" {cross_account_copy!r}"
            )

        self.credential = StorageCredential.from_credential(
            credential, **storage_options
        )
        if self.credential.kind != "TokenCredential":
            # A SAS, a key or anonymous access fits one account: none is the default.
            raise ValueError(
                "The default credential must be a Microsoft Entra ID token credential,"
                f" not {self.credential.kind}: give an account its own in accounts"
            )
        # One made from a string is the router's to close, with its accounts.
        self._owns_credential = self.credential.value is not credential
        self._closed = False
        self.operation_concurrency = operation_concurrency
        self.io_concurrency = io_concurrency
        self.cross_account_copy: CrossAccountCopy = cross_account_copy
        self._accounts: dict[_Account, dict[str, Any]] = {}
        for options in accounts or []:
            account = resolve_account(
                account_name=options.get("account_name"),
                account_domain=options.get("account_domain"),
                account_url=options.get("account_url"),
            )
            if account in self._accounts:
                raise ValueError(
                    f"The account {account[0]}.{account[1]} is given twice"
                )
            self._accounts[account] = dict(options)

        super().__init__(
            transfer_concurrency=transfer_concurrency or self.transfer_concurrency,
            # No listing is cached: other clients change the storage too. See
            # USAGE.md's "No directory cache".
            use_listings_cache=False,
            **storage_options,
        )

    # Paths.

    @classmethod
    def _strip_protocol(cls, path: Any) -> Any:
        """A Hadoop URL as the router's path: kept whole, as ``abfss://``, without a trailing "/".

        Raises ValueError for any other path, as no account would be known.
        """
        if isinstance(path, list):
            return [cls._strip_protocol(p) for p in path]
        else:
            account, domain, container, key = parse_hadoop_url(str(path))
            return hadoop_url(container, key, account, domain)

    def unstrip_protocol(self, name: str) -> str:
        """A path's URL: the path itself, a Hadoop URL."""
        return self._strip_protocol(name)

    def _route(self, path: str) -> tuple[AsyncFileSystem, str]:
        account, domain, container, key = parse_hadoop_url(path)
        return self.filesystem(account, domain), f"/{container}/{key}".rstrip("/")

    def _outer(self, fs: AsyncFileSystem, path: str) -> str:
        container, key = split_stripped(path)
        return hadoop_url(container, key, fs.account_name, fs.account_domain)  # type: ignore[attr-defined]

    async def _roots(self, path: str) -> list[dict[str, Any]]:
        # Every path routes: a URL names a container of an account.
        raise ValueError(f"No ABFS URL of a container: {path!r}")

    def filesystem(
        self, account_name: str, account_domain: str = DEFAULT_DOMAIN
    ) -> AbfsFileSystem:
        """The `AbfsFileSystem` of an account, made the first time, with its arguments.

        Its arguments are those of ``accounts`` for the account. The
        default credential and concurrency fill in those it lacks.
        A closed router makes none: it raises RuntimeError.
        """
        if self._closed:
            raise RuntimeError("The file system is already closed")
        account = check_account(account_name, account_domain)

        def make() -> AbfsFileSystem:
            options = dict(self._accounts.get(account, {}))
            options.pop("account_url", None)
            options.update(account_name=account[0], account_domain=account[1])
            if "credential" not in options:
                # The router's own object, shared: the account's file system
                # does not close it.
                options["credential"] = self.credential.value
            for name in ("operation_concurrency", "io_concurrency"):
                if options.get(name) is None and getattr(self, name) is not None:
                    options[name] = getattr(self, name)
            # On the router's loop, uncached: the router keeps and closes it.
            return AbfsFileSystem(
                **options, asynchronous=True, loop=self.loop, skip_instance_cache=True
            )

        return self._backend(account, make)  # type: ignore[return-value]

    def close(self) -> None:
        """Close the accounts' file systems, then the credential the router made.

        The credential is closed on the loop where the accounts' clients ran,
        which its sessions are bound to: unused, it holds nothing. A closed
        router refuses further calls, as `AbfsFileSystem` does.
        """
        loops = [
            fs._client_loop  # type: ignore[attr-defined]
            for fs in self._backends.values()
            if getattr(fs, "_client_loop", None) is not None
        ]
        self._closed = True
        try:
            super().close()
        finally:
            if self._owns_credential and loops:
                close_on_loop(loops[0], [self.credential.value])
                self._owns_credential = False

    # Copies between two accounts.

    async def _cross_fs_cp_entry(
        self,
        src_fs: AsyncFileSystem,
        src_entry: dict[str, Any],
        dest_fs: AsyncFileSystem,
        dest_path: str,
    ) -> None:
        """Copy a file between two accounts: streamed, or on the server, as ``cross_account_copy`` says."""
        if self.cross_account_copy == "streaming":
            await super()._cross_fs_cp_entry(src_fs, src_entry, dest_fs, dest_path)
        else:
            url = await src_fs._readable_url(src_entry["name"])  # type: ignore[attr-defined]
            await dest_fs._copy_from_url(url, src_entry["name"], dest_path)  # type: ignore[attr-defined]
