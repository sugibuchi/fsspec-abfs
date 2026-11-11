"""`AbfsFileSystems`: an async fsspec filesystem of any Azure storage account, by Hadoop's ABFS URLs.

It routes each URL to the `AbfsFileSystem` of its account, made when first
needed, with the library's `AsyncRoutingFileSystem`. A copy or move within an
account is that account's own. Between two accounts, each file is streamed
through this process, or copied on the server from the source's URL with a
user delegation SAS: see ``cross_account_copy``.
"""

from __future__ import annotations

import re
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING, Any, ClassVar, Literal

from azure.storage.blob import BlobSasPermissions, generate_blob_sas

from ._contrib.strategy.router import AsyncRoutingFileSystem
from ._errors import azure_errors
from ._paths import (
    _HADOOP_URL,
    DEFAULT_DOMAIN,
    check_account,
    parse_account_url,
    split_stripped,
)
from .abfs import AbfsFileSystem, close_on_loop
from .credentials import StorageCredential, token_credential_options

if TYPE_CHECKING:
    from azure.storage.blob import UserDelegationKey
    from fsspec.asyn import AsyncFileSystem

__all__ = ["AbfsFileSystems", "CrossAccountCopy", "get_user_delegation_key"]

CrossAccountCopy = Literal["streaming", "server-side"]
"""How a file is copied between two accounts: through this process, or on the server."""

_Account = tuple[str, str]
"""An account's name and domain, e.g. ("myaccount", "core.windows.net")."""

USER_DELEGATION_KEY_LIFETIME = timedelta(days=1)
"""How long a user delegation key asked for is valid. Azure allows 7 days at most."""

USER_DELEGATION_KEY_MARGIN = timedelta(hours=12)
"""How long a kept key must still be valid to sign a SAS: else a new one is asked for.

A copy between two accounts reads its source while it runs, so the source's SAS
must outlive it: each SAS is valid until its key expires, so at least as long as
this.
"""

_CLOCK_SKEW = timedelta(minutes=5)
"""How far back a key and a SAS start, as the service's clock may run ahead of this one."""


def _now() -> datetime:
    return datetime.now(UTC)


async def get_user_delegation_key(fs: AbfsFileSystem) -> UserDelegationKey:
    """The user delegation key of ``fs``'s account, kept on ``fs`` until it nears its expiry.

    A user delegation key signs SASs with a Microsoft Entra ID identity, the
    credential of ``fs``. Asking for one needs the action
    ``generateUserDelegationKey``, which Storage Blob Data Reader, Contributor
    and Owner, and Storage Blob Delegator, give at the account's level.

    The key is asked for once, valid for `USER_DELEGATION_KEY_LIFETIME`, and
    kept on ``fs``. A new one is asked for when the kept one has less than
    `USER_DELEGATION_KEY_MARGIN` left. Concurrent callers wait for one request.

    Parameters
    ----------
    fs : AbfsFileSystem
        The file system of the account, with a token credential.

    Returns
    -------
    azure.storage.blob.UserDelegationKey

    Raises
    ------
    ValueError
        If the credential of ``fs`` is no token credential: only one can ask.
    PermissionError
        If the identity may not ask for a key, e.g. with a role on a
        container only.
    """
    if fs.credential.kind != "TokenCredential":
        raise ValueError(
            "A user delegation key needs a Microsoft Entra ID token credential, not"
            f" {fs.credential.kind}"
        )

    async with fs._user_delegation_lock:
        key = fs._user_delegation_key
        if key is None or _expiry(key) - _now() < USER_DELEGATION_KEY_MARGIN:
            start = _now() - _CLOCK_SKEW
            with azure_errors(f"{fs.account_name}.blob.{fs.account_domain}"):
                key = await fs._blob_service().get_user_delegation_key(
                    key_start_time=start,
                    key_expiry_time=start + USER_DELEGATION_KEY_LIFETIME,
                )
            fs._user_delegation_key = key
        return key


def _expiry(key: UserDelegationKey) -> datetime:
    """A key's expiry: the SDK gives it as Azure's ISO 8601 text, in UTC."""
    return datetime.fromisoformat(str(key.signed_expiry))


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
          see `get_user_delegation_key`, the source's own SAS when it is one,
          or nothing when anonymous.
    **storage_options
        The default credential's options, and those of
        `fsspec.asyn.AsyncFileSystem`, e.g. ``asynchronous=True``.

    Raises
    ------
    ValueError
        If ``credential`` is no token credential, ``cross_account_copy`` none
        of its values, or an account of ``accounts`` given twice or not at
        all.

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

        # Checked first, as a SAS or a key would make a credential of another kind.
        options = token_credential_options(credential, storage_options)
        self.credential = StorageCredential.from_credential(credential, **options)
        # One made from a string is the router's to close, with its accounts.
        self._owns_credential = self.credential.value is not credential
        self._closed = False
        self.operation_concurrency = operation_concurrency
        self.io_concurrency = io_concurrency
        self.cross_account_copy: CrossAccountCopy = cross_account_copy
        self._accounts: dict[_Account, dict[str, Any]] = {}
        for options in accounts or []:
            account = _account_of(options)
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
            account, domain, container, key = _parse(str(path))
            return _url(account, domain, container, key)

    def unstrip_protocol(self, name: str) -> str:
        """A path's URL: the path itself, a Hadoop URL."""
        return self._strip_protocol(name)

    @staticmethod
    def _get_kwargs_from_urls(path: str) -> dict[str, Any]:
        # The URL names the account, which the router finds itself.
        return {}

    def _route(self, path: str) -> tuple[AsyncFileSystem, str]:
        account, domain, container, key = _parse(path)
        return self.filesystem(account, domain), f"/{container}/{key}".rstrip("/")

    def _outer(self, fs: AsyncFileSystem, path: str) -> str:
        container, key = split_stripped(path)
        return _url(fs.account_name, fs.account_domain, container, key)  # type: ignore[attr-defined]

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
            source: AbfsFileSystem = src_fs  # type: ignore[assignment]
            url = await self._source_url(source, src_entry["name"])
            await dest_fs._copy_from_url(url, src_entry["name"], dest_path)  # type: ignore[attr-defined]

    async def _source_url(self, fs: AbfsFileSystem, path: str) -> str:
        """A blob's URL, with what lets another account read it, by the credential of its account."""
        container, key = fs.split_path(path)
        blob = fs._blob(container, key)
        kind = fs.credential.kind
        if kind == "TokenCredential":
            delegation = await get_user_delegation_key(fs)
            sas = self._sas(fs, container, key, user_delegation_key=delegation)
            return f"{blob.url}?{sas}"
        elif kind == "SharedKey":
            sas = self._sas(fs, container, key, account_key=fs.credential.value)
            return f"{blob.url}?{sas}"
        else:
            # A SAS is in the client's URL already. Anonymous, the blob is public.
            return blob.url

    @staticmethod
    def _sas(fs: AbfsFileSystem, container: str, key: str, **signing: Any) -> str:
        """A SAS reading one blob, valid until its user delegation key expires, or as long as one lives."""
        delegation = signing.get("user_delegation_key")
        start = _now() - _CLOCK_SKEW
        if delegation is not None:
            expiry = _expiry(delegation)
        else:
            expiry = start + USER_DELEGATION_KEY_LIFETIME
        return generate_blob_sas(
            fs.account_name,
            container,
            key,
            permission=BlobSasPermissions(read=True),
            start=start,
            expiry=expiry,
            **signing,
        )


def _parse(path: str) -> tuple[str, str, str, str]:
    """A Hadoop URL's account, domain, container and path in it."""
    m = _HADOOP_URL.fullmatch(path.strip())
    if m is None or not re.match(r"abfss?://", path.strip(), re.IGNORECASE):
        raise ValueError(
            "AbfsFileSystems takes Hadoop's URLs only,"
            f" abfss://<container>@<account>.dfs.<domain>/<path>: {path!r}"
        )
    else:
        return (
            m.group("account").lower(),
            m.group("domain").lower(),
            m.group("container").lower(),
            (m.group("path") or "").strip("/"),
        )


def _url(account: str, domain: str, container: str, key: str) -> str:
    """A path's Hadoop URL, without a trailing "/"."""
    return f"abfss://{container}@{account}.dfs.{domain}" + (f"/{key}" if key else "")


def _account_of(options: dict[str, Any]) -> _Account:
    """The account an entry of ``accounts`` names, checked."""
    if options.get("account_url"):
        if options.get("account_name") or options.get("account_domain"):
            raise ValueError(
                "Give an account by account_url, or by account_name and account_domain:"
                f" not both: {options.get('account_url')!r}"
            )
        return parse_account_url(options["account_url"])
    elif options.get("account_name"):
        return check_account(
            options["account_name"], options.get("account_domain") or DEFAULT_DOMAIN
        )
    else:
        raise ValueError(
            "Each of accounts names its account: account_name, or account_url"
        )
