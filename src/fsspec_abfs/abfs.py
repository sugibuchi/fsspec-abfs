"""`AbfsFileSystem`: an async fsspec filesystem of an Azure storage account.

It walks folder trees with the tree strategies of fsspec-alt-strategies,
copied into ``_contrib.strategy``, and implements their primitives with
Azure's two APIs of one account:

- the blob API, for what both kinds of account answer alike: a path's
  properties, deletions, server-side copies, and the files' data;
- the Data Lake API, for what only an account with a hierarchical namespace
  (HNS) has: real folders, listings of one folder, atomic renames and
  recursive deletes.

Whether the account has a hierarchical namespace is declared to the
constructor, or else asked once. While it is unknown, the filesystem takes the
way right on both kinds: the blob API, and moves by copy and delete.
"""

from __future__ import annotations

import asyncio
import base64
import contextlib
import errno
import logging
import sys
import uuid
from datetime import UTC, datetime, timedelta
from io import UnsupportedOperation
from typing import TYPE_CHECKING, Any, ClassVar

from azure.core import MatchConditions
from azure.core.exceptions import (
    AzureError,
    HttpResponseError,
    ResourceExistsError,
    ResourceModifiedError,
    ResourceNotFoundError,
)
from azure.storage.blob import BlobBlock, BlobSasPermissions, generate_blob_sas
from azure.storage.blob.aio import BlobServiceClient
from azure.storage.filedatalake.aio import DataLakeServiceClient
from fsspec.asyn import sync, sync_wrapper
from fsspec_alt_strategies.profile import StorageProfile

from ._contrib.strategy.files.adaptive import (
    AdaptiveReadableFile,
    AdaptiveWritableFile,
)
from ._contrib.strategy.files.fsspec_async import AsyncFileAdapter
from ._contrib.strategy.files.fsspec_transaction import (
    AdaptiveWritableTransaction,
    TransactionFile,
)
from ._contrib.strategy.tree.filesystem import TreeFileSystem
from ._errors import azure_errors, describe, os_error
from ._listing import bare_etag, list_blobs_flat
from ._paths import (
    DEFAULT_DOMAIN,
    AccountDomain,
    account_of,
    check_account,
    hadoop_url,
    parse_account_url,
    split_stripped,
    strip_protocol,
)
from .credentials import StorageCredential

if TYPE_CHECKING:
    from collections.abc import AsyncIterator

    from azure.storage.blob import UserDelegationKey
    from azure.storage.blob.aio import BlobClient


__all__ = [
    "HNS_PROFILE",
    "NO_HNS_PROFILE",
    "UNKNOWN_HNS_PROFILE",
    "AbfsFileSystem",
    "AbfsReadableFile",
    "AbfsWritableFile",
    "resolve_account",
]

logger = logging.getLogger(__name__)

_MiB = 1024 * 1024

HNS_PROFILE = StorageProfile(
    fs_type="tree",
    parents_required=False,
    implicit_directories=False,
    renames_files=True,
    renames_directories=True,
    deletes_directories=True,
)
"""An account with a hierarchical namespace.

- Its folders are paths of their own: an empty one exists, and is deleted by
  a request of its own.
- A blob written creates its missing parent folders, as the blob API does on
  such an account. A rename does not: the strategies create them first.
- A rename moves a file, or a folder with all in it, in one atomic request,
  within a container or between two: see `AbfsFileSystem._mv_file`.
- A folder is deleted with all in it in one request, by the Data Lake API.
"""

NO_HNS_PROFILE = StorageProfile(
    fs_type="tree",
    parents_required=False,
    implicit_directories=True,
    renames_files=False,
    renames_directories=False,
    deletes_directories=False,
)
"""An account without a hierarchical namespace: blobs whose folders are prefixes.

It is still listed a folder per request, by the blob API's delimiter, and a
whole subtree by one listing without it, so the tree strategies suit it. A
folder exists only through the blobs under it, and there is no rename: a move
copies on the server, then deletes.
"""

UNKNOWN_HNS_PROFILE = StorageProfile(fs_type="tree", parents_required=False)
"""An account not known to have a hierarchical namespace, or not to.

A blob written creates its parents on both kinds of account. The rest is left
unknown, which is right on both: an emptied folder is deleted, as it may be a
path of its own, and a move copies and deletes.
"""

_PROFILES = {True: HNS_PROFILE, False: NO_HNS_PROFILE, None: UNKNOWN_HNS_PROFILE}

# The error codes of a Data Lake request on an account without a hierarchical
# namespace: 400, or 409 where the account has features the dfs endpoint
# refuses, e.g. soft delete.
_NO_HNS_CODES = frozenset(
    {"HierarchicalNamespaceNotEnabled", "EndpointUnsupportedAccountFeatures"}
)

_root_info = {"name": "/", "size": 0, "type": "directory"}

_CLOCK_SKEW = timedelta(minutes=5)
"""How far back a user delegation key and a SAS start, as the service's clock may run ahead of this one."""


def _key_expiry(key: UserDelegationKey) -> datetime:
    """A user delegation key's expiry: the SDK gives it as Azure's ISO 8601 text, in UTC."""
    return datetime.fromisoformat(str(key.signed_expiry))


def _is_folder_blob(metadata: dict[str, str] | None) -> bool:
    """Whether a blob stands for a folder: a folder of an HNS account, or a marker blob.

    Both carry the metadata ``hdi_isfolder=true``, the Hadoop drivers' marker.
    """
    return (metadata or {}).get("hdi_isfolder", "").lower() == "true"


def _blob_entry(container: str, props: Any) -> dict[str, Any]:
    """The fsspec entry of a blob's properties: a file, or a folder by its metadata."""
    name = f"/{container}/{props.name.rstrip('/')}"
    if _is_folder_blob(props.metadata):
        return {
            "name": name,
            "size": 0,
            "type": "directory",
            "last_modified": props.last_modified,
            "etag": bare_etag(props.etag),
        }
    else:
        return {
            "name": name,
            "size": props.size,
            "type": "file",
            "last_modified": props.last_modified,
            "etag": bare_etag(props.etag),
            "content_type": getattr(props.content_settings, "content_type", None),
        }


def _folder_entry(name: str) -> dict[str, Any]:
    return {"name": name.rstrip("/"), "size": 0, "type": "directory"}


def close_on_loop(
    loop: asyncio.AbstractEventLoop | None, closeables: list[Any]
) -> None:
    """Close async clients or credentials, each by its ``close()``, on the loop their sessions are bound to.

    On that loop itself, nothing can wait for it: the closing is a task. From
    any other thread, it is waited for, 5 s at most. A loop closed, or not
    running, or an interpreter finishing, closes nothing: their sessions go
    with it. Failures are ignored, as nothing is left to do with them.
    """
    if not closeables or loop is None or loop.is_closed():
        return

    try:
        running = asyncio.get_running_loop()
    except RuntimeError:
        running = None

    async def close_all() -> None:
        for closeable in closeables:
            with contextlib.suppress(Exception):
                await closeable.close()

    if running is loop:
        loop.create_task(close_all())
    elif loop.is_running() and not sys.is_finalizing():
        sync(loop, close_all, timeout=5.0)


class _AccountCached(type(TreeFileSystem)):  # type: ignore[misc]
    """The metaclass of `AbfsFileSystem`: its instances are cached by account.

    fsspec caches a filesystem by the arguments it is made with. The account
    may come as ``account_url``, or as ``account_name`` and an optional
    ``account_domain``: each is made ``account_name`` and ``account_domain``
    here, before fsspec computes the key, so that one account gives one key
    however it is named. ``account_url`` is then left out of the key, by
    `AbfsFileSystem._strip_tokenize_options`.
    """

    def __call__(cls, *args: Any, **kwargs: Any) -> Any:
        if args:
            # Refused before the account is checked, which would say it is missing.
            raise TypeError(f"{cls.__name__} takes keyword arguments only")

        kwargs["account_name"], kwargs["account_domain"] = resolve_account(
            account_name=kwargs.get("account_name"),
            account_domain=kwargs.get("account_domain"),
            account_url=kwargs.get("account_url"),
        )
        return super().__call__(**kwargs)


def resolve_account(
    *,
    account_name: str | None = None,
    account_domain: str | None = None,
    account_url: str | None = None,
) -> tuple[str, AccountDomain]:
    """The name and domain of the account given as `AbfsFileSystem` takes it, checked.

    The account is ``account_name`` with an optional ``account_domain``, Azure's
    by default, or else ``account_url``, an endpoint's URL: not both.

    Parameters
    ----------
    account_name : str, optional
        The account's name, e.g. ``myaccount``.
    account_domain : {"core.windows.net", "fabric.microsoft.com"}, optional
        The account's domain.
    account_url : str, optional
        The account's dfs or blob endpoint, e.g.
        ``https://myaccount.dfs.core.windows.net/``.

    Returns
    -------
    tuple of str
        The name, in lower case, and the domain.

    Raises
    ------
    ValueError
        If no account is given, or it is given both ways, or the name, the
        domain or the URL is none Azure takes.

    Examples
    --------
    >>> resolve_account(account_url="https://MyAccount.blob.core.windows.net")
    ('myaccount', 'core.windows.net')
    """
    if account_url is not None:
        if account_name or account_domain:
            raise ValueError(
                "Give the account by account_url, or by account_name and "
                "account_domain: not both"
            )
        else:
            return parse_account_url(account_url)
    elif account_name:
        return check_account(account_name, account_domain or DEFAULT_DOMAIN)
    else:
        raise ValueError("The account is required: account_name, or account_url")


class AbfsFileSystem(TreeFileSystem, metaclass=_AccountCached):
    """An async fsspec filesystem of an Azure storage account, e.g. ADLS Gen2.

    Its paths are ``/<container>/<path>``, and its root, "/", lists the
    containers. It makes no container: a container is made by Azure's own
    means. It takes the URLs adlfs takes:

    - Hadoop's, ``abfss://<container>@<account>.dfs.core.windows.net/<path>``,
      with or without its protocol;
    - adlfs' own, ``abfs://<container>/<path>`` or ``az://<container>/<path>``;
    - the endpoints', ``https://<account>.dfs.core.windows.net/<container>/<path>``.

    Its operations refuse a URL of another account: see `split_path`.
    `unstrip_protocol` gives a path's URL in Hadoop's form.

    The recursive methods are the tree strategies of fsspec-alt-strategies.
    Their storage profile follows the account: see `HNS_PROFILE`,
    `NO_HNS_PROFILE` and `UNKNOWN_HNS_PROFILE`.

    The account is ``account_name`` with ``account_domain``, or else
    ``account_url``, not both. fsspec caches one filesystem per account,
    credential and options, however the account is given.

    Parameters
    ----------
    account_name : str, optional
        The account's name, e.g. ``myaccount``.
    credential : AsyncTokenCredential or str, default "default"
        An async token credential, or a string naming one, a SAS token or a
        shared key, as `StorageCredential.from_credential` reads it. The
        default is ``DefaultAzureCredential``, and "anonymous" sends none. A
        token credential given is the caller's to close. One made from a
        string is the file system's, closed by `close`.
    account_domain : {"core.windows.net", "fabric.microsoft.com"}, optional
        The account's domain: Azure's, the default, or Fabric OneLake's.
    account_url : str, optional
        The account's dfs or blob endpoint, in place of ``account_name`` and
        ``account_domain``, e.g. ``https://<account>.dfs.core.windows.net/``,
        or Fabric OneLake's ``https://onelake.dfs.fabric.microsoft.com/``.
    is_hns_enabled : bool, optional
        Whether the account has a hierarchical namespace, as Azure's account
        information names it. None asks the account on first need: see
        `is_hns_enabled`.
    operation_concurrency : int or asyncio.Semaphore, optional
        The strategies' requests at a time, e.g. listings and deletions: 100
        by default.
    io_concurrency : int or asyncio.Semaphore, optional
        The range reads and block uploads at a time, of all files opened: 16
        by default.
    **storage_options
        Passed to `fsspec.asyn.AsyncFileSystem`, e.g. ``asynchronous=True``. A
        token credential a string names takes its own from them, e.g.
        ``tenant_id``: see `StorageCredential.from_credential`.

    Raises
    ------
    ValueError
        If no account is given, or it is given both ways, or ``account_url`` is
        no account endpoint, or ``account_name`` or ``account_domain`` is none
        Azure takes. Also if ``credential`` is a string of none of the forms
        `StorageCredential.from_credential` reads.
    TypeError
        If ``credential`` is neither an async token credential nor a string,
        e.g. None.

    Examples
    --------
    >>> from azure.identity.aio import DefaultAzureCredential
    >>> fs = AbfsFileSystem(
    ...     account_name="myaccount",
    ...     credential=DefaultAzureCredential(),
    ...     is_hns_enabled=True,
    ... )
    >>> fs.ls("/mycontainer/data")
    """

    protocol: ClassVar[tuple[str, ...]] = ("abfs", "abfss")
    root_marker = "/"

    # The account is in the key as account_name and account_domain, which the
    # metaclass makes of an account_url.
    _strip_tokenize_options: ClassVar[tuple[str, ...]] = ("account_url",)

    operation_concurrency: int = 100
    """The default of the strategies' requests at a time.

    Most are small, e.g. a deletion or a server-side copy, so their round trips
    weigh more than the account's throughput: adlfs sends about as many.
    """

    io_concurrency: int = 16
    """The default of the files' range reads and block uploads at a time."""

    copy_poll_interval: float = 0.2
    """The first wait, in seconds, before checking a pending server-side copy. It doubles up to 5 s."""

    _user_delegation_key_lifetime: timedelta = timedelta(days=1)
    """How long a user delegation key asked for is valid. Azure allows 7 days at most."""

    _user_delegation_key_margin: timedelta = timedelta(hours=12)
    """How long a kept user delegation key must still be valid: else a new one is asked for.

    A copy from another account reads its source while it runs, so the
    source's SAS must outlive it. `AbfsFileSystems` signs that SAS for this
    long, which a key kept always covers.
    """

    def __init__(
        self,
        *,
        account_name: str | None = None,
        credential: Any = "default",
        account_domain: AccountDomain | None = None,
        account_url: str | None = None,
        is_hns_enabled: bool | None = None,
        operation_concurrency: int | asyncio.Semaphore | None = None,
        io_concurrency: int | asyncio.Semaphore | None = None,
        **storage_options: Any,
    ) -> None:
        # The metaclass made account_name and account_domain, of account_url too.
        self.account_name: str = account_name  # type: ignore[assignment]
        self.account_domain: AccountDomain = account_domain or DEFAULT_DOMAIN
        # The endpoints the clients are made on, at first use.
        self._dfs_url = f"https://{self.account_name}.dfs.{self.account_domain}"
        self._blob_url = f"https://{self.account_name}.blob.{self.account_domain}"
        self.credential = StorageCredential.from_credential(
            credential, **storage_options
        )
        # A token credential made from a string is this file system's to close.
        self._owns_credential = (
            self.credential.kind == "TokenCredential"
            and self.credential.value is not credential
        )
        # Declared, or set once `_check_hns_enabled` asks: the storage profile follows it. Until
        # loaded, it is the declared account's profile, or the unknown one's.
        self._is_hns_enabled: bool | None = is_hns_enabled
        self.storage_profile = _PROFILES[is_hns_enabled]

        # The user delegation key that `_get_user_delegation_key` keeps, and
        # the lock asking for one at a time.
        self._user_delegation_key: Any = None
        self._user_delegation_lock = asyncio.Lock()

        # Both clients, made together on the loop of their first use.
        self._blob_service_client: BlobServiceClient | None = None
        self._dfs_service_client: DataLakeServiceClient | None = None
        self._client_loop: asyncio.AbstractEventLoop | None = None

        super().__init__(
            operation_concurrency=operation_concurrency or self.operation_concurrency,
            io_concurrency=io_concurrency or self.io_concurrency,
            # No listing is cached: other clients change the storage too. See
            # USAGE.md's "No directory cache".
            use_listings_cache=False,
            **storage_options,
        )

    # Paths.

    @classmethod
    def _strip_protocol(cls, path: Any) -> Any:
        if isinstance(path, list):
            return [cls._strip_protocol(p) for p in path]
        else:
            return strip_protocol(str(path))

    def split_path(self, path: str) -> tuple[str, str]:
        """A path's container and its path within the container, each "" if none.

        A Hadoop-style URL must name the filesystem's own account, as the
        filesystem would otherwise act on a path of its own by that name.

        Parameters
        ----------
        path : str
            A path, ``/<container>/<path>``, or a URL of any form the
            filesystem takes, stripped here.

        Returns
        -------
        tuple of str
            The container, and the path within it, without a leading "/".

        Raises
        ------
        ValueError
            If ``path`` is a URL of another account.

        Examples
        --------
        >>> fs.split_path("abfss://data@myaccount.dfs.core.windows.net/raw/x.csv")
        ('data', 'raw/x.csv')
        """
        account = account_of(path)
        own = (self.account_name, self.account_domain)
        if account is not None and account != own:
            raise ValueError(
                f"{path!r} is of the account {account[0]}.{account[1]}, not of this "
                f"filesystem's, {own[0]}.{own[1]}"
            )
        else:
            return split_stripped(strip_protocol(path))

    def unstrip_protocol(self, name: str) -> str:
        """A path's URL in Hadoop's form, ``abfss://<container>@<account>.dfs.<domain>/<path>``.

        It is the form Spark and Hadoop's ABFS driver take, and names the
        account, so it is a path's full name outside the filesystem.

        Parameters
        ----------
        name : str
            A path, or a URL of the filesystem's account.

        Returns
        -------
        str
            The URL. A container's root has no trailing "/".

        Raises
        ------
        ValueError
            If ``name`` has no container, e.g. the root, as Hadoop's form has
            none for it, or is a URL of another account.

        Examples
        --------
        >>> fs.unstrip_protocol("/data/raw/x.csv")
        'abfss://data@myaccount.dfs.core.windows.net/raw/x.csv'
        """
        container, key = self.split_path(name)
        return hadoop_url(container, key, self.account_name, self.account_domain)

    @staticmethod
    def _get_kwargs_from_urls(path: str) -> dict[str, Any]:
        # A Hadoop-style URL names its account: fsspec.open() then needs no
        # account_name.
        account = account_of(path)
        if account is None:
            return {}
        else:
            return {"account_name": account[0], "account_domain": account[1]}

    # The clients.

    # The SDK's async clients hold an aiohttp session, bound to the loop of its
    # first request. A filesystem belongs to one loop, as fsspec caches its
    # async instances per loop: the clients are made once, and that loop kept
    # for closing them.

    def _ensure_service_clients(self) -> None:
        """Make both service clients, on the running loop, unless made already."""
        if self._closed:
            raise RuntimeError("The file system is already closed")
        if self._client_loop is None:
            loop = asyncio.get_running_loop()
            # Both on the account's two endpoints, with the same credential.
            self._blob_service_client = BlobServiceClient(
                self._blob_url, credential=self.credential.value
            )
            self._dfs_service_client = DataLakeServiceClient(
                self._dfs_url, credential=self.credential.value
            )
            self._client_loop = loop

    def _blob_service(self) -> BlobServiceClient:
        """The Blob service client."""
        self._ensure_service_clients()
        return self._blob_service_client  # type: ignore[return-value]

    def _dfs_service(self) -> DataLakeServiceClient:
        """The Data Lake service client."""
        self._ensure_service_clients()
        return self._dfs_service_client  # type: ignore[return-value]

    def _blob(self, container: str, key: str) -> BlobClient:
        return self._blob_service().get_blob_client(container, key)

    _closed = False

    def close(self) -> None:
        """Close both clients, then what `TreeFileSystem` holds. A closed filesystem refuses further calls."""
        try:
            clients: list[Any] = [
                c for c in (self._blob_service_client, self._dfs_service_client) if c
            ]
            # A credential of its own is closed with the clients, on their loop,
            # where its requests ran. Unused, it holds nothing.
            if clients and self._owns_credential:
                clients.append(self.credential.value)
                self._owns_credential = False
            loop = self._client_loop
            self._blob_service_client = self._dfs_service_client = None
            self._client_loop = None
            self._closed = True
            close_on_loop(loop, clients)
        finally:
            super().close()

    # Whether the account has a hierarchical namespace.

    @property
    def is_hns_enabled(self) -> bool | None:
        """Whether the account has a hierarchical namespace: None while unknown.

        Declared to the constructor, or else asked once, by the first
        operation needing the storage profile. An account that cannot tell
        stays unknown for the filesystem's life, with `UNKNOWN_HNS_PROFILE`,
        right on both kinds of account.
        """
        return self._is_hns_enabled

    async def _load_storage_profile(self, path: str) -> StorageProfile:
        """The profile of the account, asked at the container of ``path``: see `_check_hns_enabled`.

        The account is one namespace, so one profile fits the filesystem. The
        base class loads it once, with the paths of the first operation needing
        it, and keeps it as ``storage_profile``. A namespace declared to the
        constructor is never asked. Else the account is asked, by
        `_check_hns_enabled`, at the container of ``path``: a container's SAS or
        role is allowed its root ACL. What it tells, or None, is kept as
        `is_hns_enabled`.
        """
        if self._is_hns_enabled is None:
            self._is_hns_enabled = await self._check_hns_enabled(
                self.split_path(path)[0]
            )
        return _PROFILES[self._is_hns_enabled]

    async def _check_hns_enabled(self, container: str) -> bool | None:
        """Ask the account whether it has a hierarchical namespace: True, False, or None if unsure.

        It asks every time: `_load_storage_profile` keeps what it tells.

        1. The blob service's account information says so, but needs a role
           on the account: a container's SAS or role is refused it.
        2. Else the root ACL of ``container``, the container the first
           operation acted in, from the Data Lake API: an account without a
           hierarchical namespace refuses it by an error code of its own. An
           operation at the root has no container to ask. Any other failure
           tells nothing.

        Only an error's type, status and code are logged: Azure's messages may
        quote request URLs, with a SAS token in them.
        """
        blob_service, dfs_service = self._blob_service(), self._dfs_service()
        last: BaseException | None = None
        try:
            info = await blob_service.get_account_information()
            return bool(info.get("is_hns_enabled"))
        except AzureError as e:
            logger.debug("Account information refused: %s", describe(e))
            last = e

        if container:
            directory = dfs_service.get_file_system_client(
                container
            ).get_directory_client("/")
            try:
                await directory.get_access_control()
                return True
            except HttpResponseError as e:
                if getattr(e, "error_code", None) in _NO_HNS_CODES:
                    return False
                last = e
            except AzureError as e:
                last = e

        logger.warning(
            "Cannot tell whether the account %s has a hierarchical namespace"
            " (%s): using the blob API, and moves by copy and delete. Pass"
            " is_hns_enabled to say.",
            self.account_name,
            describe(last) if last else "no container to ask",
        )
        return None

    # Describing and listing.

    async def _info(self, path: str, **kwargs: Any) -> dict[str, Any]:
        container, key = self.split_path(path)
        path = self._strip_protocol(path)
        if not container:
            return dict(_root_info)

        blob_service = self._blob_service()
        container_client = blob_service.get_container_client(container)
        with azure_errors(path):
            if not key:
                props = await container_client.get_container_properties()
                return {
                    **_folder_entry(f"/{container}"),
                    "last_modified": props.last_modified,
                    "etag": bare_etag(props.etag),
                }

            try:
                props = await container_client.get_blob_client(
                    key
                ).get_blob_properties()
                return _blob_entry(container, props)
            except ResourceNotFoundError:
                # With a hierarchical namespace, a folder is a path of its
                # own: no prefix to list. The profile is asked once at most.
                if (
                    await self._get_storage_profile(path)
                ).implicit_directories is False:
                    raise

            # A folder that is only a prefix: one blob under it is enough.
            async for _ in container_client.list_blobs(
                name_starts_with=key + "/", results_per_page=1
            ):
                return _folder_entry(path)
        raise FileNotFoundError(errno.ENOENT, "No such file or directory", path)

    async def _ils(
        self, path: str, recursive: bool = False, **kwargs: Any
    ) -> AsyncIterator[dict[str, Any]]:
        container, key = self.split_path(path)
        path = self._strip_protocol(path)
        if not container:
            listing = self._ils_containers(recursive)
        elif (await self._get_storage_profile(path)).implicit_directories is False:
            listing = self._ils_dfs(container, key, recursive)
        else:
            listing = self._ils_blob(container, key, recursive)

        listed = False
        try:
            async for entry in listing:
                listed = True
                yield entry
        except ResourceNotFoundError as e:
            # A Data Lake listing of a file is refused as not found: the file
            # lists as itself, as fsspec's _ls lists it, and a path missing
            # raises FileNotFoundError by its info.
            entry = None if listed or not container else await self._info(path)
            if entry is None or entry["type"] == "directory":
                raise os_error(e, path) from e
            yield entry
            return
        except AzureError as e:
            # Any other failure, e.g. refused or throttled, is the listing's.
            raise os_error(e, path) from e

        if not listed and container:
            # Nothing listed: an empty folder, a file, or nothing at all.
            entry = await self._info(path)
            if entry["type"] != "directory":
                yield entry

    async def _ils_containers(self, recursive: bool) -> AsyncIterator[dict[str, Any]]:
        """The containers at the root, each followed by all in it when ``recursive``: depth first."""
        async for c in self._blob_service().list_containers():
            yield {
                **_folder_entry(f"/{c.name}"),
                "last_modified": c.last_modified,
                "etag": bare_etag(c.etag),
            }
            if recursive:
                async for entry in self._ils(f"/{c.name}", recursive=True):
                    yield entry

    async def _ils_dfs(
        self, container: str, key: str, recursive: bool = False
    ) -> AsyncIterator[dict[str, Any]]:
        """One folder's paths, or all below it, from the Data Lake API, a page at a time."""
        file_system = self._dfs_service().get_file_system_client(container)
        async for p in file_system.get_paths(path=key or None, recursive=recursive):
            name = f"/{container}/{p.name}"
            # The Data Lake SDK gives a time of no zone, in UTC: the blob API's
            # are aware, so the entries of both compare.
            modified = p.last_modified
            if modified is not None and modified.tzinfo is None:
                modified = modified.replace(tzinfo=UTC)
            if p.is_directory:
                yield {
                    **_folder_entry(name),
                    "last_modified": modified,
                    "etag": bare_etag(p.etag),
                }
            else:
                yield {
                    "name": name,
                    "size": p.content_length,
                    "type": "file",
                    "last_modified": modified,
                    "etag": bare_etag(p.etag),
                }

    async def _ils_blob(
        self, container: str, key: str, recursive: bool = False
    ) -> AsyncIterator[dict[str, Any]]:
        """One folder's blobs and prefixes, from the blob API, as they come a page at a time.

        On an account with a hierarchical namespace, or with marker blobs, a
        folder comes twice: as a blob with ``hdi_isfolder``, and as a prefix
        if it holds anything. Only its first is yielded: the names of the
        folders yielded are kept, those of the files are not.
        """
        prefix = f"{key}/" if key else ""
        container_client = self._blob_service().get_container_client(container)
        if recursive:
            # All blobs below, folders' marker blobs included: the strategies
            # tell the folders without one from the paths below them. Each next
            # page is asked for while one is parsed: see _listing.
            async for entry in list_blobs_flat(container_client, container, prefix):
                yield entry
            return
        folders: set[str] = set()
        async for item in container_client.walk_blobs(
            name_starts_with=prefix, delimiter="/", include=["metadata"]
        ):
            if item.name == prefix:
                continue  # The folder's own marker blob.
            if item.name.endswith("/"):  # A prefix.
                entry = _folder_entry(f"/{container}/{item.name}")
            else:
                entry = _blob_entry(container, item)
            if entry["type"] == "directory":
                if entry["name"] in folders:
                    continue
                folders.add(entry["name"])
            yield entry

    # Changing paths.

    async def _rm_file(self, path: str, **kwargs: Any) -> None:
        container, key = self.split_path(path)
        path = self._strip_protocol(path)
        if not key:
            raise IsADirectoryError(errno.EISDIR, "Is a container", path)
        with azure_errors(path):
            await self._blob(container, key).delete_blob()

    async def _has_children(self, container: str, key: str) -> bool:
        container_client = self._blob_service().get_container_client(container)
        prefix = f"{key}/" if key else ""
        async for blob in container_client.list_blobs(
            name_starts_with=prefix, results_per_page=2
        ):
            if blob.name != prefix:
                return True
        return False

    async def _rmdir(self, path: str) -> None:
        container, key = self.split_path(path)
        path = self._strip_protocol(path)
        if not container:
            raise PermissionError(errno.EACCES, "Not deleting the root", path)

        info = await self._info(path)
        if info["type"] != "directory":
            raise NotADirectoryError(errno.ENOTDIR, "Not a directory", path)

        with azure_errors(path):
            # The Data Lake API's deletion of a folder is recursive: the blob
            # API's is not, on an account with a hierarchical namespace. Where
            # the service does not refuse a folder with things in, it is checked.
            # A container's deletion is recursive on both kinds of account.
            real = (await self._get_storage_profile(path)).implicit_directories is False
            if not (real and key) and await self._has_children(container, key):
                raise OSError(errno.ENOTEMPTY, "Directory not empty", path)
            if not key:
                await self._blob_service().delete_container(container)
                return
            try:
                await self._blob(container, key).delete_blob()
            except ResourceNotFoundError:
                if real:
                    raise
                # A folder with no blob of its own is gone with its last blob.

    async def _rm_tree(self, path: str) -> None:
        """Delete a folder with all in it, or a file, by one Data Lake request.

        Called only with a hierarchical namespace, as `HNS_PROFILE` declares.
        """
        container, key = self.split_path(path)
        path = self._strip_protocol(path)
        if not key:
            raise IsADirectoryError(errno.EISDIR, "Not deleting a container", path)
        # A path's deletion: of a folder, recursive, or of a file.
        directory = self._dfs_service().get_directory_client(container, key)
        with azure_errors(path):
            await directory.delete_directory()

    async def _makedirs(self, path: str, exist_ok: bool = False) -> None:
        """Make a folder and its missing parents, in an existing container.

        No container is made: a folder in a missing one raises
        FileNotFoundError, and a container missing itself PermissionError.
        Containers are made by Azure's own means, with their settings.
        """
        container, key = self.split_path(path)
        path = self._strip_protocol(path)
        if not container:
            if exist_ok:
                return
            raise FileExistsError(errno.EEXIST, "The root exists", path)

        try:
            info = await self._info(path)
        except FileNotFoundError:
            if not key:
                raise PermissionError(
                    errno.EPERM, "AbfsFileSystem makes no container", path
                ) from None
        else:
            if info["type"] != "directory":
                raise FileExistsError(errno.EEXIST, "A file exists", path)
            if not exist_ok:
                raise FileExistsError(errno.EEXIST, "File exists", path)
            return

        dfs_service = self._dfs_service()
        with azure_errors(path):
            implicit = (await self._get_storage_profile(path)).implicit_directories
            if implicit is False:
                await dfs_service.get_directory_client(
                    container, key
                ).create_directory()
            elif implicit is None:
                # A blob marking a folder: a real folder on an account with a
                # hierarchical namespace, a marker blob on one without, which
                # the listings take as a folder.
                await self._blob(container, key).upload_blob(
                    b"", metadata={"hdi_isfolder": "true"}, overwrite=True
                )
            else:
                # Without a hierarchical namespace, a folder is made by its
                # files: nothing is sent, but a missing container is told.
                await self._info(f"/{container}")

    async def _cp_file(self, path1: str, path2: str, **kwargs: Any) -> None:
        """Copy a blob on the server, waiting for the copy if it is pending.

        Works with or without a hierarchical namespace. The source's URL is
        its client's, carrying a SAS token when the credential is one.
        """
        container, key = self.split_path(path1)
        path1 = self._strip_protocol(path1)
        if not key:
            raise IsADirectoryError(errno.EISDIR, "Is a container", path1)

        # The destination is checked there, as for a copy from another account.
        await self._copy_from_url(self._blob(container, key).url, path1, path2)

    async def _copy_from_url(self, source_url: str, source: str, path: str) -> None:
        """Copy a blob from ``source_url`` to ``path`` on the server, waiting for the copy if it is pending.

        ``source_url`` is a blob's URL, of this account or another, with what
        authorizes reading it, e.g. a SAS. ``source`` names it in errors.
        """
        container, key = self.split_path(path)
        path = self._strip_protocol(path)
        if not key:
            raise IsADirectoryError(errno.EISDIR, "Is a container", path)

        destination = self._blob(container, key)
        try:
            copy = await destination.start_copy_from_url(source_url)
            status = copy.get("copy_status")
            delay = self.copy_poll_interval
            while status == "pending":
                await asyncio.sleep(delay)
                delay = min(delay * 2, 5.0)
                props = await destination.get_blob_properties()
                status = props.copy.status
        except ResourceNotFoundError as e:
            # The source missing, by the service's code: else the destination's container.
            missing = path if e.error_code == "ContainerNotFound" else source
            raise os_error(e, missing) from e
        except AzureError as e:
            raise os_error(e, path) from e
        if status != "success":
            raise OSError(errno.EIO, f"Copy from {source} ended as {status}", path)

    # Signed URLs, e.g. for another account's server-side copy.

    async def _get_user_delegation_key(self) -> UserDelegationKey:
        """The account's user delegation key, asked for once and kept until it nears its expiry.

        A user delegation key signs SASs with the Microsoft Entra ID identity
        of the credential. Asking for one needs the action
        ``generateUserDelegationKey``, which Storage Blob Data Reader,
        Contributor and Owner, and Storage Blob Delegator, give at the
        account's level: see Azure's
        https://learn.microsoft.com/en-us/rest/api/storageservices/create-user-delegation-sas#assign-permissions-with-rbac.

        The key is asked for once, valid for `_user_delegation_key_lifetime`,
        and kept. A new one is asked for when the kept one has less than
        `_user_delegation_key_margin` left. Concurrent callers wait for one
        request.

        Returns
        -------
        azure.storage.blob.UserDelegationKey

        Raises
        ------
        ValueError
            If the credential is no token credential: only one can ask.
        PermissionError
            If the identity may not ask for a key, e.g. with a role on a
            container only.
        """
        if self.credential.kind != "TokenCredential":
            raise ValueError(
                "A user delegation key needs a Microsoft Entra ID token credential,"
                f" not {self.credential.kind}"
            )

        async with self._user_delegation_lock:
            key = self._user_delegation_key
            margin = self._user_delegation_key_margin
            if key is None or _key_expiry(key) - datetime.now(UTC) < margin:
                start = datetime.now(UTC) - _CLOCK_SKEW
                with azure_errors(f"{self.account_name}.blob.{self.account_domain}"):
                    key = await self._blob_service().get_user_delegation_key(
                        key_start_time=start,
                        key_expiry_time=start + self._user_delegation_key_lifetime,
                    )
                self._user_delegation_key = key
            return key

    async def _sign(self, path: str, expiration: int = 100, **kwargs: Any) -> str:
        """A blob's URL that reads it with no other credential, e.g. for a browser or another service.

        It is the URL of the blob endpoint, as another account's server-side
        copy takes it. What the URL carries depends on the credential:

        - a token credential signs a user delegation SAS with the account's
          user delegation key, which is asked for once and kept;
        - a shared key signs a SAS;
        - a SAS is the URL's own, valid as long as it is;
        - anonymous, the URL carries nothing: the blob must be public.

        A SAS signed here reads that blob only.

        Parameters
        ----------
        path : str
            The blob's path or URL.
        expiration : int, default 100
            How long a SAS signed here is valid, in seconds. One of a user
            delegation key expires with the key at the latest, at least 12
            hours away.
        **kwargs
            Ignored, as by fsspec's ``sign``.

        Returns
        -------
        str
            The blob's URL, with its SAS if any.

        Raises
        ------
        IsADirectoryError
            If ``path`` is a container or the root.
        PermissionError
            If the identity may not ask for a user delegation key, e.g. with a
            role on a container only.

        Examples
        --------
        >>> fs.sign("/mycontainer/data/file.csv", expiration=3600)
        'https://myaccount.blob.core.windows.net/mycontainer/data/file.csv?skoid=...&sig=...'
        """
        container, key = self.split_path(path)
        path = self._strip_protocol(path)
        if not key:
            raise IsADirectoryError(errno.EISDIR, "Is a container", path)

        url = self._blob(container, key).url
        now = datetime.now(UTC)
        expiry = now + timedelta(seconds=expiration)
        if self.credential.kind == "TokenCredential":
            delegation = await self._get_user_delegation_key()
            signing = {
                "user_delegation_key": delegation,
                "expiry": min(expiry, _key_expiry(delegation)),
            }
        elif self.credential.kind == "SharedKey":
            signing = {"account_key": self.credential.value, "expiry": expiry}
        else:
            return url

        sas = generate_blob_sas(
            self.account_name,
            container,
            key,
            permission=BlobSasPermissions(read=True),
            start=now - _CLOCK_SKEW,
            **signing,
        )
        return f"{url}?{sas}"

    sign = sync_wrapper(_sign)

    async def _cp_file_entry(
        self, entry: dict[str, Any], path2: str, **kwargs: Any
    ) -> None:
        """Copy a blob by its entry, as `_cp_file` does: a copy on the server needs nothing of it."""
        await self._cp_file(entry["name"], path2, **kwargs)

    async def _mv_file_entry(
        self, entry: dict[str, Any], path2: str, **kwargs: Any
    ) -> None:
        """Move a file or a folder by its entry, as `_mv_file` does: a rename needs nothing of it."""
        await self._mv_file(entry["name"], path2)

    async def _mv_tree(self, path1: str, path2: str) -> None:
        """Move a folder with all in it, or a file, as `_mv_file` does: one rename."""
        await self._mv_file(path1, path2)

    async def _mv_file(self, path1: str, path2: str, **kwargs: Any) -> None:
        """Move a file, or a folder with all in it: a rename with a hierarchical namespace.

        A rename is one atomic request, within a container or between two of
        the account. It needs the destination's parent folder, which the
        strategies create first. It asks no properties of the path, where the
        library's ``_mv_file`` always asks them.

        On an account without a hierarchical namespace, or not known to have
        one, a file is copied on the server, then deleted. A container itself
        is never renamed: moving one raises IsADirectoryError, from the copy.
        """
        (c1, k1), (c2, k2) = self.split_path(path1), self.split_path(path2)
        path1, path2 = self._strip_protocol(path1), self._strip_protocol(path2)
        profile = await self._get_storage_profile(path1)
        if not profile.renames_files or not k1 or not k2:
            await self._cp_file(path1, path2)
            await self._rm_file(path1)
            return

        # A rename creates no parent: the strategies create it first. Its new
        # name is "<container>/<path>", without the leading "/": the container
        # may be another of the account.
        new_name = f"{c2}/{k2}"
        if c1 != c2:
            # The SDK sends another container's rename with the client's
            # credential, but holds a SAS in the source's URL only: it takes the
            # destination's from the new name. Anonymous, it refuses the rename.
            credential = self.credential
            if credential.kind == "Anonymous":
                raise PermissionError(
                    errno.EACCES, "No anonymous rename between containers", path1
                )
            elif credential.kind == "SAS":
                new_name += f"?{credential.value}"
        source = self._dfs_service().get_file_client(c1, k1)
        with azure_errors(path1):
            await source.rename_file(new_name)

    # Files.

    async def _cat_file(
        self,
        path: str,
        start: int | None = None,
        end: int | None = None,
        **kwargs: Any,
    ) -> bytes:
        """Read a blob, or a range of it, by one download, with no properties read first.

        The SDK asks a range by its offsets, so a start or an end counted from
        the end of the file needs the file's size first. A missing blob raises
        FileNotFoundError from the download's 404. A folder is a marker blob
        with a hierarchical namespace, or no blob without one: it raises
        IsADirectoryError or FileNotFoundError, as `_open_file` does.
        """
        container, key = self.split_path(path)
        path = self._strip_protocol(path)
        if not key:
            raise IsADirectoryError(errno.EISDIR, "Is a directory", path)
        info: dict[str, Any] = {}
        start = start or 0
        if start < 0 or (end is not None and end < 0):
            info.update(await self._info(path))
            size = int(info["size"])
            if start < 0:
                start = max(0, start + size)
            if end is not None and end < 0:
                end = max(0, end + size)
        if end is not None and end <= start:
            if (info or await self._info(path))["type"] == "directory":
                raise IsADirectoryError(errno.EISDIR, "Is a directory", path)
            return b""
        length = None if end is None else end - start
        # A read of data, as the files' are: under io_limit, or fsspec's cat of many paths sends them all at once.
        with azure_errors(path):
            async with self.io_limit:
                try:
                    # The SDK fetches beyond its first 32 MiB in 4 MiB chunks, one at a
                    # time by default, so a large file costs many round trips. They go
                    # at once, as many as the readers' spread, under this one permit.
                    downloader = await self._blob(container, key).download_blob(
                        offset=None if start == 0 and end is None else start,
                        length=length,
                        max_concurrency=AbfsReadableFile.spread,
                    )
                except HttpResponseError as e:
                    if e.status_code == 416:
                        return (
                            b""  # Beyond the end: nothing, as fsspec's slicing gives.
                        )
                    raise
                if _is_folder_blob(downloader.properties.metadata):
                    raise IsADirectoryError(errno.EISDIR, "Is a directory", path)
                return await downloader.readall()

    async def _open_file(self, path: str, mode: str, **kwargs: Any) -> Any:
        """Open an adaptive file on the running loop."""
        if mode not in ("rb", "wb", "xb"):
            raise UnsupportedOperation(f"Unsupported mode {mode!r}: rb, wb or xb")
        container, key = self.split_path(path)
        path = self._strip_protocol(path)
        if not key:
            raise IsADirectoryError(errno.EISDIR, "Is a directory", path)

        blob = self._blob(container, key)
        loop = asyncio.get_running_loop()
        if mode == "rb":
            info = await self._info(path)
            if info["type"] == "directory":
                raise IsADirectoryError(errno.EISDIR, "Is a directory", path)
            return AbfsReadableFile(
                path,
                loop,
                int(info["size"]),
                blob=blob,
                version=info.get("etag"),
                concurrency_limit=self.io_limit,
            )
        else:
            return AbfsWritableFile(
                path,
                loop,
                blob=blob,
                mode=mode,
                concurrency_limit=self.io_limit,
            )

    async def open_async(self, path: str, mode: str = "rb", **kwargs: Any) -> Any:
        if kwargs.get("compression"):
            raise ValueError("Compression is not supported by open_async")
        return AsyncFileAdapter(await self._open_file(path, mode))

    def _open(
        self,
        path: str,
        mode: str = "rb",
        block_size: int | None = None,
        autocommit: bool = True,
        cache_options: dict[str, Any] | None = None,
        **kwargs: Any,
    ) -> Any:
        f = sync(self.loop, self._open_file, path, mode)
        if autocommit or mode == "rb":
            return f
        else:
            # In a transaction, the block list is committed on commit: no other
            # client sees the file before.
            return TransactionFile(AdaptiveWritableTransaction(f))


class AbfsReadableFile(AdaptiveReadableFile):
    """A blob read by ranges, all of one version: its ETag, by If-Match.

    Its block and split sizes are the library's defaults, which suit Azure.
    """

    def __init__(
        self,
        path: str,
        loop: asyncio.AbstractEventLoop,
        size: int,
        *,
        blob: BlobClient,
        **kwargs: Any,
    ) -> None:
        super().__init__(path, loop, size, **kwargs)
        self._blob_client = blob

    async def _fetch_range(self, start: int, end: int) -> bytes:
        conditions: dict[str, Any] = {}
        if self.version is not None:
            conditions = {
                # An entry's ETag is bare: the header takes it quoted.
                "etag": f'"{self.version}"',
                "match_condition": MatchConditions.IfNotModified,
            }
        with azure_errors(self.path):
            try:
                downloader = await self._blob_client.download_blob(
                    offset=start, length=end - start, **conditions
                )
                data = await downloader.readall()
            except ResourceModifiedError as e:
                raise OSError(
                    errno.ESTALE, f"Changed while read, from version {self.version}"
                ) from e
        self._check_version(bare_etag(downloader.properties.etag))
        return data


class AbfsWritableFile(AdaptiveWritableFile):
    """A block blob, uploaded whole, or in blocks committed on closing.

    Blocks staged but never committed take no visible place: the service
    drops them after a week. So the file is written whole, or not at all,
    and an abort sends nothing, leaving any file already at the path as it
    was. Mode "xb" is checked by the request making the file visible, with
    ``If-None-Match: *``, so that no other writer can slip in.
    """

    min_block_size: int = 4 * _MiB
    max_block_size: int = 64 * _MiB
    min_multipart_size: int = 8 * _MiB

    def __init__(
        self,
        path: str,
        loop: asyncio.AbstractEventLoop,
        *,
        blob: BlobClient,
        **kwargs: Any,
    ) -> None:
        super().__init__(path, loop, **kwargs)
        self._blob_client = blob
        self._upload_id = ""

    def _conditions(self) -> dict[str, Any]:
        """Commit only if no blob exists, for mode "xb"."""
        if not self.exclusive:
            return {}
        else:
            # The SDK sends If-None-Match: * itself. An etag given too would not
            # be taken, but passed on to the HTTP session, which refuses it.
            return {"match_condition": MatchConditions.IfMissing}

    def _exists_error(self, e: AzureError) -> OSError:
        return FileExistsError(errno.EEXIST, f"File exists ({describe(e)})", self.path)

    async def _upload_all(self, data: bytes) -> None:
        with azure_errors(self.path):
            try:
                await self._blob_client.upload_blob(data, overwrite=not self.exclusive)
            except ResourceExistsError as e:
                raise self._exists_error(e) from e

    async def _start_multipart_upload(self) -> None:
        # Block ids must have one length within a blob: a random prefix, then
        # the index, zero-padded.
        self._upload_id = uuid.uuid4().hex

    async def _upload_part(
        self, data: bytes, start: int, end: int, index: int, last: bool
    ) -> str:
        block_id = base64.b64encode(f"{self._upload_id}-{index:06d}".encode()).decode()
        with azure_errors(self.path):
            await self._blob_client.stage_block(block_id, data, length=len(data))
        return block_id

    async def _complete_multipart_upload(self, parts: list[Any]) -> None:
        with azure_errors(self.path):
            try:
                await self._blob_client.commit_block_list(
                    [BlobBlock(block_id=b) for b in parts], **self._conditions()
                )
            except (ResourceExistsError, ResourceModifiedError) as e:
                if self.exclusive:
                    raise self._exists_error(e) from e
                raise

    async def _abort_multipart_upload(self) -> None:
        pass  # Uncommitted blocks are dropped by the service.
