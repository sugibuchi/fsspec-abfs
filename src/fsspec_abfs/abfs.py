"""`AbfsFileSystem`: an async fsspec filesystem of an Azure storage account.

It walks folder trees with the tree strategies of fsspec-alt-strategies,
copied into ``_contrib.strategy``, and implements their primitives with
Azure's two APIs of one account:

- the blob API, for what both kinds of account answer alike: a path's
  properties, deletions, server-side copies, and the files' data;
- the Data Lake API, for what only an account with a hierarchical namespace
  (HNS) has: real folders, listings of one folder, atomic renames and
  recursive deletes.

Whether the account has a hierarchical namespace is declared, or asked once
known for sure. While unknown, the filesystem takes the way right on both
kinds: the blob API, and moves by copy and delete.
"""

from __future__ import annotations

import asyncio
import base64
import contextlib
import errno
import logging
import sys
import uuid
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
from azure.storage.blob import BlobBlock
from azure.storage.blob.aio import BlobServiceClient
from azure.storage.filedatalake.aio import DataLakeServiceClient
from fsspec.asyn import sync
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
from ._paths import account_url_of, parse_account_url, split_path, strip_protocol
from .credentials import StorageCredential

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Awaitable, Callable, Sequence

    from azure.storage.blob.aio import BlobClient

    from ._paths import AccountEndpoints

__all__ = [
    "HNS_PROFILE",
    "NO_HNS_PROFILE",
    "UNKNOWN_HNS_PROFILE",
    "AbfsFileSystem",
    "AbfsReadableFile",
    "AbfsWritableFile",
]

logger = logging.getLogger(__name__)

_MiB = 1024 * 1024

HNS_PROFILE = StorageProfile(
    fs_type="tree",
    parents_required=False,
    implicit_folders=False,
    renames_files=True,
    renames_folders=True,
)
"""An account with a hierarchical namespace.

- Its folders are paths of their own: an empty one exists, and is deleted by
  a request of its own.
- A blob written creates its missing parent folders, as the blob API does on
  such an account. A rename does not: `AbfsFileSystem._mv_file` creates them.
- A rename moves a file, or a folder with all in it, in one atomic request.
"""

NO_HNS_PROFILE = StorageProfile(
    fs_type="tree",
    parents_required=False,
    implicit_folders=True,
    renames_files=False,
    renames_folders=False,
)
"""An account without a hierarchical namespace: blobs whose folders are prefixes.

It is still listed a folder per request, by the blob API's delimiter, so the
tree strategies suit it. A folder exists only through the blobs under it, and
there is no rename: a move copies on the server, then deletes.
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

_root_info = {"name": "", "size": 0, "type": "directory"}


def _is_folder_blob(metadata: dict[str, str] | None) -> bool:
    """Whether a blob stands for a folder: a folder of an HNS account, or a marker blob.

    Both carry the metadata ``hdi_isfolder=true``, the Hadoop drivers' marker.
    """
    return (metadata or {}).get("hdi_isfolder", "").lower() == "true"


def _blob_entry(container: str, props: Any) -> dict[str, Any]:
    """The fsspec entry of a blob's properties: a file, or a folder by its metadata."""
    name = f"{container}/{props.name.rstrip('/')}"
    if _is_folder_blob(props.metadata):
        return {
            "name": name,
            "size": 0,
            "type": "directory",
            "last_modified": props.last_modified,
            "etag": props.etag,
        }
    return {
        "name": name,
        "size": props.size,
        "type": "file",
        "last_modified": props.last_modified,
        "etag": props.etag,
        "content_type": getattr(props.content_settings, "content_type", None),
    }


def _folder_entry(name: str) -> dict[str, Any]:
    return {"name": name.rstrip("/"), "size": 0, "type": "directory"}


class AbfsFileSystem(TreeFileSystem):
    """An async fsspec filesystem of an Azure storage account, e.g. ADLS Gen2.

    Its paths are ``<container>/<path>``, and its root lists the containers.
    It takes ``abfs://`` and ``abfss://`` URLs, in adlfs' form,
    ``abfs://<container>/<path>``, or Hadoop's,
    ``abfss://<container>@<account>.dfs.core.windows.net/<path>``. ``az://``
    URLs are taken too.

    The recursive methods are the tree strategies of fsspec-alt-strategies.
    Their storage profile follows the account: see `HNS_PROFILE`,
    `NO_HNS_PROFILE` and `UNKNOWN_HNS_PROFILE`.

    Parameters
    ----------
    account_url : str
        The account's dfs or blob endpoint, e.g.
        ``https://<account>.dfs.core.windows.net/``, or Fabric OneLake's
        ``https://onelake.dfs.fabric.microsoft.com/``.
    credential : AsyncTokenCredential or str, optional
        An async token credential, or a string naming one, a SAS token or a
        shared key, as `StorageCredential.from_credential` reads it. None is
        anonymous. A token credential given is the caller's to close. One made
        from a string is the file system's, closed by `close`.
    credential_options : dict, optional
        Arguments of the token credential a string names, e.g.
        ``{"tenant_id": ...}``.
    is_hns_enabled : bool, optional
        Whether the account has a hierarchical namespace, as Azure's account
        information names it. None asks the account on first need: see
        `is_hns_enabled`.
    operation_concurrency : int or asyncio.Semaphore, optional
        The strategies' requests at a time, e.g. listings and deletions.
    io_concurrency : int or asyncio.Semaphore, optional
        The range reads and block uploads at a time, of all files opened.
    **storage_options
        Passed to `fsspec.asyn.AsyncFileSystem`, e.g. ``asynchronous=True``.

    Raises
    ------
    ValueError
        If ``account_url`` is no account endpoint.

    Examples
    --------
    >>> from azure.identity.aio import DefaultAzureCredential
    >>> fs = AbfsFileSystem(
    ...     "https://myaccount.dfs.core.windows.net/",
    ...     credential=DefaultAzureCredential(),
    ...     is_hns_enabled=True,
    ... )
    >>> fs.ls("mycontainer/data")
    """

    protocol: ClassVar[tuple[str, ...]] = ("abfs", "abfss")
    root_marker = ""

    operation_concurrency: int = 32
    """The default of the strategies' requests at a time."""

    io_concurrency: int = 16
    """The default of the files' range reads and block uploads at a time."""

    copy_poll_interval: float = 0.2
    """The first wait, in seconds, before checking a pending server-side copy. It doubles up to 5 s."""

    def __init__(
        self,
        account_url: str,
        credential: Any = None,
        *,
        credential_options: dict[str, Any] | None = None,
        is_hns_enabled: bool | None = None,
        operation_concurrency: int | asyncio.Semaphore | None = None,
        io_concurrency: int | asyncio.Semaphore | None = None,
        _endpoints: AccountEndpoints | None = None,
        **storage_options: Any,
    ) -> None:
        # `_endpoints` is for tests and emulators only: their endpoints, e.g. on
        # http://127.0.0.1, are no account URL. account_url is then ignored.
        self.endpoints = _endpoints or parse_account_url(account_url)
        self.account_url = self.endpoints.dfs_url
        self.storage_credential = StorageCredential.from_credential(
            credential, **(credential_options or {})
        )
        # A token credential made from a string is this file system's to close.
        self._owns_credential = (
            self.storage_credential.kind == "TokenCredential"
            and self.storage_credential.value is not credential
        )
        # Set before TreeFileSystem's check of the profile, which reads it.
        self._is_hns_enabled: bool | None = is_hns_enabled
        self._hns_warned = False

        # Both clients, made together on the loop of their first use.
        self._blob_service_client: BlobServiceClient | None = None
        self._dfs_service_client: DataLakeServiceClient | None = None
        self._client_loop: asyncio.AbstractEventLoop | None = None

        super().__init__(
            operation_concurrency=operation_concurrency or self.operation_concurrency,
            io_concurrency=io_concurrency or self.io_concurrency,
            **storage_options,
        )

    # Paths.

    @classmethod
    def _strip_protocol(cls, path: Any) -> Any:
        if isinstance(path, list):
            return [cls._strip_protocol(p) for p in path]
        return strip_protocol(str(path))

    @staticmethod
    def _get_kwargs_from_urls(path: str) -> dict[str, Any]:
        # A Hadoop-style URL names its account: fsspec.open() then needs no
        # account_url.
        account_url = account_url_of(path)
        return {"account_url": account_url} if account_url else {}

    # The clients.

    # The SDK's async clients hold an aiohttp session, bound to the loop of its
    # first request: a filesystem used on another loop gets new ones.

    def _ensure_service_clients(self) -> None:
        """Make both service clients on the running loop, unless made there already."""
        if self._closed:
            raise RuntimeError("The file system is already closed")
        loop = asyncio.get_running_loop()
        if self._client_loop is not loop:
            # Both on the account's two endpoints, with the same credential.
            self._blob_service_client = BlobServiceClient(
                self.endpoints.blob_url, credential=self.storage_credential.value
            )
            self._dfs_service_client = DataLakeServiceClient(
                self.endpoints.dfs_url, credential=self.storage_credential.value
            )
            self._client_loop = loop

    def _blob_service(self) -> BlobServiceClient:
        """The Blob service client of the running loop."""
        self._ensure_service_clients()
        return self._blob_service_client  # type: ignore[return-value]

    def _dfs_service(self) -> DataLakeServiceClient:
        """The Data Lake service client of the running loop."""
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
                clients.append(self.storage_credential.value)
                self._owns_credential = False
            loop = self._client_loop
            self._blob_service_client = self._dfs_service_client = None
            self._client_loop = None
            self._closed = True
            if clients and loop is not None and not loop.is_closed():
                try:
                    running = asyncio.get_running_loop()
                except RuntimeError:
                    running = None

                async def close_clients() -> None:
                    for client in clients:
                        with contextlib.suppress(Exception):
                            await client.close()

                if running is loop:
                    # Nothing can wait for it on its own loop.
                    loop.create_task(close_clients())
                elif loop.is_running() and not sys.is_finalizing():
                    sync(loop, close_clients, timeout=5.0)
        finally:
            super().close()

    # Whether the account has a hierarchical namespace.

    @property
    def is_hns_enabled(self) -> bool | None:
        """Whether the account has a hierarchical namespace: None while unknown.

        Declared to the constructor, or else asked by the first operation
        needing it, and kept once sure. An unknown answer is asked again by
        the next operation: a wrong one kept would last as long as the
        filesystem, e.g. folders never created, or moves not atomic.
        """
        return self._is_hns_enabled

    @property
    def storage_profile(self) -> StorageProfile:  # type: ignore[override]
        """The profile of the account, as far as known: see `is_hns_enabled`."""
        return _PROFILES[self._is_hns_enabled]

    async def _hns(self, container: str) -> bool | None:
        """Whether the account has a hierarchical namespace: True, False, or None if unsure.

        Asked until known, then kept. Concurrent first callers may each ask:
        a few requests, once, cost less than sharing one.

        1. The blob service's account information says so, but needs a role
           on the account: a container's SAS or role is refused it.
        2. Else the root ACL of ``container``, the container of the path
           operated on, from the Data Lake API: an account without a
           hierarchical namespace refuses it by an error code of its own. Any
           other failure tells nothing.

        Only an error's type, status and code are logged: Azure's messages may
        quote request URLs, with a SAS token in them.
        """
        if self._is_hns_enabled is not None:
            return self._is_hns_enabled

        blob_service, dfs_service = self._blob_service(), self._dfs_service()
        last: BaseException | None = None
        try:
            info = await blob_service.get_account_information()
            self._is_hns_enabled = bool(info.get("is_hns_enabled"))
            return self._is_hns_enabled
        except AzureError as e:
            logger.debug("Account information refused: %s", describe(e))
            last = e

        if container:
            directory = dfs_service.get_file_system_client(
                container
            ).get_directory_client("/")
            try:
                await directory.get_access_control()
                self._is_hns_enabled = True
                return True
            except HttpResponseError as e:
                if getattr(e, "error_code", None) in _NO_HNS_CODES:
                    self._is_hns_enabled = False
                    return False
                last = e
            except AzureError as e:
                last = e

        if not self._hns_warned:
            self._hns_warned = True
            logger.warning(
                "Cannot tell whether the account %s has a hierarchical namespace"
                " (%s): using the blob API, and moves by copy and delete, until"
                " known. Pass is_hns_enabled to say.",
                self.endpoints.account_name,
                describe(last) if last else "no container to ask",
            )
        return None

    async def _hns_of(self, path: str | Sequence[str]) -> bool | None:
        """`_hns`, for the container of a path or of the first of paths."""
        first = path if isinstance(path, str) else next(iter(path), "")
        return await self._hns(split_path(self._strip_protocol(first))[0])

    # The strategies read the profile when they start: the account is asked first.

    async def _rm(self, path: Any, recursive: bool = False, *args: Any, **kwargs: Any):
        await self._hns_of(path)
        return await super()._rm(path, recursive, *args, **kwargs)

    async def _copy(self, path1: Any, path2: Any, *args: Any, **kwargs: Any):
        await self._hns_of(path1)
        return await super()._copy(path1, path2, *args, **kwargs)

    async def _mv(self, path1: Any, path2: Any, *args: Any, **kwargs: Any):
        await self._hns_of(path1)
        return await super()._mv(path1, path2, *args, **kwargs)

    @property
    def _rm_tree(self) -> Callable[[str], Awaitable[None]] | None:  # type: ignore[override]
        # The Data Lake API deletes a folder with all in it, in one request.
        return self._delete_tree if self._is_hns_enabled else None

    async def _delete_tree(self, path: str) -> None:
        container, key = split_path(self._strip_protocol(path))
        if not key:
            raise IsADirectoryError(errno.EISDIR, "Not deleting a container", path)
        directory = self._dfs_service().get_directory_client(container, key)
        with azure_errors(path):
            await directory.delete_directory()

    # Describing and listing.

    async def _info(self, path: str, **kwargs: Any) -> dict[str, Any]:
        path = self._strip_protocol(path)
        container, key = split_path(path)
        if not container:
            return dict(_root_info)

        blob_service = self._blob_service()
        container_client = blob_service.get_container_client(container)
        with azure_errors(path):
            if not key:
                props = await container_client.get_container_properties()
                return {
                    **_folder_entry(container),
                    "last_modified": props.last_modified,
                    "etag": props.etag,
                }

            try:
                props = await container_client.get_blob_client(
                    key
                ).get_blob_properties()
                return _blob_entry(container, props)
            except ResourceNotFoundError:
                if self._is_hns_enabled:
                    raise

            # A folder that is only a prefix: one blob under it is enough.
            async for _ in container_client.list_blobs(
                name_starts_with=key + "/", results_per_page=1
            ):
                return _folder_entry(path)
        raise FileNotFoundError(errno.ENOENT, "No such file or directory", path)

    async def _ils(self, path: str, **kwargs: Any) -> AsyncIterator[dict[str, Any]]:
        path = self._strip_protocol(path)
        container, key = split_path(path)
        if not container:
            with azure_errors(path):
                async for c in self._blob_service().list_containers():
                    yield {
                        **_folder_entry(c.name),
                        "last_modified": c.last_modified,
                        "etag": c.etag,
                    }
            return

        if await self._hns(container):
            listing = self._ils_dfs(container, key)
        else:
            listing = self._ils_blob(container, key)

        listed = False
        try:
            async for entry in listing:
                listed = True
                yield entry
        except AzureError as e:
            if listed:
                raise os_error(e, path) from e
            # A file lists as itself, as fsspec's _ls lists it.
            entry = await self._info(path)
            if entry["type"] == "directory":
                raise os_error(e, path) from e
            yield entry
            return

        if not listed:
            # Nothing listed: an empty folder, a file, or nothing at all.
            entry = await self._info(path)
            if entry["type"] != "directory":
                yield entry

    async def _ils_dfs(self, container: str, key: str) -> AsyncIterator[dict[str, Any]]:
        """One folder's paths, from the Data Lake API, as they come a page at a time."""
        file_system = self._dfs_service().get_file_system_client(container)
        async for p in file_system.get_paths(path=key or None, recursive=False):
            name = f"{container}/{p.name}"
            if p.is_directory:
                yield {
                    **_folder_entry(name),
                    "last_modified": p.last_modified,
                    "etag": p.etag,
                }
            else:
                yield {
                    "name": name,
                    "size": p.content_length,
                    "type": "file",
                    "last_modified": p.last_modified,
                    "etag": p.etag,
                }

    async def _ils_blob(
        self, container: str, key: str
    ) -> AsyncIterator[dict[str, Any]]:
        """One folder's blobs and prefixes, from the blob API, as they come a page at a time.

        On an account with a hierarchical namespace, or with marker blobs, a
        folder comes twice: as a blob with ``hdi_isfolder``, and as a prefix
        if it holds anything. Only its first is yielded: the names of the
        folders yielded are kept, those of the files are not.
        """
        prefix = f"{key}/" if key else ""
        container_client = self._blob_service().get_container_client(container)
        folders: set[str] = set()
        async for item in container_client.walk_blobs(
            name_starts_with=prefix, delimiter="/", include=["metadata"]
        ):
            if item.name == prefix:
                continue  # The folder's own marker blob.
            if item.name.endswith("/"):  # A prefix.
                entry = _folder_entry(f"{container}/{item.name}")
            else:
                entry = _blob_entry(container, item)
            if entry["type"] == "directory":
                if entry["name"] in folders:
                    continue
                folders.add(entry["name"])
            yield entry

    # Changing paths.

    async def _rm_file(self, path: str, **kwargs: Any) -> None:
        path = self._strip_protocol(path)
        container, key = split_path(path)
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
        path = self._strip_protocol(path)
        container, key = split_path(path)
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
            refused_by_service = self._is_hns_enabled and key
            if not refused_by_service and await self._has_children(container, key):
                raise OSError(errno.ENOTEMPTY, "Directory not empty", path)
            if not key:
                await self._blob_service().delete_container(container)
                return
            try:
                await self._blob(container, key).delete_blob()
            except ResourceNotFoundError:
                if self._is_hns_enabled:
                    raise
                # A folder with no blob of its own is gone with its last blob.

    async def _makedirs(self, path: str, exist_ok: bool = False) -> None:
        path = self._strip_protocol(path)
        container, key = split_path(path)
        if not container:
            if exist_ok:
                return
            raise FileExistsError(errno.EEXIST, "The root exists", path)

        try:
            info = await self._info(path)
        except FileNotFoundError:
            pass
        else:
            if info["type"] != "directory":
                raise FileExistsError(errno.EEXIST, "A file exists", path)
            if not exist_ok:
                raise FileExistsError(errno.EEXIST, "File exists", path)
            return

        blob_service, dfs_service = self._blob_service(), self._dfs_service()
        with azure_errors(path):
            with contextlib.suppress(ResourceExistsError):
                await blob_service.create_container(container)
            if not key:
                return

            hns = await self._hns(container)
            if hns:
                await dfs_service.get_directory_client(
                    container, key
                ).create_directory()
            elif hns is None:
                # A blob marking a folder: a real folder on an account with a
                # hierarchical namespace, a marker blob on one without, which
                # the listings take as a folder.
                await self._blob(container, key).upload_blob(
                    b"", metadata={"hdi_isfolder": "true"}, overwrite=True
                )
            # Without a hierarchical namespace, a folder is made by its files.

    async def _mkdir(
        self, path: str, create_parents: bool = True, **kwargs: Any
    ) -> None:
        path = self._strip_protocol(path)
        if not create_parents:
            parent = self._parent(path)
            if parent and not await self._isdir(parent):
                raise FileNotFoundError(errno.ENOENT, "No parent folder", parent)
        await self._makedirs(path, exist_ok=False)

    async def _cp_file(self, path1: str, path2: str, **kwargs: Any) -> None:
        """Copy a blob on the server, waiting for the copy if it is pending.

        Works with or without a hierarchical namespace. The source's URL is
        its client's, carrying a SAS token when the credential is one.
        """
        path1, path2 = self._strip_protocol(path1), self._strip_protocol(path2)
        c1, k1 = split_path(path1)
        c2, k2 = split_path(path2)
        if not k1 or not k2:
            raise IsADirectoryError(errno.EISDIR, "Is a container", path1)

        source, destination = self._blob(c1, k1), self._blob(c2, k2)
        try:
            copy = await destination.start_copy_from_url(source.url)
            status = copy.get("copy_status")
            delay = self.copy_poll_interval
            while status == "pending":
                await asyncio.sleep(delay)
                delay = min(delay * 2, 5.0)
                props = await destination.get_blob_properties()
                status = props.copy.status
        except ResourceNotFoundError as e:
            # The source missing, by the service's code: else the destination's container.
            missing = path2 if e.error_code == "ContainerNotFound" else path1
            raise os_error(e, missing) from e
        except AzureError as e:
            raise os_error(e, path2) from e
        if status != "success":
            raise OSError(errno.EIO, f"Copy from {path1} ended as {status}", path2)

    async def _mv_file(self, path1: str, path2: str, **kwargs: Any) -> None:
        """Move a file, or a folder with all in it: a rename with a hierarchical namespace.

        A rename is atomic, but only within a container: a move between two,
        or on an account without a hierarchical namespace, copies on the
        server, then deletes.
        """
        path1, path2 = self._strip_protocol(path1), self._strip_protocol(path2)
        c1, k1 = split_path(path1)
        c2, k2 = split_path(path2)
        if not (await self._hns(c1)) or c1 != c2 or not k1 or not k2:
            await super()._mv_file(path1, path2)
            return

        source = self._dfs_service().get_file_client(c1, k1)
        with azure_errors(path1):
            try:
                await source.rename_file(f"{c2}/{k2}")
            except ResourceNotFoundError as e:
                # A rename creates no parent, where a write does.
                if e.error_code != "RenameDestinationParentPathNotFound":
                    raise
                await self._makedirs(self._parent(path2), exist_ok=True)
                await source.rename_file(f"{c2}/{k2}")

    # Files.

    async def _open_file(self, path: str, mode: str, **kwargs: Any) -> Any:
        """Open an adaptive file on the running loop."""
        if mode not in ("rb", "wb", "xb"):
            raise UnsupportedOperation(f"Unsupported mode {mode!r}: rb, wb or xb")
        path = self._strip_protocol(path)
        container, key = split_path(path)
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
        return AbfsWritableFile(
            path,
            loop,
            blob=blob,
            exclusive=mode == "xb",
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
        # In a transaction, the block list is committed on commit: no other
        # client sees the file before.
        return TransactionFile(AdaptiveWritableTransaction(f))


class AbfsReadableFile(AdaptiveReadableFile):
    """A blob read by ranges, all of one version: its ETag, by If-Match."""

    min_block_size: int = _MiB // 2
    max_block_size: int = 8 * _MiB

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
                "etag": self.version,
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
        self._check_version(downloader.properties.etag)
        return data


class AbfsWritableFile(AdaptiveWritableFile):
    """A block blob, uploaded whole, or in blocks committed on closing.

    Blocks staged but never committed take no visible place: the service
    drops them after a week. So an abort sends nothing, leaving any file
    already at the path as it was.
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
        exclusive: bool = False,
        **kwargs: Any,
    ) -> None:
        super().__init__(path, loop, **kwargs)
        self._blob_client = blob
        self.exclusive = exclusive
        self._upload_id = ""

    def _conditions(self) -> dict[str, Any]:
        """Commit only if no blob exists, for mode "xb"."""
        if not self.exclusive:
            return {}
        # The SDK sends If-None-Match: * itself. An etag given too would not be
        # taken, but passed on to the HTTP session, which refuses it.
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
