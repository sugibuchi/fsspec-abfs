"""An in-memory stand-in for the Azure SDK's async clients, as far as AbfsFileSystem calls them.

It models the blob API of an account without a hierarchical namespace:
folders are prefixes, and marker blobs with ``hdi_isfolder`` metadata. The
Data Lake API is modelled only as far as the probe of the hierarchical
namespace goes. Every call is counted in ``FakeAccount.calls``, by name.
"""

from __future__ import annotations

import itertools
from collections import Counter
from datetime import UTC, datetime
from types import SimpleNamespace
from typing import Any

import pytest
from azure.core import MatchConditions
from azure.core.exceptions import (
    HttpResponseError,
    ResourceExistsError,
    ResourceModifiedError,
    ResourceNotFoundError,
)

from fsspec_abfs.abfs import AbfsFileSystem

_NOW = datetime(2026, 1, 1, tzinfo=UTC)


def azure_error(cls: type[HttpResponseError], code: str, status: int) -> Exception:
    e = cls(message=f"{code} at https://x.blob.core.windows.net/c?sig=SECRET")
    e.error_code = code
    e.status_code = status
    return e


class FakeAccount:
    """The blobs of an account, by container, and the calls made."""

    def __init__(self) -> None:
        self.containers: dict[str, dict[str, dict[str, Any]]] = {}
        self.calls: Counter[str] = Counter()
        self.staged: dict[str, bytes] = {}
        self._etags = itertools.count(1)
        # What the probes answer: a value, or an exception raised.
        self.account_information: Any = {"is_hns_enabled": False}
        self.access_control: Any = azure_error(
            HttpResponseError, "HierarchicalNamespaceNotEnabled", 400
        )

    def put(self, container: str, name: str, data: bytes = b"", **metadata: str):
        self.containers.setdefault(container, {})[name] = {
            "data": data,
            "metadata": metadata,
            "etag": f'"{next(self._etags)}"',
        }

    def blobs(self, container: str) -> dict[str, dict[str, Any]]:
        if container not in self.containers:
            raise azure_error(ResourceNotFoundError, "ContainerNotFound", 404)
        return self.containers[container]


class _Paged:
    """An async iterator of items, as the SDK's pagers are."""

    def __init__(self, items: Any) -> None:
        self._items = iter(items)

    def __aiter__(self) -> _Paged:
        return self

    async def __anext__(self) -> Any:
        try:
            return next(self._items)
        except StopIteration:
            raise StopAsyncIteration from None


def _props(name: str, blob: dict[str, Any]) -> SimpleNamespace:
    return SimpleNamespace(
        name=name,
        size=len(blob["data"]),
        etag=blob["etag"],
        metadata=dict(blob["metadata"]),
        last_modified=_NOW,
        content_settings=SimpleNamespace(content_type="application/octet-stream"),
        copy=SimpleNamespace(status="success"),
    )


class FakeBlobClient:
    def __init__(self, account: FakeAccount, container: str, name: str) -> None:
        self.account, self.container, self.name = account, container, name
        self.url = f"https://acc.blob.core.windows.net/{container}/{name}"

    def _get(self) -> dict[str, Any]:
        blobs = self.account.blobs(self.container)
        if self.name not in blobs:
            raise azure_error(ResourceNotFoundError, "BlobNotFound", 404)
        return blobs[self.name]

    async def get_blob_properties(self) -> SimpleNamespace:
        self.account.calls["get_blob_properties"] += 1
        return _props(self.name, self._get())

    async def delete_blob(self) -> None:
        self.account.calls["delete_blob"] += 1
        self._get()
        del self.account.containers[self.container][self.name]

    async def upload_blob(
        self, data: bytes, overwrite: bool = False, metadata: Any = None
    ) -> None:
        self.account.calls["upload_blob"] += 1
        blobs = self.account.blobs(self.container)
        if not overwrite and self.name in blobs:
            raise azure_error(ResourceExistsError, "BlobAlreadyExists", 409)
        self.account.put(self.container, self.name, bytes(data), **(metadata or {}))

    async def stage_block(self, block_id: str, data: bytes, length: int) -> None:
        self.account.calls["stage_block"] += 1
        self.account.staged[f"{self.container}/{self.name}/{block_id}"] = bytes(data)

    async def commit_block_list(
        self, blocks: list[Any], etag: str | None = None, match_condition: Any = None
    ) -> None:
        self.account.calls["commit_block_list"] += 1
        blobs = self.account.blobs(self.container)
        if match_condition == MatchConditions.IfMissing and self.name in blobs:
            raise azure_error(ResourceModifiedError, "ConditionNotMet", 412)
        prefix = f"{self.container}/{self.name}/"
        data = b"".join(self.account.staged.pop(prefix + b.id) for b in blocks)
        self.account.put(self.container, self.name, data)

    async def download_blob(
        self,
        offset: int,
        length: int,
        etag: str | None = None,
        match_condition: Any = None,
    ) -> SimpleNamespace:
        self.account.calls["download_blob"] += 1
        blob = self._get()
        if etag is not None and etag != blob["etag"]:
            raise azure_error(ResourceModifiedError, "ConditionNotMet", 412)
        data = blob["data"][offset : offset + length]

        async def readall() -> bytes:
            return data

        return SimpleNamespace(
            readall=readall, properties=SimpleNamespace(etag=blob["etag"])
        )

    async def start_copy_from_url(self, url: str) -> dict[str, str]:
        self.account.calls["start_copy_from_url"] += 1
        container, _, name = url.removeprefix(
            "https://acc.blob.core.windows.net/"
        ).partition("/")
        source = self.account.blobs(container).get(name)
        if source is None:
            raise azure_error(ResourceNotFoundError, "CannotVerifyCopySource", 404)
        self.account.blobs(self.container)
        self.account.put(self.container, self.name, source["data"])
        return {"copy_status": "success"}


class FakeContainerClient:
    def __init__(self, account: FakeAccount, container: str) -> None:
        self.account, self.container = account, container

    def get_blob_client(self, name: str) -> FakeBlobClient:
        return FakeBlobClient(self.account, self.container, name)

    async def get_container_properties(self) -> SimpleNamespace:
        self.account.calls["get_container_properties"] += 1
        self.account.blobs(self.container)
        return SimpleNamespace(last_modified=_NOW, etag='"c"')

    def list_blobs(self, name_starts_with: str = "", results_per_page: int = 0):
        self.account.calls["list_blobs"] += 1
        blobs = self.account.blobs(self.container)
        return _Paged(
            _props(n, blobs[n]) for n in sorted(blobs) if n.startswith(name_starts_with)
        )

    def walk_blobs(
        self, name_starts_with: str = "", delimiter: str = "/", include: Any = None
    ):
        self.account.calls["walk_blobs"] += 1
        blobs = self.account.blobs(self.container)
        items: dict[str, Any] = {}
        for name in sorted(blobs):
            if not name.startswith(name_starts_with):
                continue
            rest = name[len(name_starts_with) :]
            if delimiter in rest:
                prefix = name_starts_with + rest.split(delimiter)[0] + delimiter
                items.setdefault(prefix, SimpleNamespace(name=prefix))
            else:
                items[name] = _props(name, blobs[name])
        return _Paged(items[k] for k in sorted(items))


class FakeBlobService:
    def __init__(self, account: FakeAccount) -> None:
        self.account = account
        self.closed = False

    async def get_account_information(self) -> dict[str, Any]:
        self.account.calls["get_account_information"] += 1
        if isinstance(self.account.account_information, Exception):
            raise self.account.account_information
        return self.account.account_information

    def list_containers(self):
        self.account.calls["list_containers"] += 1
        return _Paged(
            SimpleNamespace(name=c, last_modified=_NOW, etag='"c"')
            for c in sorted(self.account.containers)
        )

    def get_container_client(self, container: str) -> FakeContainerClient:
        return FakeContainerClient(self.account, container)

    def get_blob_client(self, container: str, name: str) -> FakeBlobClient:
        return FakeBlobClient(self.account, container, name)

    async def create_container(self, container: str) -> None:
        self.account.calls["create_container"] += 1
        if container in self.account.containers:
            raise azure_error(ResourceExistsError, "ContainerAlreadyExists", 409)
        self.account.containers[container] = {}

    async def delete_container(self, container: str) -> None:
        self.account.calls["delete_container"] += 1
        self.account.blobs(container)
        del self.account.containers[container]

    async def close(self) -> None:
        self.closed = True


class FakeDataLakeService:
    """Only the root ACL of a container, which the HNS probe asks."""

    def __init__(self, account: FakeAccount) -> None:
        self.account = account
        self.closed = False

    def get_file_system_client(self, container: str) -> Any:
        account = self.account

        async def get_access_control() -> dict[str, str]:
            account.calls["get_access_control"] += 1
            if isinstance(account.access_control, Exception):
                raise account.access_control
            return account.access_control

        directory = SimpleNamespace(get_access_control=get_access_control)
        return SimpleNamespace(get_directory_client=lambda path: directory)

    async def close(self) -> None:
        self.closed = True


@pytest.fixture
def account() -> FakeAccount:
    return FakeAccount()


@pytest.fixture
def make_fs(account: FakeAccount, monkeypatch: pytest.MonkeyPatch):
    """Make an AbfsFileSystem on the fake account, async, uncached."""

    def make_clients(self: AbfsFileSystem) -> tuple[Any, Any]:
        return FakeBlobService(account), FakeDataLakeService(account)

    monkeypatch.setattr(AbfsFileSystem, "_make_clients", make_clients)

    def make(**kwargs: Any) -> AbfsFileSystem:
        kwargs.setdefault("is_hns_enabled", False)
        return AbfsFileSystem(
            "https://acc.dfs.core.windows.net/",
            asynchronous=True,
            skip_instance_cache=True,
            **kwargs,
        )

    return make


def pytest_configure() -> None:
    # The live tests' settings, from a .env file if any.
    try:
        from dotenv import load_dotenv  # noqa: PLC0415
    except ImportError:
        return
    load_dotenv()


@pytest.fixture(name="azure_error")
def azure_error_fixture():
    """`azure_error`, for tests, which never import conftest."""
    return azure_error
