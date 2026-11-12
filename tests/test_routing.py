"""Tests of `AbfsFileSystems`: Hadoop URLs routed to each account's file system, and copies between accounts.

The dummy server serves one store, whatever the account: the accounts ``a`` and ``b`` here are two file systems of
it, so that a copy between them goes as between two accounts.
"""

from __future__ import annotations

import asyncio
import contextlib
import importlib
import os
import uuid
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from typing import Any
from urllib.parse import unquote

import pytest
from azure.core.credentials import AccessToken
from azure.core.credentials_async import AsyncTokenCredential
from azure.storage.blob import UserDelegationKey

from fsspec_abfs._paths import hadoop_url, parse_account_url
from fsspec_abfs.abfs import AbfsFileSystem
from fsspec_abfs.routing import AbfsFileSystems

_SAS = "sv=2025-01-05&ss=b&srt=sco&sp=rwdl&sig=c2ln"
"""A SAS token in its form, which the dummy takes without checking it."""

_KEY = "a" * 86 + "=="
"""An account key in its form: 64 bytes in base64."""


def url(account: str, path: str) -> str:
    """The Hadoop URL of a path of the dummy's container ``c``, or of ``c2``, in ``account``."""
    return f"abfss://{path.partition('/')[0]}@{account}.dfs.core.windows.net/{path.partition('/')[2]}".rstrip(
        "/"
    )


class _Token(AsyncTokenCredential):
    """A token credential asking nothing, for checks that send no request."""

    async def get_token(self, *scopes: str, **kwargs: Any) -> AccessToken:
        return AccessToken("token", 2**31)

    async def close(self) -> None:
        pass


@pytest.fixture
async def make_router(dummy, dummy_endpoints, hns):
    """Make an `AbfsFileSystems` whose accounts ``a`` and ``b`` are the dummy's, anonymous unless told."""
    dummy.create_container("c2")
    made: list[AbfsFileSystems] = []

    def make(
        credentials: dict[str, Any] | None = None, **kwargs: Any
    ) -> AbfsFileSystems:
        credentials = {"a": "anonymous", "b": "anonymous", **(credentials or {})}
        kwargs.setdefault(
            "accounts",
            [
                {"account_name": name, "credential": credential, "is_hns_enabled": hns}
                for name, credential in credentials.items()
            ],
        )
        # A default credential asking nothing: each account here has its own.
        kwargs.setdefault("credential", _Token())
        router = AbfsFileSystems(asynchronous=True, skip_instance_cache=True, **kwargs)
        for name in credentials:
            # The clients are made on these at first use: the dummy's, then.
            inner = router.filesystem(name)
            inner._dfs_url, inner._blob_url = dummy_endpoints
        made.append(router)
        return router

    try:
        yield make
    finally:
        loop = asyncio.get_running_loop()
        for router in made:
            for inner in router._backends.values():
                # close() only schedules the closing of clients on the running loop: they are closed here.
                if inner._client_loop is loop:
                    for client in (
                        inner._blob_service_client,
                        inner._dfs_service_client,
                    ):
                        await client.close()
                    inner._blob_service_client = inner._dfs_service_client = None
                    inner._client_loop = None
            router.close()


# Paths.


async def test_names_are_hadoop_urls(make_router) -> None:
    fs = make_router()
    await fs._pipe_file(url("a", "c/work/x.txt"), b"x")

    listed = await fs._ls(url("a", "c/work"), detail=False)
    assert listed == [url("a", "c/work/x.txt")]
    assert (await fs._info(url("a", "c/work/x.txt")))["name"] == url(
        "a", "c/work/x.txt"
    )
    assert await fs._find(url("a", "c/work")) == [url("a", "c/work/x.txt")]
    assert await fs._cat_file(url("a", "c/work/x.txt")) == b"x"


def test_strip_protocol_keeps_the_url() -> None:
    # abfs:// is taken, and answered as abfss://, in lower case, without a trailing "/".
    assert (
        AbfsFileSystems._strip_protocol(
            "abfs://Data@MyAccount.dfs.core.windows.net/a/b/"
        )
        == "abfss://data@myaccount.dfs.core.windows.net/a/b"
    )
    assert (
        AbfsFileSystems._strip_protocol("abfss://data@onelake.dfs.fabric.microsoft.com")
        == "abfss://data@onelake.dfs.fabric.microsoft.com"
    )


@pytest.mark.parametrize(
    "path",
    [
        "/c/work/x.txt",
        "c/work/x.txt",
        "abfs://c/work/x.txt",
        "az://c/work/x.txt",
        "https://a.dfs.core.windows.net/c/work/x.txt",
        "c@a.dfs.core.windows.net/work/x.txt",
    ],
)
def test_refuses_any_other_path(path: str) -> None:
    with pytest.raises(ValueError, match="Not a Hadoop URL"):
        AbfsFileSystems._strip_protocol(path)


def test_unstrip_protocol() -> None:
    fs = AbfsFileSystems(skip_instance_cache=True)
    assert (
        fs.unstrip_protocol("abfs://c@a.dfs.core.windows.net/x")
        == "abfss://c@a.dfs.core.windows.net/x"
    )


# The accounts' file systems.


@pytest.mark.parametrize("credential", ["anonymous", _SAS, _KEY])
def test_the_default_credential_is_a_token_credential(credential: str) -> None:
    with pytest.raises(ValueError, match="Microsoft Entra ID"):
        AbfsFileSystems(credential=credential, skip_instance_cache=True)


def test_none_is_refused_as_a_credential() -> None:
    with pytest.raises(TypeError, match="anonymous"):
        AbfsFileSystems(credential=None, skip_instance_cache=True)


def test_an_account_takes_its_own_arguments() -> None:
    token = _Token()
    fs = AbfsFileSystems(
        credential=token,
        operation_concurrency=7,
        io_concurrency=5,
        accounts=[
            {
                "account_name": "opendata",
                "credential": "anonymous",
                "io_concurrency": 3,
            },
            {
                "account_url": "https://fab.dfs.fabric.microsoft.com/",
                "is_hns_enabled": True,
            },
        ],
        skip_instance_cache=True,
    )
    opendata = fs.filesystem("opendata")
    assert opendata.credential == "anonymous"
    assert (opendata.operation_concurrency, opendata.io_concurrency) == (7, 3)

    fab = fs.filesystem("fab", "fabric.microsoft.com")
    assert fab.credential is token
    assert fab.is_hns_enabled is True

    # Any other account takes the defaults: the shared credential object, and the concurrency.
    other = fs.filesystem("other")
    assert other.credential is token
    assert (other.operation_concurrency, other.io_concurrency) == (7, 5)
    assert fs.filesystem("other") is other


def test_a_credential_named_takes_its_options() -> None:
    fs = AbfsFileSystems(
        credential="azure-cli", tenant_id="t", skip_instance_cache=True
    )
    # Kept as given, and made at first need.
    assert fs.credential == "azure-cli"
    assert fs.credential_options["tenant_id"] == "t"
    assert fs._AbfsFileSystems__credential is None
    credential = fs._get_credential().value
    assert credential.tenant_id == "t"
    # Made once by the router, shared by the accounts, which do not close it.
    for name in ("one", "two"):
        inner = fs.filesystem(name)
        assert inner.credential is credential
        assert inner._get_credential().value is credential
        assert not inner._owns_credential()
    asyncio.run(credential.close())


def test_a_credential_object_is_the_callers() -> None:
    token = _Token()
    fs = AbfsFileSystems(credential=token, skip_instance_cache=True)
    assert fs.credential is token
    assert fs._get_credential().value is token
    inner = fs.filesystem("one")
    assert not inner._owns_credential()


def test_a_closed_router_refuses_calls() -> None:
    fs = AbfsFileSystems(credential=_Token(), skip_instance_cache=True)
    fs.close()
    with pytest.raises(RuntimeError, match="closed"):
        fs.filesystem("myaccount")


@pytest.mark.parametrize(
    ("accounts", "match"),
    [
        (
            [{"account_name": "a"}, {"account_url": "https://a.blob.core.windows.net"}],
            "given twice",
        ),
        ([{"is_hns_enabled": True}], "account is required"),
        (
            [{"account_name": "a", "account_url": "https://a.dfs.core.windows.net"}],
            "not both",
        ),
        ([{"account_name": "Not_An_Account"}], "Not an account name"),
    ],
)
def test_accounts_are_checked(accounts: list[dict[str, Any]], match: str) -> None:
    with pytest.raises(ValueError, match=match):
        AbfsFileSystems(accounts=accounts, skip_instance_cache=True)


def test_cross_account_copy_is_checked() -> None:
    with pytest.raises(ValueError, match="cross_account_copy"):
        AbfsFileSystems(cross_account_copy="fast", skip_instance_cache=True)


def test_no_listings_cache() -> None:
    assert (
        AbfsFileSystems(skip_instance_cache=True).dircache.use_listings_cache is False
    )


# Copies.


async def test_copy_within_an_account(dummy, make_router) -> None:
    fs = make_router()
    await fs._pipe_file(url("a", "c/x.txt"), b"x")
    dummy.requests.clear()
    await fs._cp_file(url("a", "c/x.txt"), url("a", "c2/x.txt"))
    # The account's own copy, on the server.
    assert dummy.count("CopyBlob") == 1
    assert dummy.files("c2") == {"x.txt": b"x"}


@pytest.mark.parametrize("recursive", [False, True])
async def test_copy_between_accounts_streams(
    dummy, make_router, recursive: bool
) -> None:
    fs = make_router()
    await fs._pipe({url("a", "c/d/x.txt"): b"x", url("a", "c/d/e/y.txt"): b"y"})
    dummy.requests.clear()
    if recursive:
        await fs._copy(url("a", "c/d"), url("b", "c2/d"), recursive=True)
        assert dummy.files("c2") == {"d/x.txt": b"x", "d/e/y.txt": b"y"}
    else:
        await fs._cp_file(url("a", "c/d/x.txt"), url("b", "c2/x.txt"))
        assert dummy.files("c2") == {"x.txt": b"x"}
    # Read, then written: no copy on the server.
    assert dummy.count("CopyBlob") == 0
    assert dummy.count("GetBlob") >= 1


@pytest.mark.parametrize("source", ["anonymous", "sas"])
async def test_copy_between_accounts_on_the_server(
    dummy, make_router, source: str
) -> None:
    credentials = {"a": _SAS} if source == "sas" else {}
    fs = make_router(credentials, cross_account_copy="server-side")
    await fs._pipe({url("a", "c/d/x.txt"): b"x", url("a", "c/d/e/y.txt"): b"y"})
    dummy.requests.clear()
    await fs._copy(url("a", "c/d"), url("b", "c2/d"), recursive=True)
    # Each file copied by the destination from the source's URL: no data through this process.
    assert dummy.count("CopyBlob") == 2
    assert dummy.count("GetBlob") == 0
    assert dummy.files("c2") == {"d/x.txt": b"x", "d/e/y.txt": b"y"}


async def test_move_between_accounts(dummy, make_router) -> None:
    fs = make_router(cross_account_copy="server-side")
    await fs._pipe_file(url("a", "c/x.txt"), b"x")
    await fs._mv(url("a", "c/x.txt"), url("b", "c2/x.txt"))
    assert dummy.files("c2") == {"x.txt": b"x"}
    assert "x.txt" not in dummy.files("c")


# The source's URL of a copy on the server, signed by its account's file system.


def _delegation_key(expiry: datetime) -> UserDelegationKey:
    key = UserDelegationKey()
    key.signed_oid = "00000000-0000-0000-0000-000000000001"
    key.signed_tid = "00000000-0000-0000-0000-000000000002"
    key.signed_start = (expiry - timedelta(days=1)).strftime("%Y-%m-%dT%H:%M:%SZ")
    key.signed_expiry = expiry.strftime("%Y-%m-%dT%H:%M:%SZ")
    key.signed_service = "b"
    key.signed_version = "2025-01-05"
    key.value = "a2V5"
    return key


def _expiry(url: str) -> datetime:
    """The expiry a signed URL's SAS names, its ``se``."""
    query = dict(part.split("=", 1) for part in url.partition("?")[2].split("&"))
    return datetime.fromisoformat(unquote(query["se"]))


async def test_sign(make_router) -> None:
    fs = make_router({"a": _SAS, "b": _KEY})
    # The account's own SAS, in its client's URL.
    assert (await fs._sign(url("a", "c/x.txt"))).endswith(f"/c/x.txt?{_SAS}")

    # A SAS signed with the account key, reading the blob for the seconds asked.
    signed = await fs._sign(url("b", "c/x.txt"), expiration=3600)
    assert "sp=r&" in signed
    assert "sig=" in signed
    later = datetime.now(UTC) + timedelta(hours=1)
    assert abs(_expiry(signed) - later) < timedelta(seconds=5)


async def test_sign_with_a_user_delegation_key() -> None:
    fs = _keyed()
    # By the key kept: no request.
    fs._AbfsFileSystem__user_delegation_key = _delegation_key(
        datetime.now(UTC) + timedelta(days=1)
    )
    signed = await fs._sign("/c/x.txt", expiration=600)
    assert "skoid=" in signed
    assert "sp=r&" in signed
    assert fs.asked == []
    # A SAS expires with its key at the latest.
    signed = await fs._sign("/c/x.txt", expiration=7 * 24 * 3600)
    assert abs(_expiry(signed) - (datetime.now(UTC) + timedelta(days=1))) < timedelta(
        seconds=5
    )


async def test_sign_anonymous(make_router) -> None:
    fs = make_router()
    assert "?" not in await fs._sign(url("a", "c/x.txt"))


async def test_sign_a_container_is_refused(make_router) -> None:
    fs = make_router()
    with pytest.raises(IsADirectoryError):
        await fs._sign(url("a", "c"))


def test_parent() -> None:
    assert AbfsFileSystems._parent(url("a", "c/d/x.txt")) == url("a", "c/d")
    assert AbfsFileSystems._parent(url("a", "c/x.txt")) == url("a", "c")
    # A container's root is its own parent: nothing is above it.
    assert AbfsFileSystems._parent(url("a", "c")) == url("a", "c")


async def test_etags_are_bare(make_router) -> None:
    fs = make_router()
    await fs._pipe_file(url("a", "c/d/x.txt"), b"x")
    listed = (await fs._ls(url("a", "c/d")))[0]["etag"]
    found = (await fs._find(url("a", "c"), detail=True))[url("a", "c/d/x.txt")]["etag"]
    info = (await fs._info(url("a", "c/d/x.txt")))["etag"]
    assert listed == found == info
    assert not info.startswith('"')
    # A file opened reads by its bare ETag, quoted for If-Match.
    async with await fs.open_async(url("a", "c/d/x.txt")) as f:
        assert await f.read() == b"x"


def test_sync_router(dummy, dummy_endpoints, hns) -> None:
    # A sync router's calls run the accounts' sync methods, e.g. open: on its loop.
    dummy.create_container("c2")
    fs = AbfsFileSystems(
        credential=_Token(),
        accounts=[
            {"account_name": "a", "credential": "anonymous", "is_hns_enabled": hns},
            {"account_name": "b", "credential": _SAS, "is_hns_enabled": hns},
        ],
        cross_account_copy="server-side",
        skip_instance_cache=True,
    )
    for name in ("a", "b"):
        inner = fs.filesystem(name)
        inner._dfs_url, inner._blob_url = dummy_endpoints
    try:
        with fs.open(url("a", "c/x.txt"), "wb") as f:
            f.write(b"x")
        with fs.open(url("a", "c/x.txt"), "rb") as f:
            assert f.read() == b"x"
        fs.copy(url("a", "c/x.txt"), url("b", "c2/x.txt"))
        assert fs.cat_file(url("b", "c2/x.txt")) == b"x"
        assert fs.sign(url("b", "c2/x.txt")).endswith(f"?{_SAS}")
    finally:
        fs.close()


@pytest.mark.parametrize("cross_account", [False, True])
@pytest.mark.parametrize("adlfs_protocols", [False, True])
def test_use(cross_account: bool, adlfs_protocols: bool) -> None:
    import fsspec  # noqa: PLC0415

    import fsspec_abfs  # noqa: PLC0415

    try:
        fsspec_abfs.use(cross_account=cross_account, adlfs_protocols=adlfs_protocols)
        expected = AbfsFileSystems if cross_account else AbfsFileSystem
        for protocol in ("abfs", "abfss"):
            assert fsspec.get_filesystem_class(protocol) is expected
        if adlfs_protocols:
            assert fsspec.get_filesystem_class("az") is expected
    finally:
        fsspec_abfs.use()  # As the entry points register it.
        if adlfs_protocols:
            # A class registered is kept apart from those fsspec knows by name:
            # forgetting it gives "az" back to adlfs.
            importlib.import_module("fsspec.registry")._registry.pop("az", None)


def test_az_urls_naming_their_account() -> None:
    # adlfs' protocol, in Hadoop's form, naming its account: answered as abfss://.
    assert (
        AbfsFileSystems._strip_protocol("az://c@a.dfs.core.windows.net/x")
        == "abfss://c@a.dfs.core.windows.net/x"
    )


# The user delegation key.


def _keyed(credential: Any = None) -> AbfsFileSystem:
    """An account's file system whose blob service answers user delegation keys, counting its requests in ``asked``."""
    fs = AbfsFileSystem(
        account_name="a",
        credential=credential or _Token(),
        asynchronous=True,
        skip_instance_cache=True,
    )
    fs.asked = []

    async def ask(
        key_start_time: datetime, key_expiry_time: datetime
    ) -> UserDelegationKey:
        await asyncio.sleep(0.01)
        fs.asked.append((key_start_time, key_expiry_time))
        return _delegation_key(key_expiry_time)

    service = fs._blob_service
    # The blob clients are the real ones, which name the blob's URL without a request.
    fs._blob_service = lambda: SimpleNamespace(
        get_user_delegation_key=ask, get_blob_client=service().get_blob_client
    )
    return fs


async def test_user_delegation_key_is_kept() -> None:
    fs = _keyed()
    keys = await asyncio.gather(*(fs._get_user_delegation_key() for _ in range(5)))
    # Concurrent callers wait for one request, and later ones take the key kept.
    assert len(fs.asked) == 1
    assert all(k is keys[0] for k in keys)
    assert await fs._get_user_delegation_key() is keys[0]
    start, expiry = fs.asked[0]
    assert expiry - start == fs._user_delegation_key_lifetime


async def test_user_delegation_key_is_renewed_near_its_expiry() -> None:
    fs = _keyed()
    fs._AbfsFileSystem__user_delegation_key = _delegation_key(
        datetime.now(UTC) + timedelta(hours=1)
    )
    await fs._get_user_delegation_key()
    assert len(fs.asked) == 1


async def test_user_delegation_key_needs_a_token_credential() -> None:
    with pytest.raises(ValueError, match="token credential"):
        await _keyed(_SAS)._get_user_delegation_key()


# Live accounts.


@pytest.mark.live_only
@pytest.mark.parametrize("mode", ["streaming", "server-side"])
async def test_copy_between_live_accounts(api: str, mode: str) -> None:
    # From the account with a hierarchical namespace to the one without, both
    # by Microsoft Entra ID. On the server, the identity asks the source account
    # for a user delegation key, which needs a role at its level.
    from azure.identity.aio import DefaultAzureCredential  # noqa: PLC0415

    hns = os.environ.get("FSSPEC_ABFS_TEST_HNS_ACCOUNT_URL")
    flat = os.environ.get("FSSPEC_ABFS_TEST_FLAT_ACCOUNT_URL")
    container = os.environ.get("FSSPEC_ABFS_TEST_CONTAINER")
    if not (hns and flat and container):
        pytest.skip("A copy between accounts needs both live accounts")

    folder = f"fsspec-abfs-test-{uuid.uuid4().hex[:8]}/router-{mode}"
    source, destination = (
        hadoop_url(container, folder, *parse_account_url(account))
        for account in (hns, flat)
    )
    credential = DefaultAzureCredential()
    fs = AbfsFileSystems(
        credential=credential,
        cross_account_copy=mode,
        asynchronous=True,
        skip_instance_cache=True,
    )
    try:
        await fs._pipe(
            {f"{source}/d/x.txt": b"x", f"{source}/d/e/y.txt": b"y" * 100_000}
        )
        await fs._copy(f"{source}/d", f"{destination}/d", recursive=True)
        assert await fs._find(destination) == [
            f"{destination}/d/e/y.txt",
            f"{destination}/d/x.txt",
        ]
        assert await fs._cat_file(f"{destination}/d/e/y.txt") == b"y" * 100_000

        await fs._mv(f"{source}/d/x.txt", f"{destination}/moved.txt")
        assert await fs._cat_file(f"{destination}/moved.txt") == b"x"
        assert not await fs._exists(f"{source}/d/x.txt")
    finally:
        for path in (source, destination):
            with contextlib.suppress(FileNotFoundError):
                await fs._rm(path, recursive=True)
        loop = asyncio.get_running_loop()
        for inner in fs._backends.values():
            # close() only schedules the closing of clients on the running loop: they are closed here.
            if inner._client_loop is loop:
                for client in (inner._blob_service_client, inner._dfs_service_client):
                    await client.close()
                inner._blob_service_client = inner._dfs_service_client = None
                inner._client_loop = None
        fs.close()
        await credential.close()
