"""Tests of `AbfsFileSystems`: Hadoop URLs routed to each account's file system, and copies between accounts.

The dummy server serves one store, whatever the account: the accounts ``a`` and ``b`` here are two file systems of
it, so that a copy between them goes as between two accounts.
"""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from typing import Any

import pytest
from azure.core.credentials import AccessToken
from azure.core.credentials_async import AsyncTokenCredential
from azure.storage.blob import UserDelegationKey

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
    assert opendata.credential.kind == "Anonymous"
    assert (opendata.operation_concurrency, opendata.io_concurrency) == (7, 3)

    fab = fs.filesystem("fab", "fabric.microsoft.com")
    assert fab.credential.value is token
    assert fab.is_hns_enabled is True

    # Any other account takes the defaults: the shared credential object, and the concurrency.
    other = fs.filesystem("other")
    assert other.credential.value is token
    assert (other.operation_concurrency, other.io_concurrency) == (7, 5)
    assert fs.filesystem("other") is other


def test_a_credential_named_takes_its_options() -> None:
    fs = AbfsFileSystems(
        credential="azure-cli", tenant_id="t", skip_instance_cache=True
    )
    credential = fs.credential.value
    assert credential.tenant_id == "t"
    # Made once by the router, shared by the accounts, which do not close it.
    for name in ("one", "two"):
        inner = fs.filesystem(name)
        assert inner.credential.value is credential
        assert not inner._owns_credential
    assert fs._owns_credential
    asyncio.run(credential.close())


def test_a_credential_object_is_the_callers() -> None:
    token = _Token()
    fs = AbfsFileSystems(credential=token, skip_instance_cache=True)
    assert fs.credential.value is token
    assert not fs._owns_credential


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


async def test_readable_url(make_router) -> None:
    fs = make_router({"a": _SAS, "b": _KEY})
    # The source's own SAS, in its client's URL.
    assert (await fs.filesystem("a")._readable_url("/c/x.txt")).endswith(
        f"/c/x.txt?{_SAS}"
    )
    # A SAS signed with the account key.
    query = (await fs.filesystem("b")._readable_url("/c/x.txt")).partition("?")[2]
    assert "sig=" in query
    assert "sp=r" in query

    token = _keyed()
    # A user delegation SAS, by the key kept: no request.
    token._user_delegation_key = _delegation_key(datetime.now(UTC) + timedelta(days=1))
    query = (await token._readable_url("/c/x.txt")).partition("?")[2]
    assert "skoid=" in query
    assert "sp=r" in query
    assert token.asked == []


async def test_anonymous_readable_url(make_router) -> None:
    fs = make_router()
    assert "?" not in await fs.filesystem("a")._readable_url("/c/x.txt")


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
    fs._user_delegation_key = _delegation_key(datetime.now(UTC) + timedelta(hours=1))
    await fs._get_user_delegation_key()
    assert len(fs.asked) == 1


async def test_user_delegation_key_needs_a_token_credential() -> None:
    with pytest.raises(ValueError, match="token credential"):
        await _keyed(_SAS)._get_user_delegation_key()
