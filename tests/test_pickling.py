"""Tests of pickling: a file system is pickled as its constructor's arguments, and nothing it made is."""

from __future__ import annotations

import asyncio
import pickle
from typing import Any

import pytest
from azure.storage.blob import UserDelegationKey

from fsspec_abfs.abfs import AbfsFileSystem
from fsspec_abfs.routing import AbfsFileSystems


@pytest.fixture(autouse=True)
def _clear_instance_caches() -> Any:
    # Unpickling makes a file system through fsspec's instance cache.
    yield
    AbfsFileSystem.clear_instance_cache()
    AbfsFileSystems.clear_instance_cache()


async def test_nothing_made_is_pickled() -> None:
    fs = AbfsFileSystem(
        account_name="acct",
        credential="azure-cli",
        tenant_id="t",
        asynchronous=True,
        skip_instance_cache=True,
    )
    fs._ensure_service_clients()
    made = fs._get_credential().value
    fs._AbfsFileSystem__user_delegation_key = UserDelegationKey()
    try:
        copy = pickle.loads(pickle.dumps(fs))
        assert copy is not fs
        assert (copy.account_name, copy.account_domain) == ("acct", "core.windows.net")
        # The credential as given, and its options: the credential made is made again, at first need.
        assert copy.credential == "azure-cli"
        assert copy.credential_options["tenant_id"] == "t"
        assert copy._AbfsFileSystem__credential is None
        assert copy._AbfsFileSystem__user_delegation_key is None
        assert copy._blob_service_client is None
        assert copy._client_loop is None
        assert copy._get_credential().value is not made
        assert copy._get_credential().value.tenant_id == "t"
        await copy._get_credential().value.close()
    finally:
        await fs._blob_service_client.close()
        await fs._dfs_service_client.close()
        await made.close()


def test_the_loop_is_not_pickled() -> None:
    # An account's file system is made on the router's loop, passed as an argument.
    router = AbfsFileSystems(
        credential="azure-cli",
        accounts=[{"account_name": "opendata", "credential": "anonymous"}],
        skip_instance_cache=True,
    )
    inner = router.filesystem("opendata")
    assert inner.storage_options["loop"] is router.loop
    copy = pickle.loads(pickle.dumps(inner))
    assert copy.credential == "anonymous"
    assert copy.loop is not None
    assert "loop" not in copy.storage_options


def test_a_router_is_pickled_with_its_credential_as_given() -> None:
    router = AbfsFileSystems(
        credential="azure-cli",
        tenant_id="t",
        accounts=[{"account_name": "opendata", "credential": "anonymous"}],
        cross_account_copy="server-side",
        skip_instance_cache=True,
    )
    made = router._get_credential().value
    try:
        copy = pickle.loads(pickle.dumps(router))
        assert copy.credential == "azure-cli"
        assert copy.credential_options["tenant_id"] == "t"
        assert copy._AbfsFileSystems__credential is None
        assert copy.cross_account_copy == "server-side"
        assert copy.filesystem("opendata").credential == "anonymous"
    finally:
        asyncio.run(made.close())


def test_a_token_credential_object_is_pickled_as_given() -> None:
    from azure.identity.aio import AzureCliCredential  # noqa: PLC0415

    # One that pickles itself, as this one does until used: the copy's is the caller's still.
    credential = AzureCliCredential()
    fs = AbfsFileSystem(
        account_name="acct", credential=credential, skip_instance_cache=True
    )
    copy = pickle.loads(pickle.dumps(fs))
    try:
        assert isinstance(copy.credential, AzureCliCredential)
        assert copy._get_credential().value is copy.credential
        assert not copy._owns_credential()
    finally:
        asyncio.run(credential.close())
        asyncio.run(copy.credential.close())
