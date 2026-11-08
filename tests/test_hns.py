"""Whether the account has a hierarchical namespace, and the profile that follows."""

from __future__ import annotations

import asyncio
import logging

import pytest
from azure.core.exceptions import HttpResponseError

from fsspec_abfs.abfs import HNS_PROFILE, NO_HNS_PROFILE, UNKNOWN_HNS_PROFILE


@pytest.mark.parametrize(
    ("declared", "profile"),
    [(True, HNS_PROFILE), (False, NO_HNS_PROFILE), (None, UNKNOWN_HNS_PROFILE)],
)
def test_declared(make_fs, account, declared, profile) -> None:
    fs = make_fs(is_hns_enabled=declared)
    assert fs.is_hns_enabled is declared
    assert fs.storage_profile == profile
    assert (fs._rm_tree is not None) is bool(declared)
    assert not account.calls


@pytest.mark.parametrize("answer", [True, False])
async def test_account_information(make_fs, account, answer) -> None:
    account.account_information = {"is_hns_enabled": answer}
    fs = make_fs(is_hns_enabled=None)
    assert await fs._hns("c") is answer
    assert await fs._hns("c") is answer
    assert fs.is_hns_enabled is answer
    assert fs.storage_profile == (HNS_PROFILE if answer else NO_HNS_PROFILE)
    assert account.calls == {"get_account_information": 1}


async def test_root_acl(make_fs, account, azure_error) -> None:
    account.account_information = azure_error(
        HttpResponseError, "AuthorizationPermissionMismatch", 403
    )
    account.access_control = {"acl": "user::rwx"}
    fs = make_fs(is_hns_enabled=None)
    assert await fs._hns("c") is True
    assert account.calls == {"get_account_information": 1, "get_access_control": 1}


@pytest.mark.parametrize(
    ("code", "status"),
    [
        ("HierarchicalNamespaceNotEnabled", 400),
        ("EndpointUnsupportedAccountFeatures", 409),
    ],
)
async def test_root_acl_refused(make_fs, account, azure_error, code, status) -> None:
    account.account_information = azure_error(
        HttpResponseError, "AuthorizationPermissionMismatch", 403
    )
    account.access_control = azure_error(HttpResponseError, code, status)
    fs = make_fs(is_hns_enabled=None)
    assert await fs._hns("c") is False
    assert fs.is_hns_enabled is False


async def test_unknown_not_kept(make_fs, account, azure_error, caplog) -> None:
    account.account_information = azure_error(
        HttpResponseError, "AuthorizationPermissionMismatch", 403
    )
    account.access_control = azure_error(HttpResponseError, "AuthorizationFailure", 403)
    fs = make_fs(is_hns_enabled=None)
    with caplog.at_level(logging.DEBUG, logger="fsspec_abfs.abfs"):
        assert await fs._hns("c") is None
        assert await fs._hns("c") is None
    assert fs.is_hns_enabled is None
    assert fs.storage_profile == UNKNOWN_HNS_PROFILE
    # Asked again, as unknown is not kept.
    assert account.calls == {"get_account_information": 2, "get_access_control": 2}

    warnings = [r for r in caplog.records if r.levelno == logging.WARNING]
    assert len(warnings) == 1
    assert "AuthorizationFailure" in warnings[0].getMessage()
    assert "SECRET" not in caplog.text  # Never Azure's message.

    # Once the account answers, it is kept.
    account.access_control = {"acl": ""}
    assert await fs._hns("c") is True
    assert fs.storage_profile == HNS_PROFILE


async def test_no_container(make_fs, account, azure_error) -> None:
    account.account_information = azure_error(
        HttpResponseError, "AuthorizationPermissionMismatch", 403
    )
    fs = make_fs(is_hns_enabled=None)
    assert await fs._hns("") is None
    assert "get_access_control" not in account.calls


async def test_one_probe_shared(make_fs, account) -> None:
    fs = make_fs(is_hns_enabled=None)
    answers = await asyncio.gather(*(fs._hns("c") for _ in range(5)))
    assert answers == [False] * 5
    assert account.calls == {"get_account_information": 1}


async def test_operations_ask_first(make_fs, account) -> None:
    account.account_information = {"is_hns_enabled": False}
    account.put("c", "a/x", b"1")
    fs = make_fs(is_hns_enabled=None)
    await fs._rm("c/a", recursive=True)
    assert fs.is_hns_enabled is False
    # With the profile of no HNS: no folder to delete.
    assert account.containers["c"] == {}
