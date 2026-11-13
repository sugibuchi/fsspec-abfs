"""Whether the account has a hierarchical namespace, and the profile that follows.

The probe runs over HTTP: the dummy server refuses the account information as Azure does, e.g. to a SAS of one
container.
"""

from __future__ import annotations

import logging

import pytest

from fsspec_abfs.abfs import HNS_PROFILE, NO_HNS_PROFILE, UNKNOWN_HNS_PROFILE

_REFUSED = (403, "AuthorizationPermissionMismatch", "sig=SECRET")


@pytest.mark.parametrize(
    ("declared", "profile"),
    [(True, HNS_PROFILE), (False, NO_HNS_PROFILE), (None, UNKNOWN_HNS_PROFILE)],
)
async def test_declared(dummy, make_fs, declared, profile) -> None:
    fs = make_fs(is_hns_enabled=declared)
    assert fs.is_hns_enabled is declared
    assert dummy.requests == []
    if declared is not None:
        # A declared namespace is never asked.
        assert fs.storage_profile == profile
        assert await fs._get_storage_profile("/c/a") == profile
        assert profile.deletes_directories is declared
        assert dummy.requests == []


async def test_profile_asked_once(dummy, make_fs, hns) -> None:
    fs = make_fs(is_hns_enabled=None)
    expected = HNS_PROFILE if hns else NO_HNS_PROFILE
    assert await fs._get_storage_profile("/c/a") == expected
    assert await fs._get_storage_profile("/c/a") == expected
    # Asked once, then kept by the base class.
    assert dummy.operations() == ["GetAccountInformation"]


async def test_account_information(dummy, make_fs, hns) -> None:
    fs = make_fs(is_hns_enabled=None)
    assert await fs._check_hns_enabled() is hns
    assert dummy.operations() == ["GetAccountInformation"]
    # It asks each time, keeping nothing: loading the profile keeps it.
    assert fs.is_hns_enabled is None
    await fs._get_storage_profile("/c")
    await fs._get_storage_profile("/c")
    assert fs.is_hns_enabled is hns
    assert dummy.operations() == ["GetAccountInformation"] * 2


async def test_unknown_kept(dummy, make_fs, caplog) -> None:
    # E.g. a SAS of one container: refused the account information.
    dummy.failures["GetAccountInformation"] = _REFUSED
    fs = make_fs(is_hns_enabled=None)
    with caplog.at_level(logging.DEBUG, logger="fsspec_abfs.abfs"):
        assert await fs._get_storage_profile("/c/a") == UNKNOWN_HNS_PROFILE
        assert await fs._get_storage_profile("/c/a") == UNKNOWN_HNS_PROFILE
    assert fs.is_hns_enabled is None
    # Unknown is a profile right on both kinds of account: kept, never asked again.
    assert dummy.operations() == ["GetAccountInformation"]

    warnings = [r for r in caplog.records if r.levelno == logging.WARNING]
    assert len(warnings) == 1
    assert "AuthorizationPermissionMismatch" in warnings[0].getMessage()
    assert "SECRET" not in caplog.text  # Never Azure's message.


@pytest.mark.parametrize(
    "call",
    [
        lambda fs: fs._find("/c/a"),
        lambda fs: fs._du("/c/a"),
        lambda fs: fs._glob("/c/a/*"),
        lambda fs: fs._rm("/c/a", recursive=True),
        lambda fs: fs._copy("/c/a", "/c/b", recursive=True),
        lambda fs: fs._mv("/c/a", "/c/b", recursive=True),
    ],
)
async def test_a_refused_account_works_unknown(dummy, make_fs, call) -> None:
    await make_fs()._pipe_file("/c/a/x", b"1")
    dummy.failures["GetAccountInformation"] = _REFUSED
    dummy.requests.clear()
    fs = make_fs(is_hns_enabled=None)
    await call(fs)
    assert fs.is_hns_enabled is None
    assert fs.storage_profile == UNKNOWN_HNS_PROFILE


@pytest.mark.xfail(
    strict=True,
    reason="The recursive methods strip a URL's account before any primitive checks it",
)
async def test_recursive_methods_refuse_another_account(make_fs) -> None:
    fs = make_fs()
    other = "abfss://c@other.dfs.core.windows.net/a"
    with pytest.raises(ValueError, match="not of this filesystem's"):
        await fs._find(other)
    with pytest.raises(ValueError, match="not of this filesystem's"):
        await fs._copy("/c/a", other, recursive=True)


async def test_operations_ask_first(dummy, make_fs, hns) -> None:
    fs = make_fs(is_hns_enabled=None)
    await fs._pipe_file("c/a/x", b"1")
    await fs._rm("c/a", recursive=True)
    assert fs.is_hns_enabled is hns
    # With the account's profile: one recursive delete with HNS.
    assert dummy.count("DeletePath") == (1 if hns else 0)
    assert dummy.containers["c"].blobs == {}
