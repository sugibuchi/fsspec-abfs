"""Whether the account has a hierarchical namespace, and the profile that follows.

The probe runs over HTTP: the dummy server refuses the account information, or the root ACL, as Azure does.
"""

from __future__ import annotations

import logging

import pytest

from fsspec_abfs.abfs import HNS_PROFILE, NO_HNS_PROFILE, UNKNOWN_HNS_PROFILE

_REFUSED = (403, "AuthorizationPermissionMismatch")


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
        assert await fs._get_storage_profile() == profile
        assert profile.deletes_directories is declared
        assert dummy.requests == []


async def test_profile_asked_once(dummy, make_fs, hns) -> None:
    fs = make_fs(is_hns_enabled=None)
    expected = HNS_PROFILE if hns else NO_HNS_PROFILE
    assert await fs._get_storage_profile() == expected
    assert await fs._get_storage_profile() == expected
    # Asked once, then kept by the base class.
    assert dummy.operations() == ["GetAccountInformation"]


async def test_probe(make_fs, root, hns) -> None:
    fs = make_fs(is_hns_enabled=None)
    assert await fs._hns(root.partition("/")[0]) is hns


async def test_account_information(dummy, make_fs, hns) -> None:
    fs = make_fs(is_hns_enabled=None)
    assert await fs._hns("c") is hns
    assert await fs._hns("c") is hns
    assert dummy.operations() == ["GetAccountInformation"]


async def test_root_acl(dummy, make_fs, hns) -> None:
    # Without HNS, the Data Lake API refuses: HierarchicalNamespaceNotEnabled.
    dummy.failures["GetAccountInformation"] = _REFUSED
    fs = make_fs(is_hns_enabled=None)
    assert await fs._hns("c") is hns
    assert dummy.operations() == ["GetAccountInformation", "GetAccessControl"]


async def test_root_acl_of_first_container(dummy, make_fs, root, hns) -> None:
    # The profile is loaded by a primitive, which names the container to probe.
    dummy.failures["GetAccountInformation"] = _REFUSED
    fs = make_fs(is_hns_enabled=None)
    await fs._ls("/" + root.split("/")[1])
    assert fs.is_hns_enabled is hns
    assert dummy.count("GetAccessControl") == 1


@pytest.mark.flat_only
async def test_root_acl_unsupported_features(dummy, make_fs) -> None:
    dummy.failures["GetAccountInformation"] = _REFUSED
    dummy.failures["GetAccessControl"] = (409, "EndpointUnsupportedAccountFeatures")
    fs = make_fs(is_hns_enabled=None)
    assert await fs._hns("c") is False
    assert fs.is_hns_enabled is False


async def test_unknown_kept(dummy, make_fs, caplog) -> None:
    dummy.failures["GetAccountInformation"] = _REFUSED
    dummy.failures["GetAccessControl"] = (403, "AuthorizationFailure", "sig=SECRET")
    fs = make_fs(is_hns_enabled=None)
    fs._probe_container = "c"
    with caplog.at_level(logging.DEBUG, logger="fsspec_abfs.abfs"):
        assert await fs._get_storage_profile() == UNKNOWN_HNS_PROFILE
        assert await fs._get_storage_profile() == UNKNOWN_HNS_PROFILE
    assert fs.is_hns_enabled is None
    # Unknown is a profile right on both kinds of account: kept, never asked again.
    assert dummy.count("GetAccountInformation") == 1
    assert dummy.count("GetAccessControl") == 1

    warnings = [r for r in caplog.records if r.levelno == logging.WARNING]
    assert len(warnings) == 1
    assert "AuthorizationFailure" in warnings[0].getMessage()
    assert "SECRET" not in caplog.text  # Never Azure's message.


async def test_no_container(dummy, make_fs) -> None:
    dummy.failures["GetAccountInformation"] = _REFUSED
    fs = make_fs(is_hns_enabled=None)
    assert await fs._hns("") is None
    assert dummy.count("GetAccessControl") == 0


async def test_operations_ask_first(dummy, make_fs, hns) -> None:
    fs = make_fs(is_hns_enabled=None)
    await fs._pipe_file("c/a/x", b"1")
    await fs._rm("c/a", recursive=True)
    assert fs.is_hns_enabled is hns
    # With the account's profile: one recursive delete with HNS.
    assert dummy.count("DeletePath") == (1 if hns else 0)
    assert dummy.containers["c"].blobs == {}
