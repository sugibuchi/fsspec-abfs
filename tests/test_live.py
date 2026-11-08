"""Tests on a real account, run only when configured, e.g. in a .env file.

- ``FSSPEC_ABFS_TEST_ACCOUNT_URL``: the account's endpoint, e.g.
  ``https://<account>.dfs.core.windows.net/``.
- ``FSSPEC_ABFS_TEST_CONTAINER``: a container the tests write under, in a
  folder of their own deleted afterwards.
- ``FSSPEC_ABFS_TEST_IS_HNS_ENABLED``: "true" or "false" to declare it,
  unset to let the filesystem ask.

Credentials come from ``azure.identity.aio.DefaultAzureCredential``.
"""

from __future__ import annotations

import contextlib
import os
import uuid

import pytest

ACCOUNT_URL = os.environ.get("FSSPEC_ABFS_TEST_ACCOUNT_URL")
CONTAINER = os.environ.get("FSSPEC_ABFS_TEST_CONTAINER")

pytestmark = pytest.mark.skipif(
    not (ACCOUNT_URL and CONTAINER),
    reason="FSSPEC_ABFS_TEST_ACCOUNT_URL and FSSPEC_ABFS_TEST_CONTAINER not set",
)


@pytest.fixture
async def live():
    from azure.identity.aio import DefaultAzureCredential  # noqa: PLC0415

    from fsspec_abfs.abfs import AbfsFileSystem  # noqa: PLC0415

    declared = os.environ.get("FSSPEC_ABFS_TEST_IS_HNS_ENABLED")
    credential = DefaultAzureCredential()
    fs = AbfsFileSystem(
        ACCOUNT_URL,
        credential,
        is_hns_enabled=None if declared is None else declared.lower() == "true",
        asynchronous=True,
        skip_instance_cache=True,
    )
    root = f"{CONTAINER}/fsspec-abfs-test-{uuid.uuid4().hex[:8]}"
    try:
        yield fs, root
    finally:
        with contextlib.suppress(FileNotFoundError):
            await fs._rm(root, recursive=True)
        fs.close()
        await credential.close()


async def test_round_trip(live) -> None:
    fs, root = live
    await fs._pipe_file(f"{root}/a/x.txt", b"hello")
    await fs._pipe_file(f"{root}/a/b/y.txt", b"world")
    assert await fs._cat_file(f"{root}/a/x.txt") == b"hello"
    assert await fs._find(root) == [f"{root}/a/b/y.txt", f"{root}/a/x.txt"]
    assert (await fs._info(f"{root}/a"))["type"] == "directory"


async def test_copy_move_rm(live) -> None:
    fs, root = live
    await fs._pipe_file(f"{root}/src/b/y.txt", b"world")
    await fs._copy(f"{root}/src", f"{root}/copy", recursive=True)
    await fs._mv(f"{root}/copy", f"{root}/moved", recursive=True)
    assert await fs._cat_file(f"{root}/moved/b/y.txt") == b"world"
    assert not await fs._exists(f"{root}/copy/b/y.txt")
    await fs._rm(f"{root}/moved", recursive=True)
    assert not await fs._exists(f"{root}/moved")


async def test_large_file(live) -> None:
    fs, root = live
    data = os.urandom(20 * 1024 * 1024)
    await fs._pipe_file(f"{root}/big.bin", data)
    assert await fs._cat_file(f"{root}/big.bin") == data
