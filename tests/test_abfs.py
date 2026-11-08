"""AbfsFileSystem on the fake blob API: an account without a hierarchical namespace, or unknown."""

from __future__ import annotations

import errno

import pytest

from fsspec_abfs.abfs import AbfsReadableFile, AbfsWritableFile


@pytest.fixture
def fs(make_fs, account):
    account.put("c", "a/x.txt", b"hello")
    account.put("c", "a/b/y.txt", b"world")
    account.put("c", "z.txt", b"!")
    account.containers["empty"] = {}
    return make_fs()


async def test_ls_root(fs) -> None:
    assert await fs._ls("", detail=False) == ["c", "empty"]


async def test_ls(fs, account) -> None:
    assert sorted(await fs._ls("abfs://c", detail=False)) == ["c/a", "c/z.txt"]
    entries = {e["name"]: e for e in await fs._ls("c/a")}
    assert entries["c/a/b"]["type"] == "directory"
    assert entries["c/a/x.txt"] == {**entries["c/a/x.txt"], "size": 5, "type": "file"}
    # One listing per folder.
    assert account.calls["walk_blobs"] == 2


async def test_ls_file_and_missing(fs) -> None:
    assert await fs._ls("c/z.txt", detail=False) == ["c/z.txt"]
    assert await fs._ls("empty", detail=False) == []
    with pytest.raises(FileNotFoundError):
        await fs._ls("c/nothing")
    with pytest.raises(FileNotFoundError):
        await fs._ls("nocontainer/a")


async def test_info(fs) -> None:
    assert (await fs._info(""))["type"] == "directory"
    assert (await fs._info("c"))["type"] == "directory"
    assert (await fs._info("c/a"))["type"] == "directory"
    info = await fs._info("c/a/x.txt")
    assert (info["type"], info["size"], info["etag"]) == ("file", 5, '"1"')
    for missing in ("c/nothing", "nocontainer"):
        with pytest.raises(FileNotFoundError):
            await fs._info(missing)


async def test_find(fs) -> None:
    assert await fs._find("c") == ["c/a/b/y.txt", "c/a/x.txt", "c/z.txt"]


async def test_cat_and_pipe(fs, account) -> None:
    assert await fs._cat_file("c/a/x.txt") == b"hello"
    assert await fs._cat_file("c/a/x.txt", start=1, end=3) == b"el"
    await fs._pipe_file("c/new/n.txt", b"data")
    assert account.containers["c"]["new/n.txt"]["data"] == b"data"
    with pytest.raises(FileExistsError):
        await fs._pipe_file("c/new/n.txt", b"again", mode="create")


async def test_multipart_write(fs, account) -> None:
    f = await fs._open_file("c/big.bin", "wb")
    f.min_block_size = f.max_block_size = f.min_multipart_size = 4
    assert isinstance(f, AbfsWritableFile)
    await f._write(b"0123456789")
    await f.aclose()
    assert account.containers["c"]["big.bin"]["data"] == b"0123456789"
    assert account.calls["stage_block"] >= 2
    assert account.calls["commit_block_list"] == 1
    assert not account.staged


async def test_exclusive_multipart_write(fs, account) -> None:
    f = await fs._open_file("c/z.txt", "xb")
    f.min_block_size = f.max_block_size = f.min_multipart_size = 4
    await f._write(b"0123456789")
    with pytest.raises(FileExistsError):
        await f.aclose()
    assert account.containers["c"]["z.txt"]["data"] == b"!"


async def test_read_changed(fs, account) -> None:
    f = await fs._open_file("c/a/x.txt", "rb")
    assert isinstance(f, AbfsReadableFile)
    account.put("c", "a/x.txt", b"HELLO")
    with pytest.raises(OSError) as info:
        await f._read()
    assert info.value.errno == errno.ESTALE


async def test_open_folder(fs) -> None:
    with pytest.raises(IsADirectoryError):
        await fs.open_async("c/a")


async def test_rm_recursive(fs, account) -> None:
    await fs._rm("c/a", recursive=True)
    assert sorted(account.containers["c"]) == ["z.txt"]
    # Implicit folders: only the blobs are deleted.
    assert account.calls["delete_blob"] == 2


async def test_copy_recursive(fs, account) -> None:
    await fs._copy("c/a", "empty/a2", recursive=True)
    assert sorted(account.containers["empty"]) == ["a2/b/y.txt", "a2/x.txt"]
    assert account.calls["start_copy_from_url"] == 2


async def test_cp_missing_source(fs) -> None:
    with pytest.raises(FileNotFoundError) as info:
        await fs._cp_file("c/nothing", "c/copy")
    assert info.value.filename == "c/nothing"


async def test_mv_recursive(fs, account) -> None:
    await fs._mv("c/a", "c/moved", recursive=True)
    assert sorted(account.containers["c"]) == [
        "moved/b/y.txt",
        "moved/x.txt",
        "z.txt",
    ]
    # Without HNS: server-side copies, then deletions.
    assert account.calls["start_copy_from_url"] == 2


async def test_makedirs_and_rmdir(fs, account) -> None:
    await fs._makedirs("c/newdir", exist_ok=True)
    assert "newdir" not in account.containers["c"]  # Implicit: nothing written.
    with pytest.raises(FileExistsError):
        await fs._makedirs("c/a")
    await fs._makedirs("newcontainer/d")
    assert "newcontainer" in account.containers

    with pytest.raises(OSError) as info:
        await fs._rmdir("c/a")
    assert info.value.errno == errno.ENOTEMPTY
    with pytest.raises(NotADirectoryError):
        await fs._rmdir("c/z.txt")
    await fs._rmdir("empty")
    assert "empty" not in account.containers


async def test_unknown_hns_marks_folders(make_fs, account, azure_error) -> None:
    from azure.core.exceptions import HttpResponseError  # noqa: PLC0415

    account.account_information = azure_error(HttpResponseError, "Forbidden", 403)
    account.access_control = azure_error(HttpResponseError, "Forbidden", 403)
    account.put("c", "a/x.txt", b"1")
    fs = make_fs(is_hns_enabled=None)

    # A marker blob, which is a real folder on an HNS account.
    await fs._makedirs("c/a/d")
    assert account.containers["c"]["a/d"]["metadata"] == {"hdi_isfolder": "true"}
    account.put("c", "a/d/in.txt", b"2")
    # The folder's blob and its prefix are one entry.
    assert sorted(await fs._ls("c/a", detail=False)) == ["c/a/d", "c/a/x.txt"]
    assert (await fs._info("c/a/d"))["type"] == "directory"

    # Unknown: emptied folders are deleted, marker blobs too.
    await fs._rm("c/a", recursive=True)
    assert account.containers["c"] == {}


def test_sync_api_and_transaction(make_fs, account) -> None:
    from fsspec_abfs.abfs import AbfsFileSystem  # noqa: PLC0415

    account.containers["c"] = {}
    fs = AbfsFileSystem(
        "https://acc.dfs.core.windows.net/",
        is_hns_enabled=False,
        skip_instance_cache=True,
    )
    with fs.transaction:
        with fs.open("c/t.txt", "wb") as f:
            f.write(b"abc")
        assert "t.txt" not in account.containers["c"]
    assert fs.cat("c/t.txt") == b"abc"
    fs.close()
    with pytest.raises(RuntimeError):
        fs.cat("c/t.txt")
