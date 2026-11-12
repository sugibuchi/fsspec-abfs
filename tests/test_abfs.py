"""AbfsFileSystem over HTTP, on accounts with and without a hierarchical namespace.

The tests taking ``root`` run on the dummy server and on live accounts. Those taking ``dummy`` run on the dummy
server only: they count the requests served, or switch the server's behaviour.
"""

from __future__ import annotations

import errno
import os
from datetime import timedelta

import pytest

from fsspec_abfs.abfs import AbfsReadableFile, AbfsWritableFile

_SAS = "sv=2025-01-05&ss=b&srt=sco&sp=rwdl&sig=c2ln"
"""A SAS token in its form, which the dummy takes without checking it."""


@pytest.fixture
async def tree(fs, root):
    """``a/x.txt``, ``a/b/y.txt`` and ``z.txt`` under the root."""
    await fs._pipe_file(f"{root}/a/x.txt", b"hello")
    await fs._pipe_file(f"{root}/a/b/y.txt", b"world")
    await fs._pipe_file(f"{root}/z.txt", b"!")
    return root


def _small_blocks(f) -> None:
    f.min_block_size = f.max_block_size = f.min_multipart_size = 4


async def test_ls(fs, tree) -> None:
    assert sorted(await fs._ls(tree, detail=False)) == [f"{tree}/a", f"{tree}/z.txt"]
    entries = {e["name"]: e for e in await fs._ls(f"abfs://{tree}/a")}
    assert entries[f"{tree}/a/b"]["type"] == "directory"
    x = entries[f"{tree}/a/x.txt"]
    assert (x["type"], x["size"]) == ("file", 5)


def _missing_container(api: str) -> tuple[type[OSError], ...]:
    """What a missing container raises: on Azure, a role on one container only cannot tell, and is refused."""
    return (
        (FileNotFoundError,) if api == "dummy" else (FileNotFoundError, PermissionError)
    )


async def test_ls_file_and_missing(api, fs, tree) -> None:
    assert await fs._ls(f"{tree}/z.txt", detail=False) == [f"{tree}/z.txt"]
    with pytest.raises(FileNotFoundError):
        await fs._ls(f"{tree}/nothing")
    with pytest.raises(_missing_container(api)):
        await fs._ls("nocontainer0fsspecabfs/a")


@pytest.mark.parametrize(
    ("status", "code", "error"),
    [
        (403, "AuthorizationPermissionMismatch", PermissionError),
        (400, "InvalidQueryParameterValue", OSError),
    ],
)
async def test_ls_refused_raises_the_listings_error(
    dummy, fs, tree, hns, status: int, code: str, error: type[OSError]
) -> None:
    # Only a not-found listing falls back to the path's info, as a file's does.
    dummy.failures["ListPaths" if hns else "ListBlobs"] = (status, code)
    dummy.requests.clear()
    with pytest.raises(error):
        await fs._ls(f"{tree}/z.txt")
    assert dummy.count("GetBlobProperties") == 0


async def test_empty_folder(fs, root, hns) -> None:
    await fs._makedirs(f"{root}/empty")
    if hns:
        # A real folder: listed empty, and deleted by rmdir.
        assert await fs._ls(f"{root}/empty") == []
        assert await fs._isdir(f"{root}/empty")
        await fs._rmdir(f"{root}/empty")
    # Without HNS, a folder is made by its files: nothing was written.
    assert not await fs._exists(f"{root}/empty")


async def test_info(api, fs, tree) -> None:
    assert (await fs._info(""))["type"] == "directory"
    assert (await fs._info(tree.partition("/")[0]))["type"] == "directory"
    assert (await fs._info(f"{tree}/a"))["type"] == "directory"
    info = await fs._info(f"{tree}/a/x.txt")
    assert (info["type"], info["size"]) == ("file", 5)
    assert info["etag"]
    with pytest.raises(FileNotFoundError):
        await fs._info(f"{tree}/nothing")
    with pytest.raises(_missing_container(api)):
        await fs._info("nocontainer0fsspecabfs")


async def test_find(fs, tree) -> None:
    assert await fs._find(tree) == [
        f"{tree}/a/b/y.txt",
        f"{tree}/a/x.txt",
        f"{tree}/z.txt",
    ]


async def test_cat_and_pipe(fs, tree) -> None:
    assert await fs._cat_file(f"{tree}/a/x.txt") == b"hello"
    assert await fs._cat_file(f"{tree}/a/x.txt", start=1, end=3) == b"el"
    await fs._pipe_file(f"{tree}/new/n.txt", b"data")
    assert await fs._cat_file(f"{tree}/new/n.txt") == b"data"
    with pytest.raises(FileExistsError):
        await fs._pipe_file(f"{tree}/new/n.txt", b"again", mode="create")
    assert await fs._cat_file(f"{tree}/new/n.txt") == b"data"


@pytest.mark.parametrize(
    ("start", "end"),
    [
        (None, None),
        (2, None),
        (None, 3),
        (1, 4),
        (-3, None),
        (-4, -1),
        (3, 3),
        (20, None),
    ],
)
async def test_cat_file_slices_as_python(fs, tree, start, end) -> None:
    assert await fs._cat_file(f"{tree}/a/x.txt", start, end) == b"hello"[start:end]


async def test_cat_file_is_one_download(fs, dummy, tree) -> None:
    dummy.requests.clear()
    await fs._cat_file(f"{tree}/a/x.txt")
    await fs._cat_file(f"{tree}/a/x.txt", start=1, end=3)
    # No properties read first, as adlfs's.
    assert len(dummy.requests) == 2


def _max_in_flight(records: list) -> int:
    """The most requests the dummy served at once, from their start and end times."""
    events = sorted(
        [(r.started, 1) for r in records] + [(r.ended, -1) for r in records],
        # An end before a start at the same instant: they did not overlap.
        key=lambda e: (e[0], e[1]),
    )
    most = current = 0
    for _, step in events:
        current += step
        most = max(most, current)
    return most


async def test_cat_of_many_files_keeps_to_io_limit(make_fs, dummy, tree) -> None:
    fs = make_fs(io_concurrency=3)
    paths = [f"{tree}/many/{i}" for i in range(12)]
    for path in paths:
        await fs._pipe_file(path, b"x")
    dummy.requests.clear()
    dummy.delay = 0.05  # Long enough for requests sent together to overlap.

    assert await fs._cat(paths) == dict.fromkeys(paths, b"x")
    assert _max_in_flight(dummy.requests) == 3


async def test_cat_file_of_a_directory_or_missing(fs, tree, hns) -> None:
    # A folder's marker blob with HNS, none without: as opening it.
    with pytest.raises(IsADirectoryError if hns else FileNotFoundError):
        await fs._cat_file(f"{tree}/a")
    with pytest.raises(FileNotFoundError):
        await fs._cat_file(f"{tree}/nothing")


async def test_multipart_write(fs, root) -> None:
    f = await fs._open_file(f"{root}/big.bin", "wb")
    assert isinstance(f, AbfsWritableFile)
    _small_blocks(f)
    await f._write(b"0123456789")
    await f.aclose()
    assert await fs._cat_file(f"{root}/big.bin") == b"0123456789"


async def test_exclusive_multipart_write(fs, tree) -> None:
    f = await fs._open_file(f"{tree}/z.txt", "xb")
    _small_blocks(f)
    await f._write(b"0123456789")
    with pytest.raises(FileExistsError):
        await f.aclose()
    assert await fs._cat_file(f"{tree}/z.txt") == b"!"


async def test_read_changed(fs, tree) -> None:
    f = await fs._open_file(f"{tree}/a/x.txt", "rb")
    assert isinstance(f, AbfsReadableFile)
    await fs._pipe_file(f"{tree}/a/x.txt", b"HELLO")
    with pytest.raises(OSError) as info:
        await f._read()
    assert info.value.errno == errno.ESTALE


async def test_open_folder(fs, tree) -> None:
    with pytest.raises(IsADirectoryError):
        await fs.open_async(f"{tree}/a")


async def test_large_file(fs, root) -> None:
    data = os.urandom(20 * 1024 * 1024)
    await fs._pipe_file(f"{root}/big.bin", data)
    assert await fs._cat_file(f"{root}/big.bin") == data


async def test_rm_recursive(fs, tree) -> None:
    await fs._rm(f"{tree}/a", recursive=True)
    assert await fs._find(tree) == [f"{tree}/z.txt"]
    # No folder left either, with HNS.
    assert not await fs._exists(f"{tree}/a")


async def test_copy_recursive(fs, tree) -> None:
    await fs._copy(f"{tree}/a", f"{tree}/a2", recursive=True)
    assert await fs._find(f"{tree}/a2") == [f"{tree}/a2/b/y.txt", f"{tree}/a2/x.txt"]
    assert await fs._cat_file(f"{tree}/a2/b/y.txt") == b"world"


async def test_cp_missing_source(fs, tree) -> None:
    with pytest.raises(FileNotFoundError) as info:
        await fs._cp_file(f"{tree}/nothing", f"{tree}/copy")
    assert info.value.filename == f"{tree}/nothing"


async def test_mv_recursive(fs, tree) -> None:
    await fs._mv(f"{tree}/a", f"{tree}/moved", recursive=True)
    assert await fs._find(tree) == [
        f"{tree}/moved/b/y.txt",
        f"{tree}/moved/x.txt",
        f"{tree}/z.txt",
    ]
    assert not await fs._exists(f"{tree}/a")


async def test_mv_file(fs, tree) -> None:
    # Into a folder that does not exist yet.
    await fs._mv(f"{tree}/z.txt", f"{tree}/new/deep/z.txt")
    assert await fs._cat_file(f"{tree}/new/deep/z.txt") == b"!"
    assert not await fs._exists(f"{tree}/z.txt")


async def test_makedirs_and_rmdir(fs, tree, hns) -> None:
    await fs._makedirs(f"{tree}/newdir", exist_ok=True)
    assert await fs._exists(f"{tree}/newdir") is hns
    with pytest.raises(FileExistsError):
        await fs._makedirs(f"{tree}/a")
    with pytest.raises(FileExistsError):
        await fs._makedirs(f"{tree}/z.txt", exist_ok=True)

    with pytest.raises(OSError) as info:
        await fs._rmdir(f"{tree}/a")
    assert info.value.errno == errno.ENOTEMPTY
    with pytest.raises(NotADirectoryError):
        await fs._rmdir(f"{tree}/z.txt")
    assert await fs._exists(f"{tree}/a/x.txt")


def test_sync_api_and_transaction(api, make_fs, root) -> None:
    # Live, a credential of its own: the sync API binds its session to fsspec's
    # loop, where close() closes it, not the test's loop the fixture's is closed on.
    own = {"credential": "default"} if api == "live" else {}
    fs = make_fs(asynchronous=False, **own)
    fs.pipe(f"{root}/other.txt", b"x")
    with fs.transaction:
        with fs.open(f"{root}/t.txt", "wb") as f:
            f.write(b"abc")
        assert not fs.exists(f"{root}/t.txt")
    assert fs.cat(f"{root}/t.txt") == b"abc"
    fs.close()
    with pytest.raises(RuntimeError):
        fs.cat(f"{root}/t.txt")


# The requests sent, on the dummy server.


async def test_ls_requests(dummy, fs, tree, hns) -> None:
    dummy.requests.clear()
    await fs._ls(f"{tree}/a")
    # One listing per folder: the Data Lake API's with HNS, the blob API's without.
    assert dummy.operations() == (["ListPaths"] if hns else ["ListBlobs"])


async def test_listing_pages(dummy, fs, root, hns) -> None:
    for i in range(5):
        await fs._pipe_file(f"{root}/{i}.txt", b"")
    dummy.page_size = 2
    dummy.requests.clear()
    assert len(await fs._ls(root)) == 5
    assert dummy.count("ListPaths" if hns else "ListBlobs") == 3


async def test_blob_listing_of_folders(dummy, make_fs, tree) -> None:
    # Unknown, so listed by the blob API. With HNS, it lists a folder twice:
    # as a blob, and as a prefix.
    dummy.failures["GetAccountInformation"] = (403, "AuthorizationPermissionMismatch")
    dummy.failures["GetAccessControl"] = (403, "AuthorizationFailure")
    fs = make_fs(is_hns_enabled=None)
    assert sorted(await fs._ls(tree, detail=False)) == [f"{tree}/a", f"{tree}/z.txt"]
    assert dummy.count("ListBlobs") == 1


async def test_multipart_requests(dummy, fs, root) -> None:
    f = await fs._open_file(f"{root}/big.bin", "wb")
    _small_blocks(f)
    await f._write(b"0123456789")
    await f.aclose()
    assert dummy.count("PutBlock") >= 2
    assert dummy.count("PutBlockList") == 1
    assert dummy.count("PutBlob") == 0
    assert not dummy.containers["c"].staged


async def test_rm_requests(dummy, fs, tree, hns) -> None:
    dummy.requests.clear()
    await fs._rm(f"{tree}/a", recursive=True)
    if hns:
        # One recursive delete of the Data Lake API.
        assert dummy.count("DeletePath") == 1
        assert dummy.count("DeleteBlob") == 0
    else:
        # Implicit folders: only the blobs are deleted.
        assert dummy.count("DeleteBlob") == 2
    assert dummy.files("c") == {"work/z.txt": b"!"}


@pytest.mark.hns_only
async def test_mv_renames(dummy, fs, tree) -> None:
    dummy.requests.clear()
    await fs._mv(f"{tree}/a", f"{tree}/moved", recursive=True)
    # A folder with all in it, in one atomic request.
    assert dummy.count("RenamePath") == 1
    assert dummy.count("CopyBlob") == 0
    assert "work/moved/b" in dummy.folders("c")


@pytest.mark.hns_only
async def test_mv_creates_parents(dummy, fs, tree) -> None:
    dummy.requests.clear()
    await fs._mv(f"{tree}/z.txt", f"{tree}/new/deep/z.txt")
    # The strategy creates the missing parent first: one rename, never refused.
    assert dummy.count("RenamePath") == 1
    assert dummy.count("CreatePath") == 1
    assert {"work/new", "work/new/deep"} <= set(dummy.folders("c"))


@pytest.mark.hns_only
async def test_mv_between_containers(dummy, make_fs, tree) -> None:
    dummy.create_container("c2")
    # The SDK renames into another container only with a credential: a SAS,
    # which the dummy takes without checking it, and which goes over HTTP.
    fs = make_fs(credential=_SAS)
    dummy.requests.clear()
    # A folder with all in it moves to another container of the account by one
    # rename, as within a container: nothing is copied.
    await fs._mv(f"{tree}/a", "/c2/moved", recursive=True)
    assert dummy.count("RenamePath") == 1
    assert dummy.count("CopyBlob") == 0
    assert set(dummy.files("c2")) == {"moved/x.txt", "moved/b/y.txt"}
    assert not any(name.startswith("work/a") for name in dummy.files("c"))

    # A file too.
    dummy.requests.clear()
    await fs._mv(f"{tree}/z.txt", "/c2/z.txt")
    assert dummy.count("RenamePath") == 1
    assert dummy.count("CopyBlob") == 0
    assert "z.txt" in dummy.files("c2")


@pytest.mark.hns_only
async def test_mv_between_containers_anonymous(dummy, fs, tree) -> None:
    # Anonymous, the SDK sends no rename into another container: refused first.
    dummy.create_container("c2")
    dummy.requests.clear()
    with pytest.raises(PermissionError):
        await fs._mv_file(f"{tree}/z.txt", "/c2/z.txt")
    assert dummy.count("RenamePath") == 0
    assert "work/z.txt" in dummy.files("c")


@pytest.mark.hns_only
async def test_rm_recursive_file(dummy, fs, tree) -> None:
    dummy.requests.clear()
    await fs._rm(f"{tree}/z.txt", recursive=True)
    # A file root goes by the same recursive delete.
    assert dummy.count("DeletePath") == 1
    assert "work/z.txt" not in dummy.files("c")


@pytest.mark.flat_only
async def test_mv_copies(dummy, fs, tree) -> None:
    dummy.requests.clear()
    await fs._mv(f"{tree}/a", f"{tree}/moved", recursive=True)
    # Without HNS: server-side copies, then deletions.
    assert dummy.count("CopyBlob") == 2
    assert dummy.count("DeleteBlob") == 2
    assert dummy.count("RenamePath") == 0


async def test_makedirs_requests(dummy, fs, root, hns) -> None:
    await fs._makedirs(f"{root}/d1/d2")
    if hns:
        # A real folder, and its parents, by one request.
        assert dummy.count("CreatePath") == 1
        assert dummy.folders("c") == ["work", "work/d1", "work/d1/d2"]
    else:
        assert dummy.count("CreatePath") == 0
        assert dummy.containers["c"].blobs == {}


@pytest.mark.hns_only
async def test_rmdir_refused_by_service(dummy, fs, tree) -> None:
    dummy.requests.clear()
    with pytest.raises(OSError) as info:
        await fs._rmdir(f"{tree}/a")
    assert info.value.errno == errno.ENOTEMPTY
    # The blob API refuses a folder with things in: no listing first.
    assert dummy.count("ListBlobs") == 0
    assert dummy.count("DeleteBlob") == 1


async def test_pending_copy(dummy, fs, tree) -> None:
    fs.copy_poll_interval = 0.001
    dummy.copy_pending_reads = 2
    dummy.requests.clear()
    await fs._cp_file(f"{tree}/z.txt", f"{tree}/copy.txt")
    # Pending twice, then done.
    assert dummy.count("GetBlobProperties") == 3

    dummy.copy_outcome = "failed"
    with pytest.raises(OSError) as info:
        await fs._cp_file(f"{tree}/z.txt", f"{tree}/copy2.txt")
    assert info.value.errno == errno.EIO


async def test_cp_missing_container(dummy, fs, tree) -> None:
    with pytest.raises(FileNotFoundError) as info:
        await fs._cp_file(f"{tree}/z.txt", "/nocontainer/copy")
    assert info.value.filename == "/nocontainer/copy"


async def test_unknown_hns_marks_folders(dummy, make_fs, root) -> None:
    dummy.failures["GetAccountInformation"] = (403, "AuthorizationPermissionMismatch")
    dummy.failures["GetAccessControl"] = (403, "AuthorizationFailure")
    fs = make_fs(is_hns_enabled=None)
    await fs._pipe_file(f"{root}/a/x.txt", b"1")

    # A blob marking a folder: a real folder with HNS, a marker blob without.
    await fs._makedirs(f"{root}/a/d")
    assert "work/a/d" in dummy.folders("c")
    await fs._pipe_file(f"{root}/a/d/in.txt", b"2")
    # The folder's blob and its prefix are one entry.
    assert sorted(await fs._ls(f"{root}/a", detail=False)) == [
        f"{root}/a/d",
        f"{root}/a/x.txt",
    ]
    assert (await fs._info(f"{root}/a/d"))["type"] == "directory"

    # Unknown: emptied folders are deleted, marker blobs too.
    await fs._rm(f"{root}/a", recursive=True)
    assert [n for n in dummy.containers["c"].blobs if n.startswith("work/a")] == []
    assert fs.is_hns_enabled is None


async def test_containers(dummy, fs, hns) -> None:
    # The root lists the containers, but makes none: Azure's own means do.
    with pytest.raises(PermissionError):
        await fs._makedirs("/newcontainer")
    with pytest.raises(PermissionError):
        await fs._mkdir("/newcontainer")
    with pytest.raises(FileNotFoundError):
        await fs._makedirs("/newcontainer/d")
    assert "newcontainer" not in dummy.containers

    dummy.create_container("newcontainer")
    await fs._makedirs("/newcontainer", exist_ok=True)
    await fs._makedirs("/newcontainer/d")
    assert await fs._ls("/", detail=False) == ["/c", "/newcontainer"]
    assert await fs._ls("", detail=False) == ["/c", "/newcontainer"]
    if hns:
        # Its folder d is in it.
        with pytest.raises(OSError) as info:
            await fs._rmdir("/newcontainer")
        assert info.value.errno == errno.ENOTEMPTY
    else:
        await fs._rmdir("/newcontainer")
    await fs._rmdir("/c")
    assert "c" not in dummy.containers


async def test_no_listings_cache(make_fs) -> None:
    # Other clients change the storage too: no listing is ever cached.
    assert make_fs().dircache.use_listings_cache is False


async def test_last_modified_is_in_utc(fs, root) -> None:
    # The Data Lake API's listings, as the blob API's, give aware times, in UTC.
    # A folder that is only a prefix has none.
    await fs._pipe_file(f"{root}/d/x.txt", b"x")
    entries = [*await fs._ls(f"{root}/d"), *await fs._ls(root)]
    entries.append(await fs._info(f"{root}/d/x.txt"))
    times = [e["last_modified"] for e in entries if "last_modified" in e]
    assert len(times) >= 2
    assert {t.utcoffset() for t in times} == {timedelta(0)}
