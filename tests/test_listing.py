"""abfs's own ListBlobs paging, checked against the SDK's listing of the same blobs."""

from __future__ import annotations

import pytest

from fsspec_abfs._listing import entries, list_blobs_flat, next_marker
from fsspec_abfs.abfs import _blob_entry


@pytest.mark.flat_only
async def test_lists_what_the_sdk_lists_over_several_pages(fs, dummy, root) -> None:
    for i in range(23):
        await fs._pipe_file(f"{root}/d{i % 3}/f{i:02}.txt", b"x" * i)
    container, key = root.lstrip("/").split("/", 1)
    # A folder's marker blob, as Hadoop's drivers write: a directory entry.
    client = fs._blob_service().get_container_client(container)
    await client.upload_blob(f"{key}/d0/sub", b"", metadata={"hdi_isfolder": "true"})
    dummy.page_size = 5
    prefix = f"{key}/"

    sdk = [
        _blob_entry(container, b)
        async for b in client.list_blobs(name_starts_with=prefix, include=["metadata"])
        if b.name != prefix
    ]
    dummy.requests.clear()
    ours = [e async for e in list_blobs_flat(client, container, prefix)]

    assert [(e["name"], e["type"], e["size"]) for e in ours] == [
        (e["name"], e["type"], e["size"]) for e in sdk
    ]
    assert len(dummy.requests) == 5  # 24 blobs, 5 a page.


@pytest.mark.flat_only
async def test_a_missing_container_raises_as_the_sdk_does(fs, dummy) -> None:
    client = fs._blob_service().get_container_client("nocontainer")
    with pytest.raises(Exception) as raised:
        _ = [e async for e in list_blobs_flat(client, "nocontainer", "")]
    assert raised.value.status_code == 404
    assert raised.value.error_code == "ContainerNotFound"


def test_next_marker_from_the_raw_page() -> None:
    assert next_marker(b"<E><Blobs/><NextMarker>2!a&amp;b</NextMarker></E>") == "2!a&b"
    assert next_marker(b"<E><Blobs/><NextMarker /></E>") is None
    assert next_marker(b"<E><Blobs/><NextMarker></NextMarker></E>") is None


def test_an_encoded_name_is_decoded() -> None:
    body = (
        b'<EnumerationResults><Blobs><Blob><Name Encoded="true">a%0Bb</Name>'
        b"<Properties><Content-Length>3</Content-Length></Properties></Blob></Blobs>"
        b"<NextMarker /></EnumerationResults>"
    )
    assert [e["name"] for e in entries("c", body, "")] == ["/c/a\x0bb"]
