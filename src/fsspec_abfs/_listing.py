"""A container's blobs below a prefix, by ListBlobs without a delimiter, the next page asked for while one is parsed.

The SDK's pager parses a whole page into ``BlobProperties`` before its continuation token exists, so each page costs
the service's time to answer it, then the client's to parse it, one after another. Measured on Azure Open Datasets, a
page of 5000 blobs took about 0.4 s to answer and 0.55 s to parse.

This sends ListBlobs itself, through the container client's own pipeline, with its credential and policies. It finds
``<NextMarker>`` in a page's raw XML, asks for the next page at once, then parses the page into fsspec entries with
``xml.etree``, in a thread, while the next page comes. The entries are those `AbfsFileSystem._ils` makes from the SDK's
``BlobProperties``.
"""

from __future__ import annotations

import asyncio
import re
import xml.etree.ElementTree as ET
from email.utils import parsedate_to_datetime
from typing import TYPE_CHECKING, Any
from urllib.parse import quote, unquote, urlencode
from xml.sax.saxutils import unescape

from azure.core.exceptions import HttpResponseError
from azure.core.rest import HttpRequest

if TYPE_CHECKING:
    from collections.abc import AsyncIterator

    from azure.storage.blob.aio import ContainerClient

_PAGE = 5000
"""The most blobs ListBlobs returns in a page."""

_NEXT_MARKER = re.compile(rb"<NextMarker>([^<]*)</NextMarker>")


async def list_blobs_flat(
    client: ContainerClient, container: str, prefix: str
) -> AsyncIterator[dict[str, Any]]:
    """Each blob whose name starts with ``prefix``, but the prefix's own, in name order, as fsspec entries.

    A page's error, e.g. a missing container's 404, raises an `HttpResponseError` with its ``error_code``, as the
    SDK's does.
    """
    ahead: asyncio.Task[bytes] | None = asyncio.ensure_future(
        _fetch(client, prefix, None)
    )
    try:
        while ahead is not None:
            body = await ahead
            marker = next_marker(body)
            # Asked for before this page is parsed: its answer comes meanwhile.
            ahead = (
                asyncio.ensure_future(_fetch(client, prefix, marker))
                if marker
                else None
            )
            for entry in await asyncio.to_thread(entries, container, body, prefix):
                yield entry
    finally:
        if ahead is not None:
            ahead.cancel()
            # Its own outcome is dropped; the caller's cancellation still raises.
            await asyncio.gather(ahead, return_exceptions=True)


async def _fetch(client: ContainerClient, prefix: str, marker: str | None) -> bytes:
    """One page of ListBlobs, its raw XML."""
    query = {
        "restype": "container",
        "comp": "list",
        "include": "metadata",
        "maxresults": str(_PAGE),
    }
    if prefix:
        query["prefix"] = prefix
    if marker:
        query["marker"] = marker
    url = client.url
    url += ("&" if "?" in url else "?") + urlencode(query, quote_via=quote)
    request = HttpRequest(
        "GET",
        url,
        headers={"x-ms-version": client.api_version, "Accept": "application/xml"},
    )
    response = await client._client._send_request(request)
    body = await response.read()
    if response.status_code != 200:
        error = HttpResponseError(response=response)
        # As the SDK's errors carry it, for the file system's mapping to OSError.
        error.error_code = response.headers.get("x-ms-error-code")
        raise error
    return body


def next_marker(body: bytes) -> str | None:
    """The page's continuation token, found without parsing the page, or None for the last page."""
    # It comes last, after the blobs: search from the end.
    tail = body[-4096:]
    found = _NEXT_MARKER.search(tail) or _NEXT_MARKER.search(body)
    if found is None or not found.group(1):
        return None
    return unescape(found.group(1).decode())


def entries(container: str, body: bytes, prefix: str) -> list[dict[str, Any]]:
    """The fsspec entries of a page's blobs, as `AbfsFileSystem` makes them from the SDK's, but the prefix's own."""
    root = ET.fromstring(body)
    blobs = root.find("Blobs")
    found: list[dict[str, Any]] = []
    for blob in [] if blobs is None else blobs.iterfind("Blob"):
        name_element = blob.find("Name")
        name = name_element.text or "" if name_element is not None else ""
        if name_element is not None and name_element.get("Encoded") == "true":
            name = unquote(name)
        if name == prefix:
            continue  # The folder's own marker blob.

        properties = blob.find("Properties")
        get = (lambda tag: None) if properties is None else properties.findtext
        modified = get("Last-Modified")
        common = {
            "name": f"/{container}/{name.rstrip('/')}",
            "last_modified": parsedate_to_datetime(modified) if modified else None,
            "etag": get("Etag"),
        }
        metadata = blob.find("Metadata")
        folder = (
            metadata is not None
            and (metadata.findtext("hdi_isfolder") or "").lower() == "true"
        )
        if folder:
            found.append({**common, "size": 0, "type": "directory"})
        else:
            found.append(
                {
                    **common,
                    "size": int(get("Content-Length") or 0),
                    "type": "file",
                    "content_type": get("Content-Type"),
                }
            )
    return found
