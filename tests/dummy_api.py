"""A FastAPI app simulating an Azure storage account's blob and Data Lake (dfs) REST APIs, for tests.

It answers the operations `AbfsFileSystem` sends through the Azure SDK, over real HTTP, with Azure's error format: an
``x-ms-error-code`` header, and an XML body on the blob API or a JSON body on the dfs API, from which the SDK picks
its exception and its ``error_code``. Both APIs serve one store, under two path prefixes of one server: ``/blob`` and
``/dfs``, which `AbfsFileSystem` takes as its endpoints for tests.

The ``hns`` switch chooses the kind of account. With a hierarchical namespace, folders are paths of their own, seen by
the blob API as empty blobs with the metadata ``hdi_isfolder=true``, and writing a blob creates its missing parents.
Without one, folders are prefixes, and the dfs API refuses every request with ``HierarchicalNamespaceNotEnabled``.

Behaviour that Azure's references leave open is marked as an assumption. Authentication is not checked.
"""

from __future__ import annotations

import asyncio
import itertools
import json
import logging
import socket
import time
import urllib.parse
import uuid
import xml.etree.ElementTree as ET
from contextlib import closing
from dataclasses import dataclass, field
from email.utils import formatdate
from typing import Any
from xml.sax.saxutils import escape

from fastapi import FastAPI, Request, Response
from hypercorn.asyncio import serve
from hypercorn.config import Config

log = logging.getLogger(__name__)

app = FastAPI()

_METHODS = ["GET", "HEAD", "PUT", "DELETE", "POST", "PATCH"]


class ApiError(Exception):
    """An error answered in Azure's format: XML on the blob API, JSON on the dfs API."""

    def __init__(self, status: int, code: str, message: str = "") -> None:
        super().__init__(message or code)
        self.status = status
        self.code = code
        self.message = message or code


def _http_date(t: float) -> str:
    return formatdate(t, usegmt=True)


### State


@dataclass
class Blob:
    """A blob, or a folder of an account with a hierarchical namespace."""

    data: bytes = b""
    metadata: dict[str, str] = field(default_factory=dict)
    content_type: str = "application/octet-stream"
    etag: str = ""
    last_modified: float = 0.0
    copy_id: str | None = None
    copy_status: str | None = None
    copy_pending_reads: int = 0
    """The property reads left that answer the copy as pending."""

    @property
    def is_folder(self) -> bool:
        return self.metadata.get("hdi_isfolder", "").lower() == "true"


@dataclass
class Container:
    etag: str
    last_modified: float
    blobs: dict[str, Blob] = field(default_factory=dict)
    staged: dict[str, dict[str, bytes]] = field(default_factory=dict)
    """The uncommitted blocks, by blob name, then block id."""


@dataclass(frozen=True)
class Recorded:
    """A request served: its API, the REST operation's name, e.g. ``ListBlobs``, its method and path.

    ``bytes_in`` is the request body's size and ``bytes_out`` the response body's, the data that a real network would
    carry. ``started`` and ``ended`` are `time.perf_counter` readings at its arrival and its answer: the tests find
    from them which requests overlapped, so they include the injected ``DummyStorage.delay``.
    """

    service: str
    operation: str
    method: str
    path: str
    params: dict[str, str]
    bytes_in: int = 0
    bytes_out: int = 0
    started: float = 0.0
    ended: float = 0.0


@dataclass
class DummyStorage:
    """The containers of the dummy account, the requests served, and the test switches."""

    hns: bool = False
    """Whether the account has a hierarchical namespace."""

    containers: dict[str, Container] = field(default_factory=dict)
    requests: list[Recorded] = field(default_factory=list)

    failures: dict[str, tuple[int, str]] = field(default_factory=dict)
    """Operations that fail, by name, e.g. ``{"GetAccountInformation": (403, "AuthorizationPermissionMismatch")}``."""

    page_size: int = 5000
    """The most entries of a listing's page, Azure's default, unless a request asks fewer."""

    copy_pending_reads: int = 0
    """Copies answer pending, then as many property reads as this, before ``copy_outcome``."""

    copy_outcome: str = "success"

    delay: float = 0.0
    """Seconds each request waits before its answer, without blocking others.

    The handlers answer at once, so without it no two requests would overlap and the log could not tell concurrent
    requests from sequential ones. The tests set it to see which requests a client sends together.
    """

    _etags: Any = field(default_factory=lambda: itertools.count(1))

    def reset(self, *, hns: bool = False) -> None:
        self.hns = hns
        self.containers.clear()
        self.requests.clear()
        self.failures.clear()
        self.page_size = 5000
        self.copy_pending_reads = 0
        self.copy_outcome = "success"
        self.delay = 0.0

    # For tests.

    def count(self, operation: str) -> int:
        """The requests served of an operation, e.g. ``ListBlobs``."""
        return sum(1 for r in self.requests if r.operation == operation)

    def operations(self) -> list[str]:
        return [r.operation for r in self.requests]

    def create_container(self, name: str) -> None:
        if name not in self.containers:
            self.containers[name] = Container(self.new_etag(), time.time())

    def put(
        self, container: str, name: str, data: bytes = b"", **metadata: str
    ) -> None:
        """Write a blob, as a Put Blob does, creating its container if missing."""
        self.create_container(container)
        self.write(container, name, Blob(data=data, metadata=metadata))

    def files(self, container: str) -> dict[str, bytes]:
        """The data of the blobs that are no folders of a hierarchical namespace."""
        blobs = self.container(container).blobs
        return {
            n: b.data
            for n, b in sorted(blobs.items())
            if not (self.hns and b.is_folder)
        }

    def folders(self, container: str) -> list[str]:
        """The folders of a hierarchical namespace, or the marker blobs without one."""
        return sorted(
            n for n, b in self.container(container).blobs.items() if b.is_folder
        )

    # The model.

    def new_etag(self) -> str:
        return f'"0x8DE{next(self._etags):012X}"'

    def container(self, name: str, code: str = "ContainerNotFound") -> Container:
        if name not in self.containers:
            raise ApiError(404, code, "The specified container does not exist.")
        return self.containers[name]

    def write(self, container: str, name: str, blob: Blob) -> Blob:
        c = self.container(container)
        if self.hns:
            # Assumed: a write over a folder, or under a file, is refused as Azure does on such a path.
            existing = c.blobs.get(name)
            if existing is not None and existing.is_folder and not blob.is_folder:
                raise ApiError(
                    409, "OperationNotAllowedOnDirectory", "The path is a directory."
                )
            for parent in _ancestors(name):
                p = c.blobs.get(parent)
                if p is not None and not p.is_folder:
                    raise ApiError(409, "PathConflict", f"{parent} is a file.")
            for parent in _ancestors(name):
                if parent not in c.blobs:
                    c.blobs[parent] = self._stamp(
                        Blob(metadata={"hdi_isfolder": "true"})
                    )
        c.blobs[name] = self._stamp(blob)
        return blob

    def _stamp(self, blob: Blob) -> Blob:
        blob.etag = self.new_etag()
        blob.last_modified = time.time()
        return blob

    def get(self, container: str, name: str, code: str = "BlobNotFound") -> Blob:
        blobs = self.container(container).blobs
        if name not in blobs:
            raise ApiError(404, code, "The specified blob does not exist.")
        return blobs[name]

    def children(self, container: str, name: str) -> list[str]:
        prefix = f"{name}/" if name else ""
        return [n for n in self.container(container).blobs if n.startswith(prefix)]


def _ancestors(name: str) -> list[str]:
    parts = name.split("/")[:-1]
    return ["/".join(parts[: i + 1]) for i in range(len(parts))]


_storage = DummyStorage()


def dummy_storage() -> DummyStorage:
    return _storage


### HTTP


def _common_headers(request: Request) -> dict[str, str]:
    return {
        "x-ms-request-id": str(uuid.uuid4()),
        "x-ms-version": request.headers.get("x-ms-version", "2025-01-05"),
        "Date": _http_date(time.time()),
    }


def _error(service: str, request: Request, e: ApiError) -> Response:
    headers = {**_common_headers(request), "x-ms-error-code": e.code}
    if request.method == "HEAD":
        # A HEAD answer has no body: the SDK reads the code from the header.
        return Response(status_code=e.status, headers=headers)
    if service == "dfs":
        body = json.dumps({"error": {"code": e.code, "message": e.message}})
        return Response(body, e.status, headers, media_type="application/json")
    body = (
        '<?xml version="1.0" encoding="utf-8"?>'
        f"<Error><Code>{escape(e.code)}</Code><Message>{escape(e.message)}</Message></Error>"
    )
    return Response(body, e.status, headers, media_type="application/xml")


def _blob_operation(  # noqa: C901 - one branch per REST operation, as Azure's reference lists them.
    method: str, container: str, key: str, q: Any, h: Any
) -> str:
    """The REST operation a blob API request is, by its path, method, query and headers, or "" if not simulated."""
    if not container:
        if q.get("restype") == "account" and q.get("comp") == "properties":
            return "GetAccountInformation"
        if q.get("comp") == "list":
            return "ListContainers"
    elif not key and q.get("restype") == "container":
        if method == "PUT":
            return "CreateContainer"
        if method == "DELETE":
            return "DeleteContainer"
        if q.get("comp") == "list":
            return "ListBlobs"
        if method in ("GET", "HEAD") and "comp" not in q:
            return "GetContainerProperties"
    elif key:
        if method == "PUT":
            comp = q.get("comp")
            if comp == "block":
                return "PutBlock"
            if comp == "blocklist":
                return "PutBlockList"
            if comp == "metadata":
                return "SetBlobMetadata"
            if comp is None:
                return "CopyBlob" if "x-ms-copy-source" in h else "PutBlob"
        if "comp" not in q:
            return {
                "GET": "GetBlob",
                "HEAD": "GetBlobProperties",
                "DELETE": "DeleteBlob",
            }.get(method, "")
    return ""


def _dfs_operation(method: str, container: str, key: str, q: Any, h: Any) -> str:
    if not container:
        return ""
    elif method == "GET" and not key and q.get("resource") == "filesystem":
        return "ListPaths"
    elif method == "HEAD" and q.get("action") == "getAccessControl":
        return "GetAccessControl"
    elif key and method == "PUT" and "x-ms-rename-source" in h:
        return "RenamePath"
    elif key and method == "PUT" and q.get("resource") in ("directory", "file"):
        return "CreatePath"
    elif key and method == "DELETE":
        return "DeletePath"
    else:
        return ""


@app.api_route("/{service}", methods=_METHODS)
@app.api_route("/{service}/{rest:path}", methods=_METHODS)
async def handle(service: str, request: Request, rest: str = "") -> Response:
    started = time.perf_counter()
    container, _, key = rest.partition("/")
    key = key.strip("/")
    q, h = request.query_params, request.headers
    body = await request.body()
    operations = {"blob": _blob_operation, "dfs": _dfs_operation}
    operation = (
        operations[service](request.method, container, key, q, h)
        if service in operations
        else ""
    )
    storage = _storage
    if storage.delay:
        await asyncio.sleep(storage.delay)
    response = _answer(storage, service, operation, container, key, request, body)
    # Recorded once answered, so that its end and its size are known; a HEAD
    # answer carries a Content-Length but no body.
    storage.requests.append(
        Recorded(
            service,
            operation,
            request.method,
            f"{container}/{key}".rstrip("/"),
            dict(q),
            bytes_in=len(body),
            bytes_out=0 if request.method == "HEAD" else len(response.body),
            started=started,
            ended=time.perf_counter(),
        )
    )
    return response


def _answer(
    storage: DummyStorage,
    service: str,
    operation: str,
    container: str,
    key: str,
    request: Request,
    body: bytes,
) -> Response:
    q, h = request.query_params, request.headers
    try:
        if not operation:
            raise ApiError(
                400,
                "UnsupportedByDummy",
                f"{request.method} {request.url} is not simulated.",
            )
        if operation in storage.failures:
            raise ApiError(*storage.failures[operation])
        if service == "dfs" and not storage.hns:
            raise ApiError(
                400,
                "HierarchicalNamespaceNotEnabled",
                "This operation is only supported on a hierarchical namespace account.",
            )
        handler = _handlers[operation]
        response = handler(storage, container, key, q, h, body)
    except ApiError as e:
        log.debug("%s %s: %s %s", request.method, request.url, e.status, e.code)
        return _error(service, request, e)
    response.headers.update(_common_headers(request))
    return response


def _metadata(h: Any) -> dict[str, str]:
    return {
        k[len("x-ms-meta-") :]: v
        for k, v in h.items()
        if k.lower().startswith("x-ms-meta-")
    }


def _check_conditions(blob: Blob | None, h: Any) -> None:
    """If-Match and If-None-Match, as the blob API answers them on a write or a read."""
    if_match = h.get("if-match")
    if if_match is not None and (blob is None or if_match not in ("*", blob.etag)):
        raise ApiError(
            412,
            "ConditionNotMet",
            "The condition specified using HTTP conditional header(s) is not met.",
        )
    if_none_match = h.get("if-none-match")
    if (
        if_none_match is not None
        and blob is not None
        and if_none_match in ("*", blob.etag)
    ):
        raise ApiError(409, "BlobAlreadyExists", "The specified blob already exists.")


def _written(blob: Blob, status: int = 201) -> Response:
    return Response(
        status_code=status,
        headers={
            "ETag": blob.etag,
            "Last-Modified": _http_date(blob.last_modified),
            "x-ms-request-server-encrypted": "true",
        },
    )


def _page(
    items: list[tuple[str, Any]],
    q: Any,
    size_param: str,
    marker_param: str,
    page_size: int,
):
    """A page of a listing sorted by name, from its marker, the name of the first entry to come."""
    marker = q.get(marker_param) or ""
    size = min(int(q.get(size_param) or page_size), page_size)
    items = [i for i in items if i[0] >= marker]
    page, rest = items[:size], items[size:]
    return page, (rest[0][0] if rest else "")


# The blob API.


def get_account_information(storage, container, key, q, h, body) -> Response:
    return Response(
        headers={
            "x-ms-sku-name": "Standard_LRS",
            "x-ms-account-kind": "StorageV2",
            "x-ms-is-hns-enabled": str(storage.hns).lower(),
        }
    )


def list_containers(storage, container, key, q, h, body) -> Response:
    prefix = q.get("prefix") or ""
    items = [
        (n, c) for n, c in sorted(storage.containers.items()) if n.startswith(prefix)
    ]
    page, next_marker = _page(items, q, "maxresults", "marker", storage.page_size)
    xml = "".join(
        f"<Container><Name>{escape(n)}</Name><Properties>"
        f"<Last-Modified>{_http_date(c.last_modified)}</Last-Modified><Etag>{c.etag}</Etag>"
        "<LeaseStatus>unlocked</LeaseStatus><LeaseState>available</LeaseState>"
        "</Properties></Container>"
        for n, c in page
    )
    return _xml(
        f'<EnumerationResults ServiceEndpoint="http://dummy/"><Prefix>{escape(prefix)}</Prefix>'
        f"<Containers>{xml}</Containers><NextMarker>{escape(next_marker)}</NextMarker></EnumerationResults>"
    )


def create_container(storage, container, key, q, h, body) -> Response:
    if container in storage.containers:
        raise ApiError(
            409, "ContainerAlreadyExists", "The specified container already exists."
        )
    storage.create_container(container)
    c = storage.containers[container]
    return Response(
        status_code=201,
        headers={"ETag": c.etag, "Last-Modified": _http_date(c.last_modified)},
    )


def delete_container(storage, container, key, q, h, body) -> Response:
    storage.container(container)
    del storage.containers[container]
    return Response(status_code=202)


def get_container_properties(storage, container, key, q, h, body) -> Response:
    c = storage.container(container)
    return Response(
        headers={
            "ETag": c.etag,
            "Last-Modified": _http_date(c.last_modified),
            "x-ms-lease-status": "unlocked",
            "x-ms-lease-state": "available",
        }
    )


def _blob_xml(name: str, blob: Blob, with_metadata: bool) -> str:
    # A listing's ETag has no quotes, where a response's header has them, as Azure answers.
    metadata = ""
    if with_metadata:
        metadata = (
            "<Metadata>"
            + "".join(f"<{k}>{escape(v)}</{k}>" for k, v in blob.metadata.items())
            + "</Metadata>"
        )
    return (
        f"<Blob><Name>{escape(name)}</Name><Properties>"
        f"<Last-Modified>{_http_date(blob.last_modified)}</Last-Modified>"
        f"<Etag>{blob.etag.strip(chr(34))}</Etag>"
        f"<Content-Length>{len(blob.data)}</Content-Length>"
        f"<Content-Type>{escape(blob.content_type)}</Content-Type>"
        "<BlobType>BlockBlob</BlobType><LeaseStatus>unlocked</LeaseStatus>"
        "<LeaseState>available</LeaseState><ServerEncrypted>true</ServerEncrypted>"
        f"</Properties>{metadata}</Blob>"
    )


def list_blobs(storage, container, key, q, h, body) -> Response:
    c = storage.container(container)
    prefix = q.get("prefix") or ""
    delimiter = q.get("delimiter") or ""
    with_metadata = "metadata" in (q.get("include") or "").split(",")
    items: dict[str, Any] = {}
    for name in sorted(c.blobs):
        if not name.startswith(prefix):
            continue
        rest = name[len(prefix) :]
        if delimiter and delimiter in rest:
            p = prefix + rest.split(delimiter)[0] + delimiter
            items.setdefault(p, None)
        else:
            items[name] = c.blobs[name]
    page, next_marker = _page(
        sorted(items.items()), q, "maxresults", "marker", storage.page_size
    )
    xml = "".join(
        f"<BlobPrefix><Name>{escape(n)}</Name></BlobPrefix>"
        if b is None
        else _blob_xml(n, b, with_metadata)
        for n, b in page
    )
    return _xml(
        f'<EnumerationResults ServiceEndpoint="http://dummy/" ContainerName="{escape(container)}">'
        f"<Prefix>{escape(prefix)}</Prefix><Delimiter>{escape(delimiter)}</Delimiter>"
        f"<Blobs>{xml}</Blobs><NextMarker>{escape(next_marker)}</NextMarker></EnumerationResults>"
    )


def _xml(text: str) -> Response:
    return Response(
        '<?xml version="1.0" encoding="utf-8"?>' + text, media_type="application/xml"
    )


def _blob_headers(blob: Blob) -> dict[str, str]:
    headers = {
        "ETag": blob.etag,
        "Last-Modified": _http_date(blob.last_modified),
        "Content-Type": blob.content_type,
        "x-ms-blob-type": "BlockBlob",
        "x-ms-server-encrypted": "true",
        "Accept-Ranges": "bytes",
        **{f"x-ms-meta-{k}": v for k, v in blob.metadata.items()},
    }
    if blob.copy_status:
        headers["x-ms-copy-id"] = blob.copy_id or ""
        headers["x-ms-copy-status"] = blob.copy_status
    return headers


def get_blob_properties(storage, container, key, q, h, body) -> Response:
    blob = storage.get(container, key)
    if blob.copy_status == "pending":
        if blob.copy_pending_reads > 0:
            blob.copy_pending_reads -= 1
        else:
            blob.copy_status = storage.copy_outcome
    return Response(
        headers={**_blob_headers(blob), "Content-Length": str(len(blob.data))}
    )


def get_blob(storage, container, key, q, h, body) -> Response:
    blob = storage.get(container, key)
    _check_conditions(blob, h)
    size = len(blob.data)
    headers = _blob_headers(blob)
    ranged = h.get("x-ms-range") or h.get("range")
    if not ranged:
        return Response(blob.data, 200, headers, media_type=blob.content_type)
    start_text, _, end_text = ranged.removeprefix("bytes=").partition("-")
    start = int(start_text)
    end = min(int(end_text), size - 1) if end_text else size - 1
    if start >= size:
        raise ApiError(
            416,
            "InvalidRange",
            "The range specified is invalid for the current size of the resource.",
        )
    headers["Content-Range"] = f"bytes {start}-{end}/{size}"
    return Response(
        blob.data[start : end + 1], 206, headers, media_type=blob.content_type
    )


def put_blob(storage, container, key, q, h, body) -> Response:
    c = storage.container(container)
    _check_conditions(c.blobs.get(key), h)
    blob = Blob(
        data=body,
        metadata=_metadata(h),
        content_type=h.get("x-ms-blob-content-type") or "application/octet-stream",
    )
    return _written(storage.write(container, key, blob))


def put_block(storage, container, key, q, h, body) -> Response:
    c = storage.container(container)
    c.staged.setdefault(key, {})[q["blockid"]] = body
    return Response(status_code=201)


def put_block_list(storage, container, key, q, h, body) -> Response:
    c = storage.container(container)
    _check_conditions(c.blobs.get(key), h)
    staged = c.staged.get(key, {})
    ids = [e.text or "" for e in ET.fromstring(body)]
    if any(i not in staged for i in ids):
        raise ApiError(400, "InvalidBlockList", "The specified block list is invalid.")
    blob = Blob(
        data=b"".join(staged[i] for i in ids),
        metadata=_metadata(h),
        content_type=h.get("x-ms-blob-content-type") or "application/octet-stream",
    )
    storage.write(container, key, blob)
    c.staged.pop(key, None)
    return _written(blob)


def set_blob_metadata(storage, container, key, q, h, body) -> Response:
    blob = storage.get(container, key)
    blob.metadata = _metadata(h)
    storage._stamp(blob)
    return _written(blob, 200)


def delete_blob(storage, container, key, q, h, body) -> Response:
    blob = storage.get(container, key)
    if (
        storage.hns
        and blob.is_folder
        and any(n != key for n in storage.children(container, key))
    ):
        raise ApiError(
            409,
            "DirectoryIsNotEmpty",
            "This operation is not permitted on a non-empty directory.",
        )
    del storage.containers[container].blobs[key]
    return Response(status_code=202)


def copy_blob(storage, container, key, q, h, body) -> Response:
    source = urllib.parse.urlparse(h["x-ms-copy-source"])
    _, _, source_path = urllib.parse.unquote(source.path).lstrip("/").partition("/")
    source_container, _, source_key = source_path.partition("/")
    storage.container(container)
    try:
        original = storage.get(source_container, source_key)
    except ApiError as e:
        raise ApiError(
            404, "CannotVerifyCopySource", "The specified blob does not exist."
        ) from e
    # Assumed: the data is in place at once, even while the copy answers pending.
    blob = Blob(
        data=original.data,
        metadata=_metadata(h) or dict(original.metadata),
        content_type=original.content_type,
        copy_id=str(uuid.uuid4()),
        copy_status="pending" if storage.copy_pending_reads else "success",
        copy_pending_reads=storage.copy_pending_reads,
    )
    storage.write(container, key, blob)
    return Response(
        status_code=202,
        headers={
            "ETag": blob.etag,
            "Last-Modified": _http_date(blob.last_modified),
            "x-ms-copy-id": blob.copy_id or "",
            "x-ms-copy-status": blob.copy_status or "",
        },
    )


# The Data Lake (dfs) API, only with a hierarchical namespace.


def _path_entry(name: str, blob: Blob) -> dict[str, str]:
    entry = {
        "name": name,
        "lastModified": _http_date(blob.last_modified),
        "etag": blob.etag.strip('"'),
        "contentLength": str(0 if blob.is_folder else len(blob.data)),
        "owner": "$superuser",
        "group": "$superuser",
        "permissions": "rwxr-x---",
    }
    if blob.is_folder:
        entry["isDirectory"] = "true"
    return entry


def list_paths(storage, container, key, q, h, body) -> Response:
    c = storage.container(container, "FilesystemNotFound")
    directory = (q.get("directory") or "").strip("/")
    if directory:
        folder = c.blobs.get(directory)
        # Assumed: a file is not found as a directory.
        if folder is None or not folder.is_folder:
            raise ApiError(404, "PathNotFound", "The specified path does not exist.")
    recursive = (q.get("recursive") or "").lower() == "true"
    prefix = f"{directory}/" if directory else ""
    names = [
        n
        for n in sorted(c.blobs)
        if n.startswith(prefix) and (recursive or "/" not in n[len(prefix) :])
    ]
    page, next_marker = _page(
        [(n, c.blobs[n]) for n in names],
        q,
        "maxResults",
        "continuation",
        storage.page_size,
    )
    headers = (
        {"x-ms-continuation": urllib.parse.quote(next_marker)} if next_marker else {}
    )
    text = json.dumps({"paths": [_path_entry(n, b) for n, b in page]})
    response = Response(text, headers=headers, media_type="application/json")
    return response


def create_path(storage, container, key, q, h, body) -> Response:
    c = storage.container(container, "FilesystemNotFound")
    existing = c.blobs.get(key)
    if h.get("if-none-match") == "*" and existing is not None:
        raise ApiError(409, "PathAlreadyExists", "The specified path already exists.")
    if q.get("resource") == "directory":
        if existing is not None and existing.is_folder:
            # Assumed: an existing folder is kept, with all in it.
            return _written(existing)
        if existing is not None:
            raise ApiError(
                409,
                "PathConflict",
                "The specified path, or an element of the path, exists and its resource type is invalid for this operation.",
            )
        blob = Blob(metadata={"hdi_isfolder": "true"})
    else:
        blob = Blob()
    return _written(storage.write(container, key, blob))


def rename_path(storage, container, key, q, h, body) -> Response:
    source = urllib.parse.unquote(h["x-ms-rename-source"].partition("?")[0]).lstrip("/")
    source_container, _, source_key = source.partition("/")
    source_key = source_key.strip("/")
    storage.container(container, "FilesystemNotFound")
    try:
        moved = storage.get(source_container, source_key)
    except ApiError as e:
        raise ApiError(
            404,
            "SourcePathNotFound",
            "The source path for a rename operation does not exist.",
        ) from e
    blobs = storage.containers[container].blobs
    parent = key.rpartition("/")[0]
    if parent and (parent not in blobs or not blobs[parent].is_folder):
        raise ApiError(
            404,
            "RenameDestinationParentPathNotFound",
            "The parent directory of the destination path does not exist.",
        )
    if source_container == container and (
        key == source_key or key.startswith(source_key + "/")
    ):
        raise ApiError(
            400, "InvalidDestinationPath", "The destination is within the source."
        )
    existing = blobs.get(key)
    if existing is not None and (existing.is_folder or moved.is_folder):
        # Assumed: only a file replaces a file.
        raise ApiError(409, "PathAlreadyExists", "The specified path already exists.")
    source_blobs = storage.containers[source_container].blobs
    for name in [
        source_key,
        *(n for n in storage.children(source_container, source_key) if n != source_key),
    ]:
        blobs[key + name[len(source_key) :]] = source_blobs.pop(name)
    return Response(
        status_code=201,
        headers={"ETag": moved.etag, "Last-Modified": _http_date(moved.last_modified)},
    )


def delete_path(storage, container, key, q, h, body) -> Response:
    storage.container(container, "FilesystemNotFound")
    blob = storage.get(container, key, "PathNotFound")
    inside = (
        [n for n in storage.children(container, key) if n != key]
        if blob.is_folder
        else []
    )
    if inside and (q.get("recursive") or "").lower() != "true":
        raise ApiError(
            409,
            "DirectoryNotEmpty",
            "The recursive query parameter value must be true to delete a non-empty directory.",
        )
    blobs = storage.containers[container].blobs
    for name in [key, *inside]:
        del blobs[name]
    return Response(status_code=200)


def get_access_control(storage, container, key, q, h, body) -> Response:
    storage.container(container, "FilesystemNotFound")
    if key:
        storage.get(container, key, "PathNotFound")
    return Response(
        headers={
            "x-ms-owner": "$superuser",
            "x-ms-group": "$superuser",
            "x-ms-permissions": "rwxr-x---",
            "x-ms-acl": "user::rwx,group::r-x,other::---",
        }
    )


_handlers = {
    "GetAccountInformation": get_account_information,
    "ListContainers": list_containers,
    "CreateContainer": create_container,
    "DeleteContainer": delete_container,
    "GetContainerProperties": get_container_properties,
    "ListBlobs": list_blobs,
    "GetBlobProperties": get_blob_properties,
    "GetBlob": get_blob,
    "PutBlob": put_blob,
    "PutBlock": put_block,
    "PutBlockList": put_block_list,
    "SetBlobMetadata": set_blob_metadata,
    "DeleteBlob": delete_blob,
    "CopyBlob": copy_blob,
    "ListPaths": list_paths,
    "CreatePath": create_path,
    "RenamePath": rename_path,
    "DeletePath": delete_path,
    "GetAccessControl": get_access_control,
}


### Server


def _free_port() -> int:
    with closing(socket.socket(socket.AF_INET, socket.SOCK_STREAM)) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def _wait_for(port: int) -> None:
    for _ in range(100):
        try:
            with socket.create_connection(("127.0.0.1", port), timeout=0.1):
                return
        except OSError:
            time.sleep(0.05)
    raise RuntimeError("The dummy server did not start")


def serve_app(loop: asyncio.AbstractEventLoop):
    """Serve the app on ``loop``, running in another thread, and yield its base URL until closed."""
    port = _free_port()
    config = Config()
    config.bind = [f"127.0.0.1:{port}"]
    config.accesslog = None
    shutdown = asyncio.Event()
    future = asyncio.run_coroutine_threadsafe(
        serve(app, config, shutdown_trigger=shutdown.wait),  # type: ignore[arg-type]
        loop,
    )
    _wait_for(port)
    try:
        yield f"http://127.0.0.1:{port}"
    finally:
        loop.call_soon_threadsafe(shutdown.set)
        try:
            future.result(timeout=5.0)
        except TimeoutError:
            log.info("The dummy server closed with a timeout")


__all__ = ["DummyStorage", "Recorded", "dummy_storage", "serve_app"]
