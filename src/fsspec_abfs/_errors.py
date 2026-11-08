"""Azure's errors as Python's OSErrors, which fsspec's callers expect.

The mapping goes by the service's error code first, then by the HTTP status,
as the blob and Data Lake APIs name the same failure differently. A message
keeps the code and status only: Azure's own messages may quote request URLs,
which carry a SAS token when the credential is one.
"""

from __future__ import annotations

import errno
import os
from contextlib import contextmanager
from typing import TYPE_CHECKING

from azure.core.exceptions import (
    AzureError,
    ClientAuthenticationError,
    HttpResponseError,
    ResourceExistsError,
    ResourceModifiedError,
    ResourceNotFoundError,
)

if TYPE_CHECKING:
    from collections.abc import Iterator

__all__ = ["azure_errors", "describe", "os_error"]

_NOT_EMPTY = frozenset({"DirectoryNotEmpty", "DirectoryIsNotEmpty"})
_IS_A_DIRECTORY = frozenset({"OperationNotAllowedOnDirectory"})
_NOT_A_DIRECTORY = frozenset({"PathIsNotADirectory", "OperationNotAllowedOnFile"})
_EXISTS = frozenset(
    {"BlobAlreadyExists", "PathAlreadyExists", "ContainerAlreadyExists"}
)
_STALE = frozenset({"ConditionNotMet", "SourceConditionNotMet"})
_DENIED = frozenset(
    {
        "AuthorizationFailure",
        "AuthorizationPermissionMismatch",
        "AuthenticationFailed",
        "InsufficientAccountPermissions",
    }
)


def describe(error: BaseException) -> str:
    """An Azure error's type, status and code, never its message."""
    parts = [type(error).__name__]
    if (status := getattr(error, "status_code", None)) is not None:
        parts.append(f"status={status}")
    if code := getattr(error, "error_code", None):
        parts.append(f"code={code}")
    return ", ".join(parts)


def _error(code: int, path: str, error: BaseException) -> OSError:
    return OSError(code, f"{os.strerror(code)} ({describe(error)})", path)


def os_error(error: AzureError, path: str) -> OSError:
    """The OSError an Azure error stands for, on ``path``.

    ``OSError(errno, ...)`` gives the subclass of the errno, e.g.
    FileNotFoundError for ENOENT, as Python's own errors do.
    """
    code = getattr(error, "error_code", None)
    status = getattr(error, "status_code", None)
    if code in _NOT_EMPTY:
        return _error(errno.ENOTEMPTY, path, error)
    if code in _IS_A_DIRECTORY:
        return _error(errno.EISDIR, path, error)
    if code in _NOT_A_DIRECTORY:
        return _error(errno.ENOTDIR, path, error)
    if code in _EXISTS:
        return _error(errno.EEXIST, path, error)
    if code in _STALE or isinstance(error, ResourceModifiedError):
        # Read by If-Match, so the file changed while read.
        return _error(errno.ESTALE, path, error)
    if isinstance(error, ResourceNotFoundError) or status == 404:
        return _error(errno.ENOENT, path, error)
    if (
        code in _DENIED
        or isinstance(error, ClientAuthenticationError)
        or status in (401, 403)
    ):
        return _error(errno.EACCES, path, error)
    if isinstance(error, ResourceExistsError):
        # A 409 of no code known here: a conflict, e.g. a lease, rather than a
        # path existing.
        return _error(errno.EEXIST if code is None else errno.EIO, path, error)
    if isinstance(error, HttpResponseError) and status == 400:
        return _error(errno.EINVAL, path, error)
    return _error(errno.EIO, path, error)


@contextmanager
def azure_errors(path: str) -> Iterator[None]:
    """Raise Azure's errors within the block as OSErrors on ``path``.

    Only AzureError is mapped: CancelledError, and any other error, passes as
    it is.
    """
    try:
        yield
    except AzureError as e:
        raise os_error(e, path) from e
