"""Azure's errors as OSErrors."""

from __future__ import annotations

import errno

import pytest
from azure.core.exceptions import (
    ClientAuthenticationError,
    HttpResponseError,
    ResourceExistsError,
    ResourceModifiedError,
    ResourceNotFoundError,
    ServiceRequestError,
)

from fsspec_abfs._errors import azure_errors, os_error


@pytest.mark.parametrize(
    ("cls", "code", "status", "expected"),
    [
        (ResourceNotFoundError, "BlobNotFound", 404, errno.ENOENT),
        (ResourceNotFoundError, "PathNotFound", 404, errno.ENOENT),
        (ResourceNotFoundError, "ContainerNotFound", 404, errno.ENOENT),
        (ResourceExistsError, "BlobAlreadyExists", 409, errno.EEXIST),
        (ResourceExistsError, "PathAlreadyExists", 409, errno.EEXIST),
        (ResourceExistsError, "DirectoryNotEmpty", 409, errno.ENOTEMPTY),
        (ResourceExistsError, "DirectoryIsNotEmpty", 409, errno.ENOTEMPTY),
        (ResourceExistsError, "LeaseIdMissing", 409, errno.EIO),
        (HttpResponseError, "AuthorizationPermissionMismatch", 403, errno.EACCES),
        (ClientAuthenticationError, None, 401, errno.EACCES),
        (ResourceModifiedError, "ConditionNotMet", 412, errno.ESTALE),
        (HttpResponseError, "InvalidUri", 400, errno.EINVAL),
        (HttpResponseError, "ServerBusy", 503, errno.EIO),
    ],
)
def test_os_error(azure_error, cls, code, status, expected) -> None:
    error = os_error(azure_error(cls, code, status), "c/a")
    assert error.errno == expected
    assert error.filename == "c/a"
    # The message never quotes Azure's, which may hold a SAS token.
    assert "SECRET" not in str(error)


def test_subclasses(azure_error) -> None:
    assert isinstance(
        os_error(azure_error(ResourceNotFoundError, "BlobNotFound", 404), "p"),
        FileNotFoundError,
    )
    assert isinstance(
        os_error(azure_error(HttpResponseError, "AuthorizationFailure", 403), "p"),
        PermissionError,
    )


def test_azure_errors_context() -> None:
    with pytest.raises(OSError) as info, azure_errors("c/a"):
        raise ServiceRequestError("no network")
    assert info.value.errno == errno.EIO
    assert isinstance(info.value.__cause__, ServiceRequestError)

    # Other errors pass as they are.
    with pytest.raises(KeyError), azure_errors("c/a"):
        raise KeyError("x")
