"""The credentials a file system accepts, as the Azure SDK's service clients take them.

`StorageCredential.from_credential` reads what a caller passes: an async token
credential, or a string naming one, or a SAS token, or an account's shared key.
Settings from environment variables are not read here: the keyword
"environment" gives `EnvironmentCredential`, which reads Azure's own.
"""

from __future__ import annotations

import base64
import binascii
import importlib
from dataclasses import dataclass
from typing import Any, Literal

from azure.core.credentials_async import AsyncTokenCredential

__all__ = ["TOKEN_CREDENTIALS", "StorageCredential"]

TOKEN_CREDENTIALS: dict[str, str] = {
    "default": "azure.identity.aio.DefaultAzureCredential",
    "environment": "azure.identity.aio.EnvironmentCredential",
    "azure-cli": "azure.identity.aio.AzureCliCredential",
    "client-secret": "azure.identity.aio.ClientSecretCredential",
    "certificate": "azure.identity.aio.CertificateCredential",
    "managed-identity": "azure.identity.aio.ManagedIdentityCredential",
    "workload-identity": "azure.identity.aio.WorkloadIdentityCredential",
}
"""The keywords naming a token credential, and the class each names.

No "user-password": ``azure.identity.aio`` has no ``UsernamePasswordCredential``.
"""

_ANONYMOUS = "anonymous"

# An account key is 64 random bytes, in base64.
_SHARED_KEY_BYTES = 64

CredentialKind = Literal["TokenCredential", "SAS", "SharedKey", "Anonymous"]


@dataclass(frozen=True)
class StorageCredential:
    """A credential, of a kind, as the service clients take it.

    Attributes
    ----------
    kind : {"TokenCredential", "SAS", "SharedKey", "Anonymous"}
        What the credential is.
    value : Any or None
        The value given to the service clients as their ``credential``: an
        async token credential, a SAS token, an account key, or None.
    """

    kind: CredentialKind
    value: Any | None

    def __repr__(self) -> str:
        # Never the value of a SAS token or a key.
        if self.kind in ("SAS", "SharedKey"):
            return f"StorageCredential(kind={self.kind!r}, value=***)"
        return f"StorageCredential(kind={self.kind!r}, value={self.value!r})"

    @classmethod
    def from_credential(cls, credential: Any, **kwargs: Any) -> StorageCredential:
        """The credential that ``credential`` gives.

        Parameters
        ----------
        credential : AsyncTokenCredential, str or None
            One of:

            - an async token credential, used as it is;
            - a keyword of `TOKEN_CREDENTIALS`, e.g. "default", or "anonymous";
            - the full name of an async token credential class, e.g.
              "azure.identity.aio.AzureCliCredential";
            - a SAS token, with or without its leading "?";
            - an account's shared key.

            None is anonymous.
        **kwargs
            Passed to the token credential class a keyword or a class name
            names, e.g. ``tenant_id``. Refused with any other credential.

        Returns
        -------
        StorageCredential

        Raises
        ------
        TypeError
            If ``credential`` is none of these, or a sync token credential.
        ValueError
            If a string is none of these, or ``kwargs`` are given for a
            credential that takes none.
        """
        if isinstance(credential, AsyncTokenCredential):
            _no_kwargs(kwargs, "a token credential object")
            return cls("TokenCredential", credential)

        if credential is None or credential == _ANONYMOUS:
            _no_kwargs(kwargs, "anonymous access")
            return cls("Anonymous", None)

        if not isinstance(credential, str):
            raise TypeError(
                "credential must be an async token credential or a str, not"
                f" {type(credential).__name__}: the service clients are async"
            )

        if credential in TOKEN_CREDENTIALS:
            return cls(
                "TokenCredential", _load(TOKEN_CREDENTIALS[credential])(**kwargs)
            )
        if _is_class_name(credential):
            return cls("TokenCredential", _load(credential)(**kwargs))
        if _is_sas_token(credential):
            _no_kwargs(kwargs, "a SAS token")
            return cls("SAS", credential.removeprefix("?"))
        if _is_shared_key(credential):
            _no_kwargs(kwargs, "a shared key")
            return cls("SharedKey", credential)

        # The value itself is never quoted: it may be a secret.
        raise ValueError(
            "credential is neither a keyword, a token credential class name, a SAS"
            f" token nor a shared key. Keywords: {', '.join([*TOKEN_CREDENTIALS, _ANONYMOUS])}"
        )


def _no_kwargs(kwargs: dict[str, Any], what: str) -> None:
    if kwargs:
        raise ValueError(f"{what} takes no arguments: {', '.join(sorted(kwargs))}")


def _is_class_name(value: str) -> bool:
    """Whether ``value`` reads as a dotted name, e.g. ``package.module.Class``."""
    parts = value.split(".")
    return len(parts) > 1 and all(p.isidentifier() for p in parts)


def _is_sas_token(value: str) -> bool:
    """Whether ``value`` is a SAS token: a query string with a signature and a version."""
    fields = {p.partition("=")[0] for p in value.removeprefix("?").split("&")}
    return {"sig", "sv"} <= fields


def _is_shared_key(value: str) -> bool:
    """Whether ``value`` is an account key: 64 bytes in base64."""
    try:
        return len(base64.b64decode(value, validate=True)) == _SHARED_KEY_BYTES
    except (binascii.Error, ValueError):
        return False


def _load(class_name: str) -> type[AsyncTokenCredential]:
    """The async token credential class of its full name."""
    module_name, _, name = class_name.rpartition(".")
    try:
        cls = getattr(importlib.import_module(module_name), name)
    except (ImportError, AttributeError) as e:
        raise ValueError(f"No token credential class {class_name!r}") from e
    if not (isinstance(cls, type) and issubclass(cls, AsyncTokenCredential)):
        raise TypeError(
            f"{class_name} is not an async token credential class: the service"
            " clients are async, e.g. azure.identity.aio's"
        )
    return cls
