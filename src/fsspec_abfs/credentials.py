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
import inspect
from dataclasses import dataclass
from typing import Any, Literal

from azure.core.credentials_async import AsyncTokenCredential

__all__ = ["TOKEN_CREDENTIALS", "StorageCredential"]

TOKEN_CREDENTIALS: dict[str, tuple[str, tuple[str, ...]]] = {
    "default": ("azure.identity.aio.DefaultAzureCredential", ("authority",)),
    "environment": ("azure.identity.aio.EnvironmentCredential", ("authority",)),
    "azure-cli": ("azure.identity.aio.AzureCliCredential", ("tenant_id",)),
    "client-secret": (
        "azure.identity.aio.ClientSecretCredential",
        ("tenant_id", "client_id", "client_secret", "authority"),
    ),
    "certificate": (
        "azure.identity.aio.CertificateCredential",
        ("tenant_id", "client_id", "certificate_path", "authority"),
    ),
    "managed-identity": (
        "azure.identity.aio.ManagedIdentityCredential",
        ("client_id",),
    ),
    "workload-identity": (
        "azure.identity.aio.WorkloadIdentityCredential",
        ("tenant_id", "client_id", "token_file_path"),
    ),
}
"""The keywords naming a token credential: the class each names, and the options it takes.

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
    def from_credential(cls, credential: Any, **options: Any) -> StorageCredential:
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
        **options
            The options of a file system, e.g. its ``storage_options``. A token
            credential a keyword names takes those its entry of
            `TOKEN_CREDENTIALS` lists, e.g. ``tenant_id``. One a class name
            names takes those its constructor names. The others are left.

        Returns
        -------
        StorageCredential

        Raises
        ------
        TypeError
            If ``credential`` is none of these, or a sync token credential.
        ValueError
            If a string is none of these.
        """
        if isinstance(credential, AsyncTokenCredential):
            return cls("TokenCredential", credential)

        if credential is None or credential == _ANONYMOUS:
            return cls("Anonymous", None)

        if not isinstance(credential, str):
            raise TypeError(
                "credential must be an async token credential or a str, not"
                f" {type(credential).__name__}: the service clients are async"
            )

        if credential in TOKEN_CREDENTIALS:
            class_name, names = TOKEN_CREDENTIALS[credential]
            return cls("TokenCredential", _make(_load(class_name), names, options))
        if _is_class_name(credential):
            token_class = _load(credential)
            return cls(
                "TokenCredential",
                _make(token_class, _named_parameters(token_class), options),
            )
        if _is_sas_token(credential):
            return cls("SAS", credential.removeprefix("?"))
        if _is_shared_key(credential):
            return cls("SharedKey", credential)

        # The value itself is never quoted: it may be a secret.
        raise ValueError(
            "credential is neither a keyword, a token credential class name, a SAS"
            f" token nor a shared key. Keywords: {', '.join([*TOKEN_CREDENTIALS, _ANONYMOUS])}"
        )


def _make(
    token_class: type[AsyncTokenCredential],
    names: tuple[str, ...],
    options: dict[str, Any],
) -> AsyncTokenCredential:
    """A token credential of ``token_class``, made with the options it takes."""
    return token_class(**{n: options[n] for n in names if n in options})


def _named_parameters(token_class: type) -> tuple[str, ...]:
    """The parameters a class's constructor names, without ``*args`` or ``**kwargs``.

    Those only a class's ``**kwargs`` takes are not known, so a file system's
    other options are never passed through them.
    """
    named = (inspect.Parameter.POSITIONAL_OR_KEYWORD, inspect.Parameter.KEYWORD_ONLY)
    return tuple(
        p.name
        for p in inspect.signature(token_class).parameters.values()
        if p.kind in named
    )


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
