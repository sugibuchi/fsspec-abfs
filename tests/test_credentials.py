"""Tests of `credentials`: what each value passed as a credential gives."""

from __future__ import annotations

import base64

import pytest
from azure.identity import AzureCliCredential as SyncAzureCliCredential
from azure.identity.aio import AzureCliCredential, ClientSecretCredential

from fsspec_abfs.credentials import TOKEN_CREDENTIALS, StorageCredential

_KEY = base64.b64encode(bytes(range(64))).decode()
_SAS = "sv=2024-11-04&ss=b&srt=co&sp=rl&se=2030-01-01T00:00:00Z&sig=abc%3D"


async def test_a_token_credential_is_used_as_it_is() -> None:
    token = AzureCliCredential()
    assert StorageCredential.from_credential(token) == StorageCredential(
        "TokenCredential", token
    )
    await token.close()


@pytest.mark.parametrize("value", [None, "anonymous"])
def test_anonymous(value: str | None) -> None:
    assert StorageCredential.from_credential(value) == StorageCredential(
        "Anonymous", None
    )


async def test_a_keyword_makes_its_class_with_the_arguments() -> None:
    credential = StorageCredential.from_credential(
        "client-secret", tenant_id="t", client_id="c", client_secret="s"
    )
    assert credential.kind == "TokenCredential"
    assert isinstance(credential.value, ClientSecretCredential)
    await credential.value.close()


@pytest.mark.parametrize("keyword", sorted(TOKEN_CREDENTIALS))
def test_every_keyword_names_an_async_class(keyword: str) -> None:
    from fsspec_abfs.credentials import _load  # noqa: PLC0415

    assert _load(TOKEN_CREDENTIALS[keyword])


async def test_a_class_name_makes_its_class() -> None:
    credential = StorageCredential.from_credential(
        "azure.identity.aio.AzureCliCredential"
    )
    assert isinstance(credential.value, AzureCliCredential)
    await credential.value.close()


@pytest.mark.parametrize("sas", [_SAS, "?" + _SAS])
def test_a_sas_token_without_its_question_mark(sas: str) -> None:
    assert StorageCredential.from_credential(sas) == StorageCredential("SAS", _SAS)


def test_a_shared_key() -> None:
    assert StorageCredential.from_credential(_KEY) == StorageCredential(
        "SharedKey", _KEY
    )


def test_secrets_are_never_shown() -> None:
    assert _KEY not in repr(StorageCredential.from_credential(_KEY))
    assert "sig=" not in repr(StorageCredential.from_credential(_SAS))


def test_an_unknown_string_is_refused_unquoted() -> None:
    with pytest.raises(ValueError, match="neither") as e:
        StorageCredential.from_credential("s3cret-value")
    assert "s3cret-value" not in str(e.value)


@pytest.mark.parametrize(
    "value",
    ["azure.identity.aio.NoSuchCredential", "nosuchmodule.Credential"],
)
def test_an_unknown_class_is_refused(value: str) -> None:
    with pytest.raises(ValueError, match="No token credential class"):
        StorageCredential.from_credential(value)


def test_a_sync_credential_is_refused() -> None:
    with pytest.raises(TypeError, match="async"):
        StorageCredential.from_credential(SyncAzureCliCredential())
    with pytest.raises(TypeError, match="async"):
        StorageCredential.from_credential("azure.identity.AzureCliCredential")


async def test_a_keyword_takes_only_its_options() -> None:
    # A file system's other options, e.g. asynchronous, are left alone.
    credential = StorageCredential.from_credential(
        "azure-cli", tenant_id="t", asynchronous=True, client_secret="unused"
    )
    assert isinstance(credential.value, AzureCliCredential)
    assert credential.value.tenant_id == "t"
    await credential.value.close()


async def test_a_class_name_takes_the_options_its_constructor_names() -> None:
    credential = StorageCredential.from_credential(
        "azure.identity.aio.ClientSecretCredential",
        tenant_id="t",
        client_id="c",
        client_secret="s",
        asynchronous=True,
    )
    assert isinstance(credential.value, ClientSecretCredential)
    await credential.value.close()


@pytest.mark.parametrize("value", [_KEY, _SAS, "anonymous"])
def test_other_credentials_leave_the_options(value: str) -> None:
    assert StorageCredential.from_credential(value, tenant_id="t").kind != (
        "TokenCredential"
    )


async def test_the_file_system_takes_them_from_its_storage_options() -> None:
    from fsspec_abfs.abfs import AbfsFileSystem  # noqa: PLC0415

    fs = AbfsFileSystem(
        "acct",
        "azure-cli",
        tenant_id="t",
        asynchronous=True,
        skip_instance_cache=True,
    )
    assert fs.storage_credential.value.tenant_id == "t"
    await fs.storage_credential.value.close()
