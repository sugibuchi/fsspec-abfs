"""Account URLs and paths."""

from __future__ import annotations

import pytest

from fsspec_abfs._paths import account_url_of, parse_account_url, strip_protocol
from fsspec_abfs.abfs import AbfsFileSystem


@pytest.mark.parametrize(
    ("url", "domain"),
    [
        ("https://acc.dfs.core.windows.net/", "core.windows.net"),
        ("https://acc.blob.core.windows.net", "core.windows.net"),
        ("https://ACC.dfs.fabric.microsoft.com/", "fabric.microsoft.com"),
        ("https://acc.blob.fabric.microsoft.com", "fabric.microsoft.com"),
    ],
)
def test_account_url(url: str, domain: str) -> None:
    assert parse_account_url(url) == ("acc", domain)
    fs = AbfsFileSystem(url, skip_instance_cache=True)
    assert (fs.account_name, fs.account_domain) == ("acc", domain)
    assert fs.account_url == f"https://acc.dfs.{domain}"
    assert (fs._dfs_url, fs._blob_url) == (
        f"https://acc.dfs.{domain}",
        f"https://acc.blob.{domain}",
    )


@pytest.mark.parametrize(
    "url",
    [
        "http://acc.dfs.core.windows.net/",
        "https://acc.file.core.windows.net/",
        "https://acc.dfs.core.windows.net/container",
        "https://acc.dfs.example.com/",
        "abfs://c@acc.dfs.core.windows.net/",
    ],
)
def test_account_url_rejected(url: str) -> None:
    with pytest.raises(ValueError, match="account_url"):
        parse_account_url(url)


@pytest.mark.parametrize(
    ("path", "stripped"),
    [
        ("abfs://c/a/b", "c/a/b"),
        ("abfss://c/a/b/", "c/a/b"),
        ("az://c", "c"),
        ("abfss://c@acc.dfs.core.windows.net/a/b", "c/a/b"),
        ("abfs://c@acc.dfs.fabric.microsoft.com", "c"),
        ("/c/a", "c/a"),
        ("c/a", "c/a"),
        ("abfs://", ""),
        ("", ""),
    ],
)
def test_strip_protocol(path: str, stripped: str) -> None:
    assert strip_protocol(path) == stripped
    assert AbfsFileSystem._strip_protocol(path) == stripped


def test_kwargs_from_urls() -> None:
    assert account_url_of("abfss://c@acc.dfs.core.windows.net/a") == (
        "https://acc.dfs.core.windows.net"
    )
    assert AbfsFileSystem._get_kwargs_from_urls("abfs://c/a") == {}
    assert AbfsFileSystem._get_kwargs_from_urls(
        "abfss://c@acc.dfs.core.windows.net/a"
    ) == {"account_url": "https://acc.dfs.core.windows.net"}


def test_registered() -> None:
    import fsspec  # noqa: PLC0415

    assert fsspec.get_filesystem_class("abfs") is AbfsFileSystem
    assert fsspec.get_filesystem_class("abfss") is AbfsFileSystem
