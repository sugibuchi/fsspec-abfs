"""Accounts and paths."""

from __future__ import annotations

import pytest
from fsspec.core import url_to_fs

from fsspec_abfs._paths import account_of, parse_account_url, strip_protocol
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
    fs = AbfsFileSystem(account_url=url, skip_instance_cache=True)
    assert (fs.account_name, fs.account_domain) == ("acc", domain)
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


def test_account_by_name_and_domain() -> None:
    fs = AbfsFileSystem(account_name="Acc", skip_instance_cache=True)
    assert (fs.account_name, fs.account_domain) == ("acc", "core.windows.net")
    fs = AbfsFileSystem(
        account_name="acc",
        account_domain="fabric.microsoft.com",
        skip_instance_cache=True,
    )
    assert (fs._dfs_url, fs._blob_url) == (
        "https://acc.dfs.fabric.microsoft.com",
        "https://acc.blob.fabric.microsoft.com",
    )


@pytest.mark.parametrize(
    ("kwargs", "match"),
    [
        ({}, "required"),
        (
            {"account_name": "acc", "account_url": "https://acc.dfs.core.windows.net"},
            "not both",
        ),
        (
            {
                "account_domain": "core.windows.net",
                "account_url": "https://acc.dfs.core.windows.net",
            },
            "not both",
        ),
        ({"account_name": "a_c"}, "account name"),
        ({"account_name": "acc", "account_domain": "example.com"}, "account_domain"),
    ],
)
def test_account_given_wrongly(kwargs: dict, match: str) -> None:
    with pytest.raises(ValueError, match=match):
        AbfsFileSystem(skip_instance_cache=True, **kwargs)


def test_arguments_are_keywords_only() -> None:
    with pytest.raises(TypeError, match="keyword arguments only"):
        AbfsFileSystem("acc", skip_instance_cache=True)  # type: ignore[misc]


def test_cached_by_account_however_given() -> None:
    by_name = AbfsFileSystem(account_name="cacheacc")
    assert (
        AbfsFileSystem(account_name="cacheacc", account_domain="core.windows.net")
        is by_name
    )
    assert (
        AbfsFileSystem(account_url="https://cacheacc.dfs.core.windows.net/") is by_name
    )
    assert (
        AbfsFileSystem(account_url="https://cacheacc.blob.core.windows.net") is by_name
    )
    other = AbfsFileSystem(
        account_name="cacheacc", account_domain="fabric.microsoft.com"
    )
    assert other is not by_name
    # Made again from its storage options, as when unpickled: the same account.
    assert by_name.storage_options["account_name"] == "cacheacc"
    assert "account_url" not in by_name.storage_options


@pytest.mark.parametrize(
    ("path", "stripped"),
    [
        ("abfs://c/a/b", "/c/a/b"),
        ("abfss://c/a/b/", "/c/a/b"),
        ("az://c", "/c"),
        ("abfss://c@acc.dfs.core.windows.net/a/b", "/c/a/b"),
        ("abfs://c@acc.dfs.fabric.microsoft.com", "/c"),
        ("/c/a", "/c/a"),
        ("c/a", "/c/a"),
        ("abfs://", "/"),
        ("/", "/"),
        ("", "/"),
    ],
)
def test_strip_protocol(path: str, stripped: str) -> None:
    assert strip_protocol(path) == stripped
    assert AbfsFileSystem._strip_protocol(path) == stripped


def test_paths_of_another_account_are_refused() -> None:
    fs = AbfsFileSystem(account_name="acc", skip_instance_cache=True)
    assert fs._strip_protocol("abfss://c@acc.dfs.core.windows.net/a") == "/c/a"
    assert fs._strip_protocol(["abfs://c/a", "/c/b"]) == ["/c/a", "/c/b"]
    for url in (
        "abfss://c@other.dfs.core.windows.net/a",
        "abfss://c@acc.dfs.fabric.microsoft.com/a",
    ):
        with pytest.raises(ValueError, match="not of this filesystem's"):
            fs._strip_protocol(url)
        with pytest.raises(ValueError, match="not of this filesystem's"):
            fs.info(url)


def test_parents() -> None:
    assert AbfsFileSystem._parent("/c/a/b") == "/c/a"
    assert AbfsFileSystem._parent("/c") == "/"
    assert AbfsFileSystem._parent("abfs://c/a") == "/c"


def test_kwargs_from_urls() -> None:
    assert account_of("abfss://c@acc.dfs.core.windows.net/a") == (
        "acc",
        "core.windows.net",
    )
    assert account_of("abfs://c/a") is None
    assert AbfsFileSystem._get_kwargs_from_urls("abfs://c/a") == {}
    assert AbfsFileSystem._get_kwargs_from_urls(
        "abfss://c@acc.dfs.fabric.microsoft.com/a"
    ) == {"account_name": "acc", "account_domain": "fabric.microsoft.com"}


def test_url_to_fs_takes_the_account_of_the_url() -> None:
    fs, path = url_to_fs("abfss://c@urlacc.dfs.fabric.microsoft.com/a/b")
    assert isinstance(fs, AbfsFileSystem)
    assert (fs.account_name, fs.account_domain) == ("urlacc", "fabric.microsoft.com")
    assert path == "/c/a/b"
