# fsspec-abfs

[![PyPI - Version](https://img.shields.io/pypi/v/fsspec-abfs?color=blue)](https://pypi.org/project/fsspec-abfs/)
[![Python versions](https://img.shields.io/pypi/pyversions/fsspec-abfs)](https://pypi.org/project/fsspec-abfs/)
[![License](https://img.shields.io/github/license/sugibuchi/fsspec-abfs)](https://github.com/sugibuchi/fsspec-abfs/blob/main/LICENSE)
[![CI](https://github.com/sugibuchi/fsspec-abfs/actions/workflows/ci.yaml/badge.svg?branch=main)](https://github.com/sugibuchi/fsspec-abfs/actions/workflows/ci.yaml)
[![CodeQL](https://github.com/sugibuchi/fsspec-abfs/actions/workflows/codeql.yaml/badge.svg?branch=main)](https://github.com/sugibuchi/fsspec-abfs/actions/workflows/codeql.yaml)

An [fsspec](https://filesystem-spec.readthedocs.io/en/latest/) file system for
[Azure Data Lake Storage Gen2](https://learn.microsoft.com/en-us/azure/storage/blobs/data-lake-storage-introduction).

It works with accounts with or without a hierarchical namespace, and with Microsoft Fabric's OneLake. pandas, PyArrow
and any library that takes fsspec can use it through `abfs://` and `abfss://` URLs.

---

## Features

* Takes the URLs Spark and Hadoop use, `abfss://<container>@<account>.dfs.core.windows.net/<path>`, and those of
  adlfs.
* Uses the hierarchical namespace where the account has one: it renames a file or a folder in one atomic request,
  and deletes a folder with all in it in one request.
* Copies files on the server, without downloading them.
* Reads a file in one request, and pins the ranges of a file opened to one version of it.
* Writes a file whole, or not at all, and makes it visible only when it is closed.
* Authenticates with [Microsoft Entra ID](https://learn.microsoft.com/en-us/python/api/overview/azure/identity-readme)
  credentials, SAS tokens or account keys.
* Streams listings, and runs recursive operations, such as `find`, `rm` and `copy`, with concurrent requests.
* Works on many accounts at once with `AbfsFileSystems`, copying between two of them through this process or on the
  server.

---

## Installation

```bash
pip install fsspec-abfs
# or
uv add fsspec-abfs
```

---

## Usage

```python
from fsspec_abfs import AbfsFileSystem

fs = AbfsFileSystem(account_name="myaccount")

fs.ls("/mycontainer/data")

with fs.open("/mycontainer/data/file.csv", "rb") as f:
    data = f.read()
```

Installing the package registers it for the `abfs` and `abfss` protocols, in place of adlfs. A URL naming its
account needs no `account_name`:

```python
import pandas as pd

df = pd.read_parquet("abfss://mycontainer@myaccount.dfs.core.windows.net/data/table.parquet")
```

See [USAGE.md](https://github.com/sugibuchi/fsspec-abfs/blob/main/USAGE.md) for options, authentication and each
operation's behaviour.

### Paths

| Path                                                         | Meaning                                    |
|--------------------------------------------------------------|--------------------------------------------|
| `/<container>/<path>`                                        | A path in a container of the account       |
| `/`                                                          | The root, listing the account's containers |
| `abfss://<container>@<account>.dfs.core.windows.net/<path>`  | The same path, as Spark and Hadoop name it |

### Many accounts

`AbfsFileSystems` takes Spark's and Hadoop's URLs of any account, and answers the same URLs:

```python
from fsspec_abfs import AbfsFileSystems

fs = AbfsFileSystems(
    filesystems=[{"account_name": "opendata", "credential": "anonymous"}],
    cross_account_copy="server-side",
)

fs.copy(
    "abfss://public@opendata.dfs.core.windows.net/dataset",
    "abfss://raw@myaccount.dfs.core.windows.net/dataset",
    recursive=True,
)
```

---

## Compatibility

* Python 3.11 to 3.14
* `fsspec`: 2024.6.0 or later
* `azure-storage-file-datalake` and `azure-identity`
* Tested live on Azure storage accounts with and without a hierarchical namespace

---

## Status

**Alpha, not released yet.** The API and behaviour may change.

---

## Development, security and license

* [DEVELOPMENT.md](https://github.com/sugibuchi/fsspec-abfs/blob/main/DEVELOPMENT.md) explains how to work on the
  package: its setup and tests.
* [SECURITY.md](https://github.com/sugibuchi/fsspec-abfs/blob/main/SECURITY.md) explains how to report a
  vulnerability.
* The package is licensed under the Apache License 2.0. See
  [LICENSE](https://github.com/sugibuchi/fsspec-abfs/blob/main/LICENSE).
