# Usage

For installation, see [README.md](README.md).

---

## Creating a file system

```python
from fsspec_abfs import AbfsFileSystem

fs = AbfsFileSystem(account_name="myaccount")
```

Or through fsspec, for the `abfs` and `abfss` protocols:

```python
import fsspec

fs = fsspec.filesystem("abfss", account_name="myaccount")
```

A file system works on one account. fsspec caches one per account, credential and options, however the account is
given.

---

## Options

All are keyword arguments:

| Parameter               | Description                                                                         | Default              |
|-------------------------|-------------------------------------------------------------------------------------|----------------------|
| `account_name`          | The account's name, e.g. `myaccount`.                                               | `None`               |
| `account_domain`        | `"core.windows.net"` for Azure, or `"fabric.microsoft.com"` for Fabric's OneLake.   | `"core.windows.net"` |
| `account_url`           | The account's endpoint, in place of `account_name` and `account_domain`.            | `None`               |
| `credential`            | How to authenticate: see [Authentication](#authentication).                         | `"default"`          |
| `is_hns_enabled`        | Whether the account has a hierarchical namespace. `None` asks the account.          | `None`               |
| `operation_concurrency` | Maximum concurrent requests of a recursive operation, such as `rm` or `copy`.       | `100`                |
| `io_concurrency`        | Maximum concurrent range reads and block uploads, shared by all the files opened.   | `16`                 |

`account_url` is an endpoint such as `https://myaccount.dfs.core.windows.net/`, or the blob endpoint. For OneLake, it
is `https://onelake.dfs.fabric.microsoft.com/`. Give either `account_url`, or `account_name` with `account_domain`:
not both.

The other keyword arguments go to fsspec's `AsyncFileSystem`, e.g. `asynchronous=True`.

### The hierarchical namespace

The file system works the same on accounts with or without a hierarchical namespace, but some operations take fewer
requests with one. Unless `is_hns_enabled` says, the first operation needing to know asks the account:

1. The account information tells it, but needs a role on the account.
2. Else the root of the container the operation works in tells it, e.g. for a SAS token of one container.

If neither answers, a warning is logged, and the file system takes the way right on both kinds of account. Pass
`is_hns_enabled` to save the request, or where the credential cannot ask.

---

## Logging

The library logs under the `fsspec_abfs` logger, and installs no handlers. A warning says when the hierarchical
namespace cannot be told.

Operations, such as `find`, `rm` or `copy`, are reported by the monitors of `fsspec_alt_strategies`, a dependency.
For example, `LoggingMonitor` logs the operations run in a `monitoring` block:

```python
import logging

from fsspec_alt_strategies import LoggingMonitor, monitoring

logger = logging.getLogger("my_app")

with monitoring(LoggingMonitor(logger, level=logging.INFO)):
    fs.rm("/mycontainer/tmp", recursive=True)
```

---

## Authentication

`credential` takes one of:

- an async token credential of `azure.identity.aio`, used as it is;
- a keyword naming one, made with the file system's options, e.g. `tenant_id`;
- the full name of an async token credential class, e.g. `"azure.identity.aio.AzureCliCredential"`;
- a SAS token, with or without its leading `?`;
- an account's shared key;
- `"anonymous"`, sending no credential, for public containers.

The default, `"default"`, is `DefaultAzureCredential`. `None` is refused.

| Keyword               | Credential                    |
|-----------------------|-------------------------------|
| `"default"`           | `DefaultAzureCredential`      |
| `"environment"`       | `EnvironmentCredential`       |
| `"azure-cli"`         | `AzureCliCredential`          |
| `"client-secret"`     | `ClientSecretCredential`      |
| `"certificate"`       | `CertificateCredential`       |
| `"managed-identity"`  | `ManagedIdentityCredential`   |
| `"workload-identity"` | `WorkloadIdentityCredential`  |

```python
from azure.identity.aio import DefaultAzureCredential

# A credential object: closing it is yours
fs = AbfsFileSystem(account_name="myaccount", credential=DefaultAzureCredential())

# A keyword, with the credential's own options
fs = AbfsFileSystem(
    account_name="myaccount",
    credential="client-secret",
    tenant_id="...",
    client_id="...",
    client_secret="...",
)

# A SAS token
fs = AbfsFileSystem(account_name="myaccount", credential="sv=...&sig=...")
```

A credential made from a keyword or a class name is closed with the file system. Sync credentials, of
`azure.identity` rather than `azure.identity.aio`, are refused: the file system's clients are async.

---

## Paths

A path is `/<container>/<path>`. The root, `/`, lists the account's containers.

These URLs name the same path:

| Form                 | Example                                                             |
|----------------------|---------------------------------------------------------------------|
| A path               | `/mycontainer/data/file.csv`                                        |
| Spark's and Hadoop's | `abfss://mycontainer@myaccount.dfs.core.windows.net/data/file.csv`  |
| adlfs'               | `abfs://mycontainer/data/file.csv`, or `az://`                      |
| An endpoint's        | `https://myaccount.dfs.core.windows.net/mycontainer/data/file.csv`  |

A URL naming another account raises `ValueError`. `fs.unstrip_protocol(path)` gives a path's URL in Spark's and
Hadoop's form.

---

## Common operations

All standard fsspec operations are supported.

```python
# List a folder
fs.ls("/mycontainer/data")

# List the containers
fs.ls("/")

# Check existence
fs.exists("/mycontainer/data/file.parquet")

# Get metadata
info = fs.info("/mycontainer/data/file.parquet")
print(info["size"], info["last_modified"], info["etag"])

# Create folders
fs.makedirs("/mycontainer/data/subdir", exist_ok=True)

# Copy a file
fs.copy("/mycontainer/src.csv", "/mycontainer/dst.csv")

# Move or rename a file or a folder
fs.mv("/mycontainer/old", "/mycontainer/new", recursive=True)

# Delete a folder
fs.rm("/mycontainer/data/subdir", recursive=True)

# Glob
files = fs.glob("/mycontainer/data/*.parquet")
```

Errors are standard exceptions: `FileNotFoundError`, `FileExistsError`, `IsADirectoryError`, `NotADirectoryError`,
`PermissionError`, or `OSError`.

### Containers

The file system makes no container: create them with Azure's own tools, with their settings. `makedirs` of a missing
container raises `PermissionError`, and of a folder in one `FileNotFoundError`. `rmdir` deletes an empty container.

### With or without a hierarchical namespace

| Operation        | With a hierarchical namespace                     | Without one                                      |
|------------------|---------------------------------------------------|--------------------------------------------------|
| Folders          | Paths of their own: an empty folder exists        | Prefixes of the files under them                 |
| `makedirs`       | Creates the folder                                | Sends nothing: a folder exists by its files      |
| `mv`             | One atomic rename, of a file or a whole folder    | A copy on the server, then a delete, per file    |
| `rm` of a folder | One request for the folder with all in it         | One request per file                             |
| `ls`, `find`     | The Data Lake API's listings                      | The blob API's listings                          |

Renames are atomic within a container. A file moved between two containers is copied, then deleted. A folder moved
between two containers raises `OSError` with `EXDEV`, before anything is copied.

Copies run on the server, with no data through this process. A copy still pending is waited for.

### Reading and writing files

```python
# Read a file
with fs.open("/mycontainer/data/file.bin", "rb") as f:
    data = f.read()

# Write a file
with fs.open("/mycontainer/data/output.bin", "wb") as f:
    f.write(b"hello")

# With pandas
import pandas as pd

df = pd.read_parquet("/mycontainer/data/table.parquet", filesystem=fs)
df.to_parquet("/mycontainer/data/out.parquet", filesystem=fs)
```

| Mode  | Supported | Notes                            |
|-------|-----------|----------------------------------|
| `rb`  | Yes       |                                  |
| `wb`  | Yes       |                                  |
| `xb`  | Yes       | Fails if the file already exists |
| `ab`  | No        |                                  |
| `r+b` | No        |                                  |

Reading:

- `cat_file` reads a file, or a range, in one request, with no metadata read first.
- A file opened reads ranges sized by how it is read, concurrently, limited by `io_concurrency`.
- Every range is of the version opened. A file replaced while it is read fails with an `OSError`.

Writing:

- A file under 8 MiB is uploaded in one request when it is closed.
- A larger one is uploaded in blocks of 4 MiB to 64 MiB, committed when it is closed.
- Nothing is visible before the file is closed. A file left by an error leaves any file at the path as it was.
- `xb` is checked by the request making the file visible, so that no other writer can slip in.

`AbfsFileSystem` is an async fsspec file system. Its `open_async()` returns files whose methods are awaited.

### Transactions

Files written in `with fs.transaction:` become visible when the block ends. If the block raises, they are discarded:

```python
with fs.transaction:
    with fs.open("/mycontainer/data/a.bin", "wb") as f:
        f.write(b"a")
    with fs.open("/mycontainer/data/b.bin", "wb") as f:
        f.write(b"b")
```

A file's blocks are uploaded when it is closed, and committed when the block ends. A file under 8 MiB is held in
memory until then.

Limits:

- The files are committed one after another. A transaction is not atomic across files.
- Only files written with `open` join the transaction. Copies, moves and deletes take effect at once.
- Files opened with `open_async` take no part in the transaction.
- If the process dies, uploaded blocks stay uncommitted until Azure drops them, after a week.
