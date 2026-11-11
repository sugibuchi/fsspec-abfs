# Development

## Setup

```bash
uv sync
```

All Python commands go through [uv](https://docs.astral.sh/uv/). The tests run on a local dummy server by default,
with no account and no credentials.

## Tests

```bash
uv run pytest                     # all tests, on the dummy server, with and without HNS
uv run pytest --api=live          # the filesystem tests on live accounts
uv run pytest --api=both          # on both
uv run pytest --hns=true          # only accounts with a hierarchical namespace: true, false or both
uvx ruff check .                  # lint
uvx ruff format .                 # format
```

Two options of `tests/conftest.py` choose what runs:

- `--api` chooses where the filesystem tests run: `dummy`, the default, `live` or `both`;
- `--hns` chooses the kinds of account: `true` for a hierarchical namespace, `false` for none, or `both`, the default.

Without the options, `FSSPEC_ABFS_TEST_API` and `FSSPEC_ABFS_TEST_HNS` choose, from the environment or `.env`. An
option overrides its variable. A test id names its API and its kind of account, e.g. `test_ls[dummy-hns]`.

| File                  | Tests of                                                         |
|-----------------------|------------------------------------------------------------------|
| `test_abfs.py`        | `AbfsFileSystem` and its file objects, over HTTP                 |
| `test_hns.py`         | The probe of the hierarchical namespace, and the profiles        |
| `test_listing.py`     | `_listing.py`'s ListBlobs paging, against the SDK's listing      |
| `test_credentials.py` | `credentials.py`: what each value passed as a credential gives   |
| `test_errors.py`      | `_errors.py`, with no server                                     |
| `test_paths.py`       | `_paths.py`, the account's arguments and the cache, no server    |
| `test_package.py`     | The package itself                                               |

The fixtures of `tests/conftest.py`:

- `make_fs`: makes an `AbfsFileSystem` of the account the test runs on, async and uncached;
- `fs`: one made by `make_fs`;
- `root`: a folder of the test's own, not created yet, e.g. `/c/work` on the dummy server;
- `dummy`: the dummy account, its switches, and the requests it served, reset after the test.

A test taking `make_fs`, `fs` or `root` runs once per API and kind of account chosen. Some tests make sense on one
side only:

- A test taking `dummy` runs on the dummy server only. It switches the server's behaviour, or counts the requests
  it served.
- The `dummy_only`, `live_only`, `hns_only` and `flat_only` markers restrict the others.
- A test is skipped when the options exclude its side.

### The dummy server

`tests/dummy_api.py` is a FastAPI app served by Hypercorn on a free local port. The SDK's own async clients talk to it
over HTTP, so their requests, errors and paging run as against Azure. One store serves both APIs, under two path
prefixes: `/blob` and `/dfs`.

`AbfsFileSystem` only accepts Azure's accounts. The `make_fs` fixture gives it the account `dummy`, then sets its
`_dfs_url` and `_blob_url` to the dummy's before the first request, as its clients are made on them at first use. The
dummy needs no credential, and the tests send none: the SDK refuses tokens over plain HTTP.

The dummy's containers are made with `dummy.create_container`: the filesystem makes none.

It answers these operations of the blob API:

- Get Account Information, with `x-ms-is-hns-enabled`;
- List Containers, Create Container, Delete Container and Get Container Properties;
- List Blobs, with a prefix, a delimiter, metadata and pages;
- Get Blob Properties, Get Blob with a range and `If-Match`, and Set Blob Metadata;
- Put Blob, Put Block and Put Block List, with `If-None-Match: *`;
- Delete Blob and Copy Blob.

It answers these operations of the Data Lake API:

- List Paths, one folder or recursive, with pages;
- Create Path, for folders and files;
- Rename Path, of a file or a folder with all in it;
- Delete Path, recursive or not;
- Get Access Control, of a container's root too.

The `hns` switch chooses the kind of account. With a hierarchical namespace, folders are paths of their own:

- the blob API sees them as empty blobs with the metadata `hdi_isfolder=true`;
- writing a blob creates its missing parents;
- the blob API refuses to delete a folder with things in, by `DirectoryIsNotEmpty`.

Without one, folders are prefixes, and every Data Lake request fails with `HierarchicalNamespaceNotEnabled`.

Errors come in Azure's format: an `x-ms-error-code` header, with XML on the blob API and JSON on the Data Lake API.
The SDK raises its own exceptions from them, with their `error_code`.

`dummy.requests` records each request served: its API, its REST operation's name, e.g. `ListBlobs`, its method, path
and query. `dummy.count(operation)` and `dummy.operations()` read them. Tests counting requests assert on these.

Switches of `dummy` make the server answer as a given account:

- `hns` chooses the kind of account, from the test's `hns`;
- `failures` maps an operation to the status and code it fails with, e.g. to refuse the account information;
- `page_size` limits the entries of a listing's page;
- `copy_pending_reads` answers copies as pending, for as many property reads;
- `copy_outcome` is the status of a pending copy at its end, e.g. `failed`.

Where Azure's references leave behaviour open, the dummy makes an assumption, marked in a comment:

- With HNS, a blob written over a folder fails with `OperationNotAllowedOnDirectory`, and under a file with
  `PathConflict`.
- A Data Lake listing of a file fails with `PathNotFound`.
- Creating an existing folder keeps it, with all in it.
- A rename replaces a file only: onto a folder, or of a folder onto anything, it fails with `PathAlreadyExists`.
- A copy's data is in place at once, even while the copy is reported pending.

It checks no authentication, no lease, no SAS and no ACL. It keeps no versions, snapshots or tags.

### Live tests

Live tests are opt in, with `--api` or `FSSPEC_ABFS_TEST_API`. Once asked for, an account that cannot be reached is
an error, not a skip that would pass unseen. They need an account of each kind chosen by `--hns`:

| Environment variable                | Contents                                                       |
|-------------------------------------|----------------------------------------------------------------|
| `FSSPEC_ABFS_TEST_HNS_ACCOUNT_URL`  | The endpoint of an account with a hierarchical namespace       |
| `FSSPEC_ABFS_TEST_FLAT_ACCOUNT_URL` | The endpoint of an account without one                         |
| `FSSPEC_ABFS_TEST_CONTAINER`        | A container of that name in each account                       |

An endpoint is e.g. `https://<account>.dfs.core.windows.net/`. Each test works in a folder of its own,
`fsspec-abfs-test-<run>/<test>` in the container, deleted afterwards. The credential is
`azure.identity.aio.DefaultAzureCredential`, e.g. from `az login`. The tests load a `.env` file in the repository
folder, which can hold these variables.

Only live tests check what the dummy assumes, and what it does not model:

- Azure's real answers where the references are silent, listed above;
- authentication, SAS tokens and roles, and the probe refused by them;
- the latency, throttling and retries of a real service.

## The copy of fsspec-alt-strategies

`src/fsspec_abfs/_contrib/strategy/` is a copy of the package `fsspec-alt-strategies-impl`, version 0.3.0. It keeps
only what `AbfsFileSystem` uses: `tree/`, `common/` and `files/`. `AbfsFileSystem` lists a whole tree by Azure's own
recursive listings, so the tree family's fallback, `tree/listing.py`, is left out too. `_version.py` stays, as the
copy's command line recognises a previous copy by it. Ruff skips the copy.

Its interface is not copied: it is the dependency `fsspec-alt-strategies>=0.3,<0.4`, bounded at the copy's minor
version, as a minor release may break the copy before 1.0.

To refresh it:

1. Copy the package over the old copy, which replaces it whole:

   ```bash
   uv run --with "fsspec-alt-strategies-impl[cli]==<version>" fsspec-alt-strategies-impl copy src/fsspec_abfs/_contrib/strategy
   ```

2. Delete again what `AbfsFileSystem` does not use:

   ```bash
   cd src/fsspec_abfs/_contrib/strategy
   rm -r flat wrapper monitor.py router.py __init__.py common/sync_client.py tree/listing.py
   ```

3. Update the version above, and the interface's bounds in `pyproject.toml`, then run `uv lock` and the tests.
