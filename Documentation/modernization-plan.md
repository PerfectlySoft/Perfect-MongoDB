# Perfect-MongoDB modernization plan

Status: **Draft — not started.** Captured 2026-09-27 from a research session so work can be picked up later on a laptop.

The work belongs in [`PerfectlySoft/Perfect-MongoDB`](https://github.com/PerfectlySoft/Perfect-MongoDB) (or a new repo, if you decide that), not in Perfect-CRUD.

## Current state (as read, not compiled)

**Perfect-MongoDB**: about 2,800 lines of Swift plus about 900 lines of tests. Last real change was March 2019, for `swift-tools-version:4.1`.

| File | Covers |
|---|---|
| `MongoClient.swift`, `MongoConnectionPool.swift` | Connecting, pooling (`MongoClientPool`), listing databases, server status |
| `MongoDatabase.swift` | Collections, create, drop |
| `MongoCollection.swift` | Insert/update/delete/save, find, count, aggregate, indexes, bulk ops, find-and-modify |
| `MongoCursor.swift` | Iterating results |
| `BSON.swift` | Building and reading documents, JSON conversion |
| `MongoGridFS.swift` | File storage (legacy GridFS API) |
| `MongoJSONConvertables.swift` | `Date: JSONConvertible` (the only use of PerfectLib) |

Dependencies: `PerfectSideRepos/Perfect-CMongo` and `PerfectSideRepos/Perfect-CBSON` (small system-library packages linking `libmongoc-1.0` / `libbson-1.0` via pkg-config); `PerfectlySoft/PerfectLib` 3.x only for `JSONConvertible`. `Perfect-mongo-c` and `Perfect-mongo-c-linux` are older Swift 3-era wrappers nothing uses; archive them. Related repos to review later: `Perfect-Session-MongoDB`, `Perfect-Turnstile-MongoDB`.

## Deprecated libmongoc calls (replace first)

These have been deprecated since the 1.x releases. I believe 2.x removes them, but I haven't checked that against the 2.x headers yet.

| Current call | Replacement |
|---|---|
| `mongoc_collection_find` | `mongoc_collection_find_with_opts` |
| `mongoc_collection_insert` | `mongoc_collection_insert_one` |
| `mongoc_collection_update` | `mongoc_collection_update_one` / `_update_many` / `_replace_one` |
| `mongoc_collection_remove` | `mongoc_collection_delete_one` / `_delete_many` |
| `mongoc_collection_save` | `replace_one` with `upsert: true` |
| `mongoc_collection_count` | `mongoc_collection_count_documents` / `estimated_document_count` |
| `mongoc_collection_find_and_modify` | `mongoc_collection_find_and_modify_with_opts` |
| `mongoc_collection_create_index` | `mongoc_collection_create_indexes_with_opts` (or `createIndexes` command) |
| `mongoc_collection_get_last_error` | Use the reply document from each `_with_opts` call |
| `mongoc_client_get_database_names` | `mongoc_client_get_database_names_with_opts` |
| `mongoc_database_get_collection_names` | `mongoc_database_get_collection_names_with_opts` |
| `mongoc_client_get_server_status` | Run the `serverStatus` command |
| `mongoc_gridfs_file_get_md5` / `_set_md5` | Remove (MD5 dropped from the GridFS spec). Consider moving to `mongoc_gridfs_bucket_t` |

All replacements exist in libmongoc 1.x (Ubuntu ships 1.26), so this step needs no 2.x dependency.

## Bugs noticed on a first read

1. `MongoClientPool.tryPopClient()` calls `mongoc_client_pool_try_pop`, then passes the returned **client** into `mongoc_client_pool_pop`, which expects the **pool**. It should just wrap the client from `try_pop`.
2. `MongoClientPool.init(uri:)` never calls `mongoc_uri_destroy` on the `mongoc_uri_t` it creates (memory leak). It also doesn't check for a failed URI parse.

## Plan

### Phase 1: builds on Swift 6, API unchanged
- [ ] `swift-tools-version: 6.x`; set supported platforms (macOS 12+ like Perfect-CRUD, plus Linux).
- [ ] Move the C wrappers into this repo as system-library targets (`CMongoc`, `CBSON`). Drop the `PerfectSideRepos` dependencies.
- [ ] Remove the PerfectLib dependency: replace `JSONConvertible` with a local helper.
- [ ] Delete the checked-in jazzy `docs/` and `LinuxMain.swift` / `XCTestManifests.swift`.
- [ ] Get it compiling in Swift 5 language mode first, then turn on Swift 6 mode.
- [ ] Keep all public type and method names so existing users' code still compiles.

### Phase 2: current C APIs, bug fixes, CI
- [ ] Replace every deprecated call in the table above.
- [ ] Fix the two bugs listed above.
- [ ] Add GitHub Actions: Ubuntu + `libmongoc-dev`, `mongo` service container, `swift test`. Optionally a macOS job with Homebrew `mongo-c-driver`.
- [ ] Test against current MongoDB server versions (7.x/8.x). Test `mongodb+srv://` and TLS connection strings for Atlas.

### Phase 3: libmongoc 2.x
- [ ] Support both pkg-config names: `libmongoc-1.0`/`libbson-1.0` and `mongoc2`/`bson2`.
- [ ] Check 2.x header changes and update the Swift code to match.
- [ ] Add a 2.x build to the CI matrix.

### Phase 4: Swift-native API (added alongside the old one)
- [ ] Ownership and `Sendable`: the pool is the only object shared across threads; clients, collections and cursors belong to one task.
- [ ] `async` versions of the blocking calls, running on a dedicated executor like Perfect-CRUD's `AsyncExecution.swift` / `Pool.swift`.
- [ ] `Codable` encode/decode for `BSON`. The archived `mongo-swift-driver` (Apache-2.0) has a working implementation to adapt.
- [ ] `AsyncSequence` cursor.
- [ ] Deprecate, don't remove, old methods that the new ones replace.

## Open questions

1. Update `PerfectlySoft/Perfect-MongoDB` in place (preferred) or start a new repo?
2. Do the community requesters have code on the old API? If so, Phases 1–2 must not break it.
3. What they use MongoDB for (existing data, Atlas, change streams, GridFS) decides what Phase 4 does first.
4. Minimum libmongoc: support 1.x only, or require 2.x once Linux distributions ship it?

## Laptop setup

```sh
# macOS
brew install mongo-c-driver            # libmongoc + libbson; check which major version it installs
# Test server
docker run -d --name mongo -p 27017:27017 mongo:8
# Swift 6 toolchain: Xcode 16+/26
git clone https://github.com/PerfectlySoft/Perfect-MongoDB
cd Perfect-MongoDB && git checkout -b swift6-modernization
pkg-config --modversion libmongoc-1.0 mongoc2   # see which is installed
```

### Kickoff prompt for Claude Code

> Read `Documentation/modernization-plan.md`. We're working in Perfect-MongoDB on branch `swift6-modernization`. Start Phase 1: bring the package to Swift 6 tools, move the Perfect-CMongo/Perfect-CBSON module maps in as system-library targets, drop PerfectLib, and keep the public API unchanged. Build and run the tests against the local `mongo` container after each step.
