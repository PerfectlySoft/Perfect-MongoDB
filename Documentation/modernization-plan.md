# Perfect-MongoDB modernization plan

Status: **Phases 1, 2 and 4 done (Phase 4 on 2026-09-29), except the Atlas/TLS check.** 36 tests pass against MongoDB 8.3 locally and on Linux (`Scripts/test-linux.sh`). The 26 from Phases 1-2 also pass in CI on Linux (Swift 6.4, MongoDB 8, libmongoc 2.5.5 built from source). Captured 2026-09-27 from a research session so work can be picked up later on a laptop.

Decision on 2026-09-27: **target libmongoc 2.x only.** Homebrew's `mongo-c-driver` is now 2.x; 1.x survives only as the deprecated, keg-only `mongo-c-driver@1`, which Homebrew disables on 2027-04-01. Phase 3's 2.x work was therefore folded into Phase 1. The catch: Linux distributions that still ship 1.x (Ubuntu's `libmongoc-dev` is 1.26) need libmongoc 2 built from source until they package it.

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

**Checked against the 2.5.5 headers (Phase 1):** 2.x removed `find`, `count`, `save`, `create_index` (and the `mongoc_index_opt_*` structs), `get_last_error`, `get_server_status`, `get_collection_names`, plus `mongoc_collection_command`, `_stats`, `_validate`, `_create_bulk_operation`, `mongoc_gridfs_find`, `bson_as_json` / `bson_array_as_json` and `MONGOC_QUERY_SLAVE_OK`. Those are replaced. 2.x **still ships** `mongoc_collection_insert` / `_update` / `_remove` / `_find_and_modify`, `mongoc_client_get_database_names` and the GridFS MD5 accessors, so those remain for Phase 2.

## Bugs noticed on a first read

1. `MongoClientPool.tryPopClient()` calls `mongoc_client_pool_try_pop`, then passes the returned **client** into `mongoc_client_pool_pop`, which expects the **pool**. It should just wrap the client from `try_pop`.
2. `MongoClientPool.init(uri:)` never calls `mongoc_uri_destroy` on the `mongoc_uri_t` it creates (memory leak). It also doesn't check for a failed URI parse.

## Plan

### Phase 1: builds on Swift 6, API unchanged
- [x] `swift-tools-version: 6.0`; macOS 12+.
- [x] Move the C wrappers into this repo as system-library targets. They keep their old module names, `PerfectCMongo` and `PerfectCBSON`, so direct imports still work. Drop the `PerfectSideRepos` dependencies.
- [x] Remove the PerfectLib dependency. `Date.jsonEncodedString()` stays public; code that needs the `JSONConvertible` conformance can add `extension Date: JSONConvertible {}`.
- [x] Delete the checked-in jazzy `docs/`, `.jazzy.yaml` and `LinuxMain.swift` / `XCTestManifests.swift`.
- [x] Compiled in Swift 5 language mode first, then turned on Swift 6 mode.
- [x] Keep all public type and method names so existing users' code still compiles.
- [x] (From Phase 3) Build against libmongoc 2.x (`mongoc2` / `bson2`) and replace the removed calls. The legacy translation lives in `Sources/PerfectCMongo/shim.h`: `$query`/`$orderby` unwrapping, query flags to find options, save as insert or upsert-replace.
- [x] Run the full test suite against a local `mongod` (8.3.11). Added tests for `createIndex`, `stats` and legacy `$query`/`$orderby` finds.
- [x] Call `mongoc_init()` once before creating the first client or pool. libmongoc 2 no longer initializes itself when loaded, so without it every `MongoClient(uri:)` failed with "Could not parse URI".

Phase 1 behaviour changes:
- `getLastError()` is deprecated and returns an empty document, because libmongoc 2 no longer tracks it.
- `command()` returns the command's reply as a one-document cursor. Its `fields`/`skip`/`limit`/`batchSize` parameters never applied to commands.
- `count()` now uses `countDocuments`, which gives an exact count instead of the old metadata estimate.
- `MongoIndexOptions` ignores `dropDups` (removed in MongoDB 3.0) and `storageOptions` (it was never actually sent before).
- `MongoQueryFlag.slaveOk` is deprecated in favour of `secondaryOk`, which maps to a `secondaryPreferred` read preference.

### Phase 2: current C APIs, bug fixes, CI
- [x] Move writes to the current APIs: `insert` → `insert_one`; `update` → `update_one` / `update_many`, or `replace_one` when given a replacement document (legacy update accepted both); `remove` → `delete_one` / `delete_many`; `findAndModify` → `find_and_modify_with_opts`; bulk insert/update → `*_with_opts`. The `noValidate` flags map to `validate: false`. Also moved: `get_database_names` → `_with_opts`, `bson_append_array_begin` → `bson_append_array_unsafe_begin`, and Swift's deprecated `String(validatingUTF8:)`. The build now has no deprecation warnings.
- [x] Fix the two pool bugs. `tryPopClient()` wraps the client from `try_pop`. `init(uri:)` frees its URI and traps with the parse error on an invalid URI; before, libmongoc crashed on a failed assertion instead.
- [x] Pooled clients go back to the pool when they're released, instead of being destroyed. Each popped client keeps its pool alive.
- [x] `distinct()` leak fixed.
- [x] Ownership: databases keep their client alive; collections keep their client or database; cursors keep their collection; `GridFS` keeps its client; `GridFile`s from `list`/`search`/`upload` keep their `GridFS`.
- [x] README rewritten: current requirements, building libmongoc 2 on Linux, example. `README.zh_CN.md` is translated from it (2026-09-29), and each links to the other.
- [x] GitHub Actions (`.github/workflows/ci.yml`): Linux job in the `swift:6.4-noble` container builds libmongoc 2.5.5 from source (cached), runs the full suite against a `mongo:8` service (tests read `MONGODB_URI`). The macOS job does a Homebrew build plus the BSON tests. Green since run 36366712375. Getting there fixed a Linux-only build error, which means the package hadn't built on Linux under current Swift: glibc's `fwrite`/`fclose` need a non-optional `FILE*`. It also fixed a pointer that outlived its buffer in GridFS `download(to:)`.
- [x] Tests added for every update form, bulk writes, `findAndModify`, the pool, and a collection outliving its client variable.
- [ ] Test `mongodb+srv://` and TLS connection strings against Atlas. Needs an Atlas cluster.
- [ ] GridFS MD5: libmongoc 2 still supports it and it isn't deprecated there, so it's unchanged. Revisit if moving to `mongoc_gridfs_bucket_t`.

### Phase 3: libmongoc 2.x
Folded into Phase 1: the package now targets 2.x only. Linux CI builds libmongoc 2 from source (Phase 2).

### Phase 4: Swift-native API (added alongside the old one)
- [x] Ownership and `Sendable`: `MongoClientPool` is `final` and `@unchecked Sendable`, since libmongoc's pool is thread-safe. It's the only shared object. Clients, collections and cursors stay non-`Sendable` and are used by one piece of work at a time. `BSON.OID`, `BSONEncoder`, `BSONDecoder` and the new `MongoError` are `Sendable`.
- [x] async (`MongoAsync.swift`): `pool.withClient { client in ... }` pops a client, runs the body on a dedicated concurrent `DispatchQueue` behind a checked continuation, and pushes the client back even on throw. That's the same design as Perfect-CRUD's `withCRUDExecutor`. There are no per-method `async` overloads, so nothing collides with the sync names.
- [x] Codable (`BSONCodable.swift`): `BSONEncoder`/`BSONDecoder` read and write libbson directly through an intermediate tree. I wrote them fresh rather than porting `mongo-swift-driver`, which is built on its own pure-Swift BSON type. `Date` maps to datetime, `Data` to binary, `UUID` to binary subtype 4 and `BSON.OID` to ObjectId. `Int` and 64-bit integers become int64, smaller integers int32. Decoding accepts any BSON number that converts exactly. Unsupported BSON types (decimal128, regex, timestamp) throw a descriptive `typeMismatch`. `BSON.OID` is also `Codable` as its hex string for other coders.
- [x] Typed collection API (`MongoCollectionCodable.swift`): `insert(_:)`, `insert(contentsOf:)`, `find(_:filter:options:)`, `findOne`, `replaceOne`, `updateOne`, `updateMany`, `deleteOne`, `deleteMany`, `countDocuments`. They throw `MongoError` or `DecodingError` and use the `*_with_opts` APIs directly.
- [x] `AsyncSequence` cursor: `pool.find(User.self, database:collection:filter:options:batchSize:)` returns `MongoFindSequence`. One pooled client is held per iteration, documents are fetched and decoded in batches (default 100) per hop to the blocking queue, cancellation is checked between batches, and the client goes back to the pool when the loop ends, throws or breaks early. This is safe here but wasn't in Perfect-CRUD, because a libmongoc client can move between threads as long as only one uses it at a time.
- [x] Tests (`Phase4Tests.swift`, 9 tests): Codable round trip over every supported type, native BSON types in the output, numeric conversions and errors, typed CRUD, duplicate-key `MongoError`, 20 concurrent `withClient` tasks on a 4-client pool, and the find sequence (250 docs, early break releasing clients, decoding errors). Also passes under Thread Sanitizer; only the Swift code is instrumented, not libmongoc.
- Decision: **no deprecations yet.** The typed API is a parallel layer rather than a one-for-one replacement, and deprecating the old calls now would flood existing users with warnings before anyone has used the new ones. Revisit once the community requesters have tried it.
- [x] `MongoClientPool(validatingURI:)` throws `MongoError` for a bad URI or rejected options, using `mongoc_client_pool_new_with_error`. `init(uri:)` keeps its non-throwing signature and traps with that error.
- [x] macOS CI also runs the server-independent Codable tests (12 tests instead of 7).
- [ ] Not done, and worth doing on request: change streams (`watch`), aggregation into Codable types, transactions/sessions, and an async GridFS.

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
