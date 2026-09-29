# Perfect MongoDB

[简体中文](README.zh_CN.md)

<p align="center">
    <img src="https://img.shields.io/badge/Swift-6-orange.svg?style=flat" alt="Swift 6">
    <img src="https://img.shields.io/badge/Platforms-macOS%2012%2B%20%7C%20Linux-lightgray.svg?style=flat" alt="Platforms macOS 12+ | Linux">
    <a href="LICENSE"><img src="https://img.shields.io/badge/License-Apache%202.0-lightgrey.svg?style=flat" alt="License Apache 2.0"></a>
</p>

A Swift wrapper around MongoDB's C driver, [libmongoc](https://github.com/mongodb/mongo-c-driver). It covers
clients and connection pools, databases, collections, cursors, BSON documents and GridFS.

MongoDB stopped developing its official server-side Swift driver in 2023. libmongoc is the driver MongoDB
still maintains and keeps compliant with its specifications (server discovery, retryable writes,
authentication, TLS), so this package builds on it.

**Version 4 modernizes this package for Swift 6.** It builds in Swift 6 language mode against libmongoc 2.x,
keeps the existing public API, and adds Codable and async/await APIs. A few behaviour changes, forced by
libmongoc 2, are listed in [Documentation/modernization-plan.md](Documentation/modernization-plan.md).

The pre-4.0 version (Swift 4/5, libmongoc 1.x) is preserved on the [`legacy`](../../tree/legacy) branch and
the 3.x tags.

```swift
dependencies: [
    .package(url: "https://github.com/PerfectlySoft/Perfect-MongoDB.git", from: "4.0.0")
],
targets: [
    .target(name: "MyTarget", dependencies: [
        .product(name: "PerfectMongoDB", package: "Perfect-MongoDB")
    ])
]
```

## Requirements

libmongoc **2.x** must be installed where pkg-config can find `mongoc2` and `bson2`.

**macOS** ([Homebrew](https://brew.sh)):

```sh
brew install mongo-c-driver
```

On Apple silicon, if SwiftPM can't find the library, set `PKG_CONFIG_PATH=/opt/homebrew/lib/pkgconfig`.

**Linux:** most distributions still package libmongoc 1.x (for example Ubuntu's `libmongoc-dev`), which
this version doesn't support. Build libmongoc 2 from source, as the CI workflow in
`.github/workflows/ci.yml` does:

```sh
apt-get install cmake libssl-dev libsasl2-dev libzstd-dev
curl -fsSL https://github.com/mongodb/mongo-c-driver/releases/download/2.5.5/mongo-c-driver-2.5.5.tar.gz | tar xz
cmake -S mongo-c-driver-2.5.5 -B build -DCMAKE_BUILD_TYPE=Release -DENABLE_TESTS=OFF -DENABLE_EXAMPLES=OFF
cmake --build build --parallel && sudo cmake --install build
```

## Example

```swift
import PerfectMongoDB

let client = try MongoClient(uri: "mongodb://localhost")
let users = client.getDatabase(name: "app").getCollection(name: "users")!

_ = users.insert(document: try BSON(json: #"{"name": "Ada", "age": 36}"#))

if let cursor = users.find(query: try BSON(json: #"{"age": {"$gt": 30}}"#)) {
    for user in cursor {
        print(user.asString)
    }
}
```

### Codable and async/await

`BSONEncoder` and `BSONDecoder` map `Codable` types to BSON documents, keeping dates, binary data, UUIDs and
ObjectIds as native BSON types. Collections have typed, throwing methods built on them. For concurrency,
share one `MongoClientPool`. `withClient` runs blocking driver work off Swift's cooperative thread pool,
and `find` streams results as an `AsyncSequence`:

```swift
struct User: Codable, Sendable {
    var _id: BSON.OID
    var name: String
    var age: Int
    var joined: Date
}

let pool = try MongoClientPool(validatingURI: "mongodb://localhost")

try await pool.withClient { client in
    let users = client.getCollection(databaseName: "app", collectionName: "users")
    try users.insert(User(_id: BSON.OID(), name: "Ada", age: 36, joined: Date()))
    try users.updateOne(filter: try BSON(json: #"{"name": "Ada"}"#),
                        update: try BSON(json: #"{"$inc": {"age": 1}}"#))
}

for try await user in pool.find(User.self, database: "app", collection: "users",
                                filter: try BSON(json: #"{"age": {"$gt": 30}}"#)) {
    print(user.name)
}
```

A `MongoClient` and the collections and cursors made from it are not thread-safe: use each from one task at
a time, which `withClient` and `find` do for you.

## Testing

The tests expect a MongoDB server on `mongodb://localhost`:

```sh
mongod --dbpath /tmp/mongo-test
swift test
```

To run the suite on Linux locally the way CI does (Swift 6.4, libmongoc 2.5.5, MongoDB 8), use Apple's
[`container`](https://github.com/apple/container) tool:

```sh
container system start          # once
Scripts/test-linux.sh           # extra arguments go to swift test, e.g. --filter Phase4Tests
```

The first run compiles libmongoc into a cached volume (under a minute); later runs take seconds.
