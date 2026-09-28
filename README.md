# Perfect MongoDB

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

**This package is being modernized for Swift 6** on the `swift6-modernization` branch. It now builds in
Swift 6 language mode against libmongoc 2.x, with the existing public API unchanged. See
[Documentation/modernization-plan.md](Documentation/modernization-plan.md) for progress and for the few
behaviour changes. The 3.x releases (Swift 4/5, libmongoc 1.x) remain available by tag.

```swift
dependencies: [
    .package(url: "https://github.com/PerfectlySoft/Perfect-MongoDB.git", branch: "swift6-modernization")
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

For concurrent use, pop clients from a `MongoClientPool` rather than sharing one `MongoClient`.

## Testing

The tests expect a MongoDB server on `mongodb://localhost`:

```sh
mongod --dbpath /tmp/mongo-test
swift test
```
