# Perfect MongoDB

[English](README.md)

<p align="center">
    <img src="https://img.shields.io/badge/Swift-6-orange.svg?style=flat" alt="Swift 6">
    <img src="https://img.shields.io/badge/Platforms-macOS%2012%2B%20%7C%20Linux-lightgray.svg?style=flat" alt="Platforms macOS 12+ | Linux">
    <a href="LICENSE"><img src="https://img.shields.io/badge/License-Apache%202.0-lightgrey.svg?style=flat" alt="License Apache 2.0"></a>
</p>

本项目是 MongoDB 官方 C 语言驱动 [libmongoc](https://github.com/mongodb/mongo-c-driver) 的 Swift 封装，
涵盖客户端与连接池、数据库、集合、游标、BSON 文档以及 GridFS。

MongoDB 已于 2023 年停止开发官方的服务器端 Swift 驱动。libmongoc 是 MongoDB 仍在维护、并持续符合其驱动规范
（服务器发现、可重试写入、身份验证、TLS）的驱动，因此本项目基于它构建。

**4.0 版本针对 Swift 6 对本项目进行了现代化改造。** 它在 Swift 6 语言模式下基于 libmongoc 2.x 编译，保留了
现有公共 API，并新增了 Codable 与 async/await API。由 libmongoc 2 导致的少数行为变化列于
[Documentation/modernization-plan.md](Documentation/modernization-plan.md)（英文）。

4.0 之前的版本（Swift 4/5、libmongoc 1.x）保留在 [`legacy`](../../tree/legacy) 分支以及 3.x 标签中。

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

## 环境要求

必须安装 libmongoc **2.x**，并确保 pkg-config 能找到 `mongoc2` 和 `bson2`。

**macOS**（[Homebrew](https://brew.sh)）：

```sh
brew install mongo-c-driver
```

在 Apple 芯片的 Mac 上，如果 SwiftPM 找不到该库，请设置 `PKG_CONFIG_PATH=/opt/homebrew/lib/pkgconfig`。

**Linux：** 大多数发行版仍只提供 libmongoc 1.x（例如 Ubuntu 的 `libmongoc-dev`），本版本不支持。请参照
`.github/workflows/ci.yml` 中 CI 的做法，从源码编译 libmongoc 2：

```sh
apt-get install cmake libssl-dev libsasl2-dev libzstd-dev
curl -fsSL https://github.com/mongodb/mongo-c-driver/releases/download/2.5.5/mongo-c-driver-2.5.5.tar.gz | tar xz
cmake -S mongo-c-driver-2.5.5 -B build -DCMAKE_BUILD_TYPE=Release -DENABLE_TESTS=OFF -DENABLE_EXAMPLES=OFF
cmake --build build --parallel && sudo cmake --install build
```

## 示例

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

### Codable 与 async/await

`BSONEncoder` 和 `BSONDecoder` 在 `Codable` 类型与 BSON 文档之间转换，日期、二进制数据、UUID 和 ObjectId
都保留为原生 BSON 类型。集合提供了基于它们的类型化、可抛出错误的方法。并发场景下请共享同一个
`MongoClientPool`：`withClient` 会把阻塞的驱动调用放到 Swift 协作线程池之外执行，`find` 则以 `AsyncSequence`
的形式流式返回结果：

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

`MongoClient` 以及由它创建的集合和游标都不是线程安全的：同一时间只能在一个任务中使用，`withClient` 和
`find` 会替你保证这一点。

## 测试

测试需要在 `mongodb://localhost` 上运行的 MongoDB 服务器：

```sh
mongod --dbpath /tmp/mongo-test
swift test
```

如需像 CI 一样在本地的 Linux 环境中运行测试（Swift 6.4、libmongoc 2.5.5、MongoDB 8），请使用 Apple 的
[`container`](https://github.com/apple/container) 工具：

```sh
container system start          # 只需执行一次
Scripts/test-linux.sh           # 额外参数会传给 swift test，例如 --filter Phase4Tests
```

首次运行会把 libmongoc 编译到一个缓存卷中（不到一分钟），之后每次运行只需几秒钟。
