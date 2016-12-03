Perfect - MongoDB 数据库连接器 [English](README.md)
===========================

<p align="center">
    <a href="http://perfect.org/get-involved.html" target="_blank">
        <img src="http://perfect.org/assets/github/perfect_github_2_0_0.jpg" alt="Get Involed with Perfect!" width="854" />
    </a>
</p>

<p align="center">
    <a href="https://github.com/PerfectlySoft/Perfect" target="_blank">
        <img src="http://www.perfect.org/github/Perfect_GH_button_1_Star.jpg" alt="Star Perfect On Github" />
    </a>  
    <a href="http://stackoverflow.com/questions/tagged/perfect" target="_blank">
        <img src="http://www.perfect.org/github/perfect_gh_button_2_SO.jpg" alt="Stack Overflow" />
    </a>  
    <a href="https://twitter.com/perfectlysoft" target="_blank">
        <img src="http://www.perfect.org/github/Perfect_GH_button_3_twit.jpg" alt="Follow Perfect on Twitter" />
    </a>  
    <a href="http://perfect.ly" target="_blank">
        <img src="http://www.perfect.org/github/Perfect_GH_button_4_slack.jpg" alt="Join the Perfect Slack" />
    </a>
</p>

<p align="center">
    <a href="https://developer.apple.com/swift/" target="_blank">
        <img src="https://img.shields.io/badge/Swift-3.0-orange.svg?style=flat" alt="Swift 3.0">
    </a>
    <a href="https://developer.apple.com/swift/" target="_blank">
        <img src="https://img.shields.io/badge/Platforms-OS%20X%20%7C%20Linux%20-lightgray.svg?style=flat" alt="Platforms OS X | Linux">
    </a>
    <a href="http://perfect.org/licensing.html" target="_blank">
        <img src="https://img.shields.io/badge/License-Apache-lightgrey.svg?style=flat" alt="License Apache">
    </a>
    <a href="http://twitter.com/PerfectlySoft" target="_blank">
        <img src="https://img.shields.io/badge/Twitter-@PerfectlySoft-blue.svg?style=flat" alt="PerfectlySoft Twitter">
    </a>
    <a href="http://perfect.ly" target="_blank">
        <img src="http://perfect.ly/badge.svg" alt="Slack Status">
    </a>
</p>


本项目封装了 mongo-c 客户端函数库，因此可以使用 Swift 访问 MongoDB 服务器。

本项目是
[Perfect](https://github.com/PerfectlySoft/Perfect) 软件体系的一部分，但是可以独立运行，不依赖于 PerfectLib 基本库。
请确保您已经正确安装了最新版本的 Swift 3.0 工具链。




## 问题报告

我们正在过渡到 JIRA 程序错误管理系统，因此 GitHub 的问题报告功能就被禁用了。

如果您发现任何问题，或有任何意见和建议，请在我们的 JIRA 工作台指出 [http://jira.perfect.org:8080/servicedesk/customer/portal/1](http://jira.perfect.org:8080/servicedesk/customer/portal/1)。

目前的问题清单请查阅 [http://jira.perfect.org:8080/projects/ISS/issues](http://jira.perfect.org:8080/projects/ISS/issues)

OS X 注意事项
----------------

本程序依赖于 [Homebrew](http://brew.sh) 发行的 mongo-c 函数库。 

如果您要安装 Homebrew:

~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
/usr/bin/ruby -e "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/master/install)"
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

安装 mongo-c 的方法:

~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
brew install mongo-c-driver
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Linux 注意事项
-----------------

请确定以下函数库已经预装：

```
apt-get install libmongoc-dev libbson-dev libssl-dev
```

另外，Perfect 默认 libmongoc 安装在 /usr/local/include 目录下。如果不是的话，请您手工增加链接：

```
ln -s /usr/include/libmongoc-1.0/ libmongoc-1.0
```

编译
--------

请在您的 Package.swift 文件下增加以下内容：

```swift
.Package(url:"https://github.com/PerfectlySoft/Perfect-MongoDB.git", majorVersion: 2, minor: 0)
```

快速上手
-----------

您可以直接克隆我们的模板工程：

```
git clone https://github.com/PerfectlySoft/PerfectTemplate.git
cd PerfectTemplate
```

并为 Package.swift 文件增加依存关系：

```swift
let package = Package(
 name: "PerfectTemplate",
 targets: [],
 dependencies: [
     .Package(url:"https://github.com/PerfectlySoft/PerfectLib.git", majorVersion: 2, minor: 0),
     .Package(url:"https://github.com/PerfectlySoft/Perfect-MongoDB.git", majorVersion: 2, minor: 0)
    ]
)
```

按照下面的命令创建 Xcode 工程文件夹

```
swift package generate-xcodeproj
```

然后您就可以在 Xcode 里面打开工程 `PerfectTemplate.xcodeproj` 。

该项目将在 Xcode 下编译，并在运行时启动服务器监听 8181 端口。

>   **Important:** 如果您改变了 Package.swift 文件，则必须重新运行```generate-xcodeproj```脚本，而且所有 Xcode 的配置都会被覆盖。

连接到 MongoDB 并进行查询
----------------------------------------------------

在 Xcode 中，打开 ```Sources/PerfectTemplate/main.swift```，然后更新以下代码：

```swift
import PerfectLib

// 初始化基本服务
PerfectServer.initializeServices()

// 增加路由
addURLRoutes()

do {
    // 启动 HTTP 服务并监听 8181 端口
    try HTTPServer(documentRoot: "./webroot").start(port: 8181)
} catch PerfectError.networkError(let err, let msg) {
    print("网络异常 \(err) \(msg)")
}
```

在您的源代码目录下与文件 `main.swift` 同一级别下创建一个新文件并命名为 `routingHandlers.swift`

下一步

-   为 PerfectLib 和 MongoDB 连接器设置导入；

-   增加测试路由；

-   注册路由到服务器上。

```swift
import PerfectLib
import MongoDB

func addURLRoutes() {
    Routing.Routes["/test" ] = testHandler
    Routing.Routes["/mongo" ] = mongoHandler
}

// 将所有路由都注册到服务器上。
public func PerfectServerModuleInit() {
    addURLRoutes()
}
```

注意上面的程序追加了两个路由。

路由句柄 “/test” 用于返回一个 “你好，世界！” 的 JSON 字符串。

```swift
func testHandler(request: WebRequest, _ response: WebResponse) {
    let returning = "{你好，世界！}"
    response.appendBody(string: returning)
    response.requestCompleted()
}
```

更多路由例子请参考 “URL Routing” 示例：
(<https://github.com/PerfectlySoft/PerfectExample-URLRouting>)

MongoDB 访问句柄请参考如下：

```swift
func mongoHandler(request: WebRequest, _ response: WebResponse) {

    // 创建连接
    let client = try! MongoClient(uri: "mongodb://localhost")

    // 连接到具体的数据库，假设有个数据库名字叫 test
    let db = client.getDatabase(name: "test")

    // 定义集合
    guard let collection = db.getCollection(name: "testcollection") else {
        return
    }

    // 在关闭连接时注意关闭顺序与启动顺序相反
    defer {
        collection.close()
        db.close()
        client.close()
    }

    // 执行查询
    let fnd = collection.find(query: BSON())

    // 初始化一个空数组用于存放结果记录集
    var arr = [String]()

    // "fnd" 游标是一个 MongoCursor 类型，用于遍历结果
    for x in fnd! {
        arr.append(x.asString)
    }

    // 返回一个格式化的 JSON 数组。
    let returning = "{\"data\":[\(arr.joined(separator: ","))]}"

    // 返回 JSON 字符串
    response.appendBody(string: returning)
    response.requestCompleted()
}
```

## 更多信息
关于 Perfect 软件函数库的更多信息，请访问官网： [perfect.org](http://perfect.org).
