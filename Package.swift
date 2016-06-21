//
//  Package.swift
//  Perfect-MongoDB
//
//  Created by Kyle Jessup on 3/22/16.
//	Copyright (C) 2016 PerfectlySoft, Inc.
//
//===----------------------------------------------------------------------===//
//
// This source file is part of the Perfect.org open source project
//
// Copyright (c) 2015 - 2016 PerfectlySoft Inc. and the Perfect project authors
// Licensed under Apache License v2.0
//
// See http://perfect.org/licensing.html for license information
//
//===----------------------------------------------------------------------===//
//

import PackageDescription

#if os(OSX)
let package = Package(
    name: "MongoDB",
    targets: [],
    dependencies: [
        .Package(url: "https://github.com/PerfectlySoft/Perfect-mongo-c.git", versions: Version(0,0,0)..<Version(10,0,0))
    ],
    exclude: ["Sources/libmongoc"]
)
#else
let package = Package(
    name: "MongoDB",
    targets: [],
    dependencies: [
        .Package(url: "https://github.com/PerfectlySoft/Perfect-mongo-c.git", versions: Version(0,0,0)..<Version(10,0,0))
    ],
    exclude: ["Sources/libmongoc"]
)
#endif
