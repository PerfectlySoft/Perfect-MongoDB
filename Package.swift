// swift-tools-version:4.2
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

let package = Package(
	name: "PerfectMongoDB",
	products: [
		.library(name: "PerfectMongoDB", targets: ["PerfectMongoDB"])
	],
	dependencies: [
		.package(url: "https://github.com/PerfectlySoft/PerfectLib.git", from: "3.0.0")
    ],
    targets: [
        .systemLibrary(name: "PerfectCMongo",
            pkgConfig: "libmongoc-1.0",
            providers: [
                .apt(["libmongoc-dev"]),
                .brew(["mongo-c-driver"])
            ]
        ),
        .systemLibrary(name: "PerfectCBSON",
            pkgConfig: "libbson-1.0",
            providers: [
                .apt(["libbson-dev"]),
                .brew(["mongo-c-driver"])
            ]
        ),
		.target(name: "PerfectMongoDB", dependencies: ["PerfectCMongo", "PerfectCBSON", "PerfectLib"]),
		.testTarget(name: "PerfectMongoDBTests", dependencies: ["PerfectMongoDB"])
    ]
)