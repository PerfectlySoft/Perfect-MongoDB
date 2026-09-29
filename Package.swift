// swift-tools-version:6.0
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
	platforms: [
		.macOS(.v12)
	],
	products: [
		.library(name: "PerfectMongoDB", targets: ["PerfectMongoDB"])
	],
	targets: [
		.systemLibrary(
			name: "PerfectCBSON",
			pkgConfig: "bson2",
			providers: [
				.brew(["mongo-c-driver"]),
				.apt(["libbson-dev"])
			]
		),
		.systemLibrary(
			name: "PerfectCMongo",
			pkgConfig: "mongoc2",
			providers: [
				.brew(["mongo-c-driver"]),
				.apt(["libmongoc-dev"])
			]
		),
		.target(
			name: "PerfectMongoDB",
			dependencies: ["PerfectCBSON", "PerfectCMongo"],
			swiftSettings: [.swiftLanguageMode(.v6)]
		),
		.testTarget(
			name: "PerfectMongoDBTests",
			dependencies: ["PerfectMongoDB"],
			swiftSettings: [.swiftLanguageMode(.v6)]
		)
	]
)
