// swift-tools-version:4.1
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
		.package(url: "https://github.com/PerfectSideRepos/Perfect-CMongo.git", from: "0.1.0"),
		.package(url: "https://github.com/PerfectSideRepos/Perfect-CBSON.git", from: "0.0.0"),
		.package(url: "https://github.com/PerfectlySoft/PerfectLib.git", from: "3.0.0")
	],
	targets: [
		.target(name: "PerfectMongoDB", dependencies: ["PerfectLib"]),
		.testTarget(name: "PerfectMongoDBTests", dependencies: ["PerfectMongoDB"])
	]
)
