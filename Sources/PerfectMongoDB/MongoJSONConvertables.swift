//
//  MongoJSONConvertables.swift
//  MongoDB
//
//  Created by thislooksfun on 2017-07-09.
//  Copyright © 2017 PerfectlySoft. All rights reserved.
//
//===----------------------------------------------------------------------===//
//
// This source file is part of the Perfect.org open source project
//
// Copyright (c) 2016 - 2017 PerfectlySoft Inc. and the Perfect project authors
// Licensed under Apache License v2.0
//
// See http://perfect.org/licensing.html for license information
//
//===----------------------------------------------------------------------===//
//

import Foundation

/// MongoDB extended JSON encodings for Foundation types.
///
/// These used to conform to PerfectLib's `JSONConvertible`. The package no longer
/// depends on PerfectLib; code that needs the conformance can restore it with
/// `extension Date: JSONConvertible {}`.

extension Date {
	public func jsonEncodedString() throws -> String {
		return "{\"$date\":{\"$numberLong\":\"\(Int64(self.timeIntervalSince1970 * 1000))\"}}"
	}
}
