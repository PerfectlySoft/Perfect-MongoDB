//
//  MongoCursor.swift
//  MongoDB
//
//  Created by Kyle Jessup on 2015-11-19.
//  Copyright © 2015 PerfectlySoft. All rights reserved.
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

import libmongoc

/// The Mongo Cursor interface
public class MongoCursor {

	var ptr: OpaquePointer? = OpaquePointer(bitPattern: 0)

	init(rawPtr: OpaquePointer?) {
		self.ptr = rawPtr
	}
    
    deinit {
        close()
    }
    
    /// Close and destroy current cursor
	public func close() {
		if self.ptr != nil {
			mongoc_cursor_destroy(self.ptr!)
			self.ptr = nil
		}
	}
    
    /// return next document if available, else nil
	public func next() -> BSON? {
		var bson = UnsafePointer<bson_t>(nil)
		if mongoc_cursor_next(self.ptr!, &bson) {
			return NoDestroyBSON(rawBson: UnsafeMutablePointer<bson_t>(bson))
		}
		return nil
	}
}
