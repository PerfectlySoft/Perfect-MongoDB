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

import PerfectCMongo

/// The Mongo Cursor interface
public class MongoCursor: Sequence, IteratorProtocol {
    
	var ptr = OpaquePointer(bitPattern: 0)
    
    /// JSON string representation.
    public var jsonString: String {
        
        var results = [String]()
        
        for object in self {
            results.append(object.asString)
        }
        
        return "[\(results.joined(separator: ","))]"
    }

	/// Documents served in place of a libmongoc cursor, for command replies.
	var documents: [BSON] = []

	/// The collection this cursor came from, kept alive while the cursor is in use.
	var owner: AnyObject?

	init(rawPtr: OpaquePointer?, owner: AnyObject? = nil) {
		self.ptr = rawPtr
		self.owner = owner
	}

	init(documents: [BSON]) {
		self.documents = documents
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
		documents.removeAll()
	}
    
    /// - returns: next document if available, else nil
	public func next() -> BSON? {
        guard let ptr = self.ptr else {
            return documents.isEmpty ? nil : documents.removeFirst()
        }
		
		var bson = UnsafeRawPointer(nil as OpaquePointer?)
		// this func is in PerfectCMongo shim.h
		if _mongoc_cursor_next(ptr, &bson), let bson = bson?.assumingMemoryBound(to: bson_t.self) {
			return NoDestroyBSON(rawBson: UnsafeMutablePointer(mutating: bson))
		}
		
// the code before swift 5
//		var bson = UnsafePointer<bson_t>(nil as OpaquePointer?)
//		if mongoc_cursor_next(ptr, &bson) {
//			return NoDestroyBSON(rawBson: UnsafeMutablePointer(mutating: bson))
//		}
		
		return nil
	}
}
