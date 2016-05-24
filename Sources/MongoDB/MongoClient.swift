//
//  MongoClient.swift
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

/// Result Status for a MongoDB event
public enum MongoResult {
	case Success
	case Error(UInt32, UInt32, String)
	case ReplyDoc(BSON)
	case ReplyInt(Int)
	case ReplyCollection(MongoCollection)

	static func fromError(_ error: bson_error_t) -> MongoResult {
		var vError = error
		let message = withUnsafePointer(&vError.message) {
			String(validatingUTF8: UnsafePointer($0))!
		}
		return .Error(error.domain, error.code, message)
	}
}

public enum MongoClientError: ErrorProtocol {
    case InitError(String)
}

public class MongoClient {

	var ptr = OpaquePointer(bitPattern: 0)
    
    /// Result Status enum for a MongoDB event
	public typealias Result = MongoResult
    
    /// Create new Mongo Client connection
	public init(uri: String) throws {
		self.ptr = mongoc_client_new(uri)
        
        if nil == ptr {
            throw MongoClientError.InitError("Could not parse URI '\(uri)'")
        }
	}
	#if swift(>=3.0)
    init(pointer: OpaquePointer?) {
        ptr = pointer
    }
	#else
	init(pointer: OpaquePointer) {
		ptr = pointer
	}
	#endif
	
    deinit {
        close()
    }

    /// terminate current Mongo Client connection
	public func close() {
		if self.ptr != nil {
			mongoc_client_destroy(self.ptr)
			self.ptr = nil
		}
	}

    /// Return the specified MongoCollection from the specified database using current connection
	public func getCollection(databaseName: String, collectionName: String) -> MongoCollection {
		return MongoCollection(client: self, databaseName: databaseName, collectionName: collectionName)
	}

    /// Return the named database as a MongoDatabase object 
	public func getDatabase(name databaseName: String) -> MongoDatabase {
		return MongoDatabase(client: self, databaseName: databaseName)
	}

    /// Get current Mongo server status
	public func serverStatus() -> Result {
		var error = bson_error_t()
		let readPrefs = mongoc_read_prefs_new(MONGOC_READ_PRIMARY)
		defer {
			mongoc_read_prefs_destroy(readPrefs)
		}
		let bson = BSON()
		guard mongoc_client_get_server_status(self.ptr, readPrefs, bson.doc!, &error) else {
			return Result.fromError(error)
		}
		return .ReplyDoc(bson)
	}

    /// Build String Array of current database names
	public func databaseNames() -> [String] {
		var ret = [String]()
	#if swift(>=3.0)
		guard let names = mongoc_client_get_database_names(self.ptr, nil) else {
			return ret
		}
		
		var curr = names
		while let currPtr = curr[0] {
			ret.append(String(validatingUTF8: currPtr) ?? "")
			curr = curr.successor()
		}
	#else
		let names = mongoc_client_get_database_names(self.ptr, nil)
		guard nil != names else {
			return ret
		}
		
		var curr = names
		while nil != curr[0] {
			ret.append(String(validatingUTF8: curr[0]) ?? "")
			curr = curr.successor()
		}
	#endif
		bson_strfreev(names)
		
		return ret
	}

}
