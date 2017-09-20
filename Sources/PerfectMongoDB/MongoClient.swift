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

/**
 *  Result Status for a MongoDB event
 */
public enum MongoResult {
	case success
	case error(UInt32, UInt32, String)
	case replyDoc(BSON)
	case replyInt(Int)
	case replyCollection(MongoCollection)

	static func fromError(_ error: bson_error_t) -> MongoResult {
		var vError = error
		let message = withUnsafePointer(to: &vError.message) {
			return $0.withMemoryRebound(to: CChar.self, capacity: 0) {
				String(validatingUTF8: $0) ?? "unknown error"
			}
		}
		return .error(error.domain, error.code, message)
	}
}
/**
 *  ErrorType for MongoClient error reporting
 */
public enum MongoClientError: Error {
    /**
     *  returns error string
     */
    case initError(String)
}

public class MongoClient {

	var ptr = OpaquePointer(bitPattern: 0)
    
    /**
     *  Result Status enum for a MongoDB event
    */
	public typealias Result = MongoResult
    
    /**
     *  Create new Mongo Client connection
     *
     * - throws: MongoClientError "Could not parse URI" if nil response
     *
    */
	public init(uri: String) throws {
        guard let ptr = mongoc_client_new(uri) else {
            throw MongoClientError.initError("Could not parse URI '\(uri)'")
        }
        self.ptr = ptr
	}

    init(pointer: OpaquePointer?) {
        ptr = pointer
    }
	
    deinit {
        close()
    }

    /// terminate current Mongo Client connection
	public func close() {
        guard let ptr = self.ptr else {
            return
        }
        mongoc_client_destroy(ptr)
        self.ptr = nil
	}

    /**
     *  Return the specified MongoCollection from the specified database using current connection
     *
     *  - parameter databaseName: String name of database to be used
     *  - parameter collectionName: String name of collection to be retrieved
     *
     *  - returns: MongoCollection from specified database
    */
	public func getCollection(databaseName: String, collectionName: String) -> MongoCollection {
		return MongoCollection(client: self, databaseName: databaseName, collectionName: collectionName)
	}

    /**
     *  Return the named database as a MongoDatabase object
     * 
     *  - parameter name: String name of database to be retrieved
     *  - returns: a MongoDatabase object
    */
	public func getDatabase(name databaseName: String) -> MongoDatabase {
		return MongoDatabase(client: self, databaseName: databaseName)
	}

    /** 
     *  Get current Mongo server status
     *
     *  - returns: a Result object representing the server status
    */
	public func serverStatus() -> Result {
		var error = bson_error_t()
		let readPrefs = mongoc_read_prefs_new(MONGOC_READ_PRIMARY)
		defer {
			mongoc_read_prefs_destroy(readPrefs)
		}
		let bson = BSON()
        guard let doc = bson.doc else {
            return .error(1, 1, "Invalid BSON doc")
        }
		guard mongoc_client_get_server_status(self.ptr, readPrefs, doc, &error) else {
			return Result.fromError(error)
		}
		return .replyDoc(bson)
	}

    /** 
     *  Build String Array of current database names
     *
     * - returns: [String] of current database names
    */
	public func databaseNames() -> [String] {
		var ret = [String]()
		guard let names = mongoc_client_get_database_names(self.ptr, nil) else {
			return ret
		}
		var curr = names
		while let currPtr = curr[0] {
			ret.append(String(validatingUTF8: currPtr) ?? "")
			curr = curr.successor()
		}
		bson_strfreev(names)
		return ret
	}
}
