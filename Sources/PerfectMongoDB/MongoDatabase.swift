//
//  MongoDatabase.swift
//  MongoDB
//
//  Created by Kyle Jessup on 2015-11-20.
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

/// MongoDatabase is an open-source document database that provides high performance, high availability, and automatic scaling.
public class MongoDatabase {

	var ptr = OpaquePointer(bitPattern: 0)

	public typealias Result = MongoResult

    /** 
     *  Get reference to named database using provided MongoClient instance
     *
     *  - parameter client:           a MongoClient
     *  - parameter databaseName:     A String identifying the requested database
     *
     *  - returns: a reference to the requested database
    */
	public init(client: MongoClient, databaseName: String) {
		self.ptr = mongoc_client_get_database(client.ptr, databaseName)
	}
    
    deinit {
        close()
    }

    /// Close connection to database
	public func close() {
		if self.ptr != nil {
			mongoc_database_destroy(self.ptr!)
			self.ptr = nil
		}
	}

    /// Drops the current database, deleting the associated data files
	public func drop() -> Result {
        guard let ptr = self.ptr else {
            return .error(1, 1, "Invalid database")
        }
		var error = bson_error_t()
		if mongoc_database_drop(ptr, &error) {
			return .success
		}
		return Result.fromError(error)
	}

    /// - returns: a string, the name of the current database
    public func name() -> String {
        guard let ptr = self.ptr else {
            return ""
        }
		return String(validatingUTF8: mongoc_database_get_name(ptr)) ?? ""
	}

    /**
     *  Create new Collection
     *
     *  - parameter name:     String, name of collection to be created
     *  - parameter options:  BSON document listing options for new collection
     *
     *  - returns: MongoCollection
    */
    public func createCollection(name collectionName: String, options: BSON?) -> Result {
        guard let ptr = self.ptr else {
            return .error(1, 1, "Invalid database")
        }
		var error = bson_error_t()
		guard let col = mongoc_database_create_collection(ptr, collectionName, options?.doc, &error) else {
			return Result.fromError(error)
		}
		return .replyCollection(MongoCollection(rawPtr: col))
	}

    /**
     *  MongoCollection referenced by name
     *
     *  - parameter name: String collection name
     *
     *  - returns: MongoCollection
    */
    public func getCollection(name collectionName: String) -> MongoCollection? {
        guard let ptr = self.ptr else {
            return nil
        }
		let col = mongoc_database_get_collection(ptr, collectionName)
        return MongoCollection(rawPtr: col)
	}
    
    /// - returns: String Array of current database collections' names
	public func collectionNames() -> [String] {
        var ret = [String]()
        guard let ptr = self.ptr else {
            return ret
        }
		guard let names = mongoc_database_get_collection_names(ptr, nil) else {
			return ret
		}
		var curr = names
		while let pointee = curr.pointee {
			ret.append(String(validatingUTF8: pointee) ?? "")
			curr = curr.successor()
		}
		bson_strfreev(names)
		return ret
	}
}

