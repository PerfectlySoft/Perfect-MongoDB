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

#if swift(>=3.0)
	extension UnsafeMutablePointer {
		public static func allocatingCapacity(_ num: Int) -> UnsafeMutablePointer<Pointee> {
			return UnsafeMutablePointer<Pointee>(allocatingCapacity: num)
		}
	}
#else
	typealias ErrorProtocol = ErrorType
	typealias OpaquePointer = COpaquePointer
	typealias OptionSet = OptionSetType
	
	extension String {
		init?(validatingUTF8: UnsafePointer<Int8>) {
			if let s = String.fromCString(validatingUTF8) {
				self.init(s)
			} else {
				return nil
			}
		}
	}
	
	extension UnsafeMutablePointer {
		static func allocatingCapacity(num: Int) -> UnsafeMutablePointer<Memory> {
			return UnsafeMutablePointer<Memory>.allocatingCapacity(num)
		}
	
		var pointee: Memory {
			get { return self.memory }
			set { self.memory = newValue }
		}
		func deallocateCapacity(num: Int) {
			self.dealloc(num)
		}
		
		func deinitialize(count count: Int) {
			self.destroy(count)
		}
	}
#endif

/// MongoDatabase is an open-source document database that provides high performance, high availability, and automatic scaling.
public class MongoDatabase {

	var ptr: OpaquePointer? = OpaquePointer(bitPattern: 0)

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
		var error = bson_error_t()
		if mongoc_database_drop(self.ptr!, &error) {
			return .Success
		}
		return Result.fromError(error)
	}

    /// - returns: a string, the name of the current database
	public func name() -> String {
		return String(validatingUTF8: mongoc_database_get_name(self.ptr!))!
	}

    /**
     *  Create new Collection
     *
     *  - parameter name:     String, name of collection to be created
     *  - parameter options:  BSON document listing options for new collection
     *
     *  - returns: MongoCollection
    */
	public func createCollection(name collectionName: String, options: BSON) -> Result {
		var error = bson_error_t()
	#if swift(>=3.0)
		guard let col = mongoc_database_create_collection(self.ptr!, collectionName, options.doc!, &error) else {
			return Result.fromError(error)
		}
	#else
		let col = mongoc_database_create_collection(self.ptr!, collectionName, options.doc!, &error)
		guard nil != col else {
			return Result.fromError(error)
		}
	#endif
		return .ReplyCollection(MongoCollection(rawPtr: col))
	}

    /**
     *  MongoCollection referenced by name
     *
     *  - parameter name: String collection name
     *
     *  - returns: MongoCollection
    */
	public func getCollection(name collectionName: String) -> MongoCollection {
		let col = mongoc_database_get_collection(self.ptr!, collectionName)
        return MongoCollection(rawPtr: col)
	}
    
    /// - returns: String Array of current database collections' names
	public func collectionNames() -> [String] {
		var ret = [String]()
	#if swift(>=3.0)
		guard let names = mongoc_database_get_collection_names(self.ptr!, nil) else {
			return ret
		}
		var curr = names
		while let pointee = curr.pointee {
			ret.append(String(validatingUTF8: pointee) ?? "")
			curr = curr.successor()
		}
	#else
		let names = mongoc_database_get_collection_names(self.ptr!, nil)
		guard nil != names else {
			return ret
		}
		var curr = names
		while nil != curr.pointee {
			ret.append(String(validatingUTF8: curr.pointee) ?? "")
			curr = curr.successor()
		}
	#endif
		bson_strfreev(names)
		return ret
	}
}

