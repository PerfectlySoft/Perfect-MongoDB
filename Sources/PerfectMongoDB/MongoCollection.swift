//
//  MongoCollection.swift
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

private extension UnsafeMutablePointer {
	init?(mutating: OpaquePointer?) {
		self.init(mutating)
	}
}

/// Enum of flags for insertion options
public enum MongoInsertFlag: Int {
	case none
	case continueOnError
	case noValidate

	var mongoFlag: mongoc_insert_flags_t {
		switch self {
		case .none:
			return MONGOC_INSERT_NONE
		case .continueOnError:
			return MONGOC_INSERT_CONTINUE_ON_ERROR
		case .noValidate:
			return mongoc_insert_flags_t(rawValue: MONGOC_INSERT_NO_VALIDATE)
		}
	}
}

/// Enum of flags for update options
public enum MongoUpdateFlag: Int {
	case none
	case upsert
	case multiUpdate
	case noValidate

	var mongoFlag: mongoc_update_flags_t {
		switch self {
		case .none:
			return MONGOC_UPDATE_NONE
		case .upsert:
			return MONGOC_UPDATE_UPSERT
		case .multiUpdate:
			return MONGOC_UPDATE_MULTI_UPDATE
		case .noValidate:
			return mongoc_update_flags_t(rawValue: MONGOC_UPDATE_NO_VALIDATE)
		}
	}
}

/// Struct used to access Mongo Query options
public struct MongoQueryFlag: OptionSet, Sendable {
	public let rawValue: Int

	var queryFlags: mongoc_query_flags_t {
		return mongoc_query_flags_t(UInt32(self.rawValue))
	}

	public init(rawValue: Int) {
		self.rawValue = rawValue
	}

	private init(_ queryFlag: mongoc_query_flags_t) {
		self.init(rawValue: Int(queryFlag.rawValue))
	}

	public static let none				= MongoQueryFlag(MONGOC_QUERY_NONE)
	public static let tailableCursor	= MongoQueryFlag(MONGOC_QUERY_TAILABLE_CURSOR)
	public static let secondaryOk		= MongoQueryFlag(MONGOC_QUERY_SECONDARY_OK)
	@available(*, deprecated, renamed: "secondaryOk")
	public static let slaveOk			= secondaryOk
	public static let opLogReplay       = MongoQueryFlag(MONGOC_QUERY_OPLOG_REPLAY)
	public static let noCursorTimeout   = MongoQueryFlag(MONGOC_QUERY_NO_CURSOR_TIMEOUT)
	public static let awaitData         = MongoQueryFlag(MONGOC_QUERY_AWAIT_DATA)
	public static let exhaust			= MongoQueryFlag(MONGOC_QUERY_EXHAUST)
	public static let partial			= MongoQueryFlag(MONGOC_QUERY_PARTIAL)
}

/// Enum of flags for remove options
public enum MongoRemoveFlag: Int {
	case none
	case singleRemove

	var mongoFlag: mongoc_remove_flags_t {
		switch self {
		case .none:
			return MONGOC_REMOVE_NONE
		case .singleRemove:
			return MONGOC_REMOVE_SINGLE_REMOVE
		}
	}
}

/// class to manage Mongo Geospatial indexing options
public class MongoIndexOptionsGeo {
	let twodSphereVersion: UInt8?
	let twodBitsPrecision: UInt8?
	let twodLocationMin: Double?
	let twodLocationMax: Double?
	let haystackBucketSize: Double?

	public init(twodSphereVersion: UInt8? = nil, twodBitsPrecision: UInt8? = nil, twodLocationMin: Double? = nil, twodLocationMax: Double? = nil, haystackBucketSize: Double? = nil) {
		self.twodSphereVersion = twodSphereVersion
		self.twodBitsPrecision = twodBitsPrecision
		self.twodLocationMin = twodLocationMin
		self.twodLocationMax = twodLocationMax
		self.haystackBucketSize = haystackBucketSize
	}

	func append(to opts: BSON) {
		if let twodSphereVersion = twodSphereVersion {
			opts.append(key: "2dsphereIndexVersion", int32: Int32(twodSphereVersion))
		}
		if let twodBitsPrecision = twodBitsPrecision {
			opts.append(key: "bits", int32: Int32(twodBitsPrecision))
		}
		if let twodLocationMin = twodLocationMin {
			opts.append(key: "min", double: twodLocationMin)
		}
		if let twodLocationMax = twodLocationMax {
			opts.append(key: "max", double: twodLocationMax)
		}
		if let haystackBucketSize = haystackBucketSize {
			opts.append(key: "bucketSize", double: haystackBucketSize)
		}
	}
}

/// class to manage Mongo indexing options
///
/// `dropDups` and `storageOptions` are accepted for source compatibility but ignored:
/// MongoDB 3.0 removed `dropDups`, and the storage options were never sent to the server.
public class MongoIndexOptions {
	let name: String?
	let background: Bool?
	let unique: Bool?
	let sparse: Bool?
	let expireAfterSeconds: Int32?
	let v: Int32?
	let defaultLanguage: String?
	let languageOverride: String?
	let weights: BSON?
	let geoOptions: MongoIndexOptionsGeo?

	public init(name: String? = nil, background: Bool? = nil, unique: Bool? = nil, dropDups: Bool? = nil, sparse: Bool? = nil,
				expireAfterSeconds: Int32? = nil, v: Int32? = nil, defaultLanguage: String? = nil, languageOverride: String? = nil,
		weights: BSON? = nil, geoOptions: MongoIndexOptionsGeo? = nil, storageOptions: MongoIndexStorageOptionType? = nil) {
		self.name = name
		self.background = background
		self.unique = unique
		self.sparse = sparse
		self.expireAfterSeconds = expireAfterSeconds
		self.v = v
		self.defaultLanguage = defaultLanguage
		self.languageOverride = languageOverride
		self.weights = weights
		self.geoOptions = geoOptions
	}

	/// The index model options document for mongoc_index_model_new.
	func optionsDocument(keys: BSON) -> BSON {
		let opts = BSON()
		if let name = name {
			opts.append(key: "name", string: name)
		} else if let kdoc = keys.doc, let generated = mongoc_collection_keys_to_index_string(toOpaque(kdoc)) {
			opts.append(key: "name", string: String(cString: generated))
			bson_free(generated)
		}
		if let background = background {
			opts.append(key: "background", bool: background)
		}
		if let unique = unique {
			opts.append(key: "unique", bool: unique)
		}
		if let sparse = sparse {
			opts.append(key: "sparse", bool: sparse)
		}
		if let expireAfterSeconds = expireAfterSeconds {
			opts.append(key: "expireAfterSeconds", int32: expireAfterSeconds)
		}
		if let v = v {
			opts.append(key: "v", int32: v)
		}
		if let defaultLanguage = defaultLanguage {
			opts.append(key: "default_language", string: defaultLanguage)
		}
		if let languageOverride = languageOverride {
			opts.append(key: "language_override", string: languageOverride)
		}
		if let weights = weights {
			opts.append(key: "weights", document: weights)
		}
		geoOptions?.append(to: opts)
		return opts
	}
}

/// Enum for storage options
public enum MongoIndexStorageOptionType: UInt32 {
	case mmapV1, wiredTiger
}

/// The MongoCollection class
public class MongoCollection {

	var ptr = OpaquePointer(bitPattern: 0)

    /// Result Status enum for a MongoDB event
	public typealias Result = MongoResult

    /**
     *  obtain access to a specified database and collection using the MongoClient
     *
     *  - parameter client: the MongoClient to be used
     *  - parameter databaseName: String database name
     *  - parameter collectionName: String collection name
     *
    */
	public init(client: MongoClient, databaseName: String, collectionName: String) {
		self.ptr = mongoc_client_get_collection(client.ptr, databaseName, collectionName)
	}

	init(rawPtr: OpaquePointer?) {
		self.ptr = rawPtr
	}
    
    deinit {
        close()
    }

    /// close connection to the current collection
	public func close() {
        guard let ptr = self.ptr else {
            return
        }
		mongoc_collection_destroy(ptr)
		self.ptr = nil
	}

    /**
     *  Insert **document** into the current collection returning a result status
     *  
     *  - parameter document: BSON document to be inserted
     *  - parameter flag: Optional MongoInsertFlag defaults to .None
     *
     *  - returns: Result object with status of insert
    */
	public func insert(document: BSON, flag: MongoInsertFlag = .none) -> Result {
        guard let doc = document.doc else {
            return .error(1, 1, "Invalid document")
        }
        guard let ptr = self.ptr else {
            return .error(1, 1, "Invalid collection")
        }
		var error = bson_error_t()
		let res = mongoc_collection_insert(ptr, flag.mongoFlag, toOpaque(doc), nil, &error)
		guard res == true else {
			return Result.fromError(error)
		}
		return .success
	}

    /**
     *  Insert **documents** into the current collection returning a result status
     *
     *  - parameter documents: BSON documents to be inserted
     *
     *  - returns: Result object with status of insert
    */
    public func insert(documents: [BSON]) -> Result {
        guard let ptr = self.ptr else {
            return .error(1, 1, "Invalid collection")
        }
        let bulk = mongoc_collection_create_bulk_operation_with_opts(ptr, nil)
        var error = bson_error_t()
        var reply = bson_t()
        defer {
            bson_destroy(toOpaque(&reply))
            mongoc_bulk_operation_destroy(bulk)
        }
        for document in documents {
            guard let doc = document.doc else {
                return .error(1, 1, "Invalid document")
            }
            mongoc_bulk_operation_insert(bulk, toOpaque(doc))
            // no need to destroy because "public func close()" does it
            // bson_destroy (doc)
        }
        guard mongoc_bulk_operation_execute(bulk, toOpaque(&reply), &error) != 0 else {
            return Result.fromError(error)
        }
        return .success
    }
	
	@available(*, deprecated, message: "Use update(selector: BSON, update: BSON, flag: MongoUpdateFlag")
	public func update(update: BSON, selector: BSON, flag: MongoUpdateFlag = .none) -> Result {
		return self.update(selector: selector, update: update, flag: flag)
	}
	
	/**
	*  Update the document found using **selector** with the **update** document returning a result status
	*
	*  - parameter selector: BSON document with selection criteria
	*  - parameter update: BSON document to be used to update
	*  - parameter flag: Optional MongoUpdateFlag defaults to .None
	*
	*  - returns: Result object with status of update
	*/
    public func update(selector: BSON, update: BSON, flag: MongoUpdateFlag = .none) -> Result {
        guard let sdoc = selector.doc else {
            return .error(1, 1, "Invalid selector document")
        }
        guard let udoc = update.doc else {
            return .error(1, 1, "Invalid update document")
        }
        guard let ptr = self.ptr else {
            return .error(1, 1, "Invalid collection")
        }
        var error = bson_error_t()
        let res = mongoc_collection_update(ptr, flag.mongoFlag, toOpaque(sdoc), toOpaque(udoc), nil, &error)
        guard res == true else {
            return Result.fromError(error)
        }
        return .success
    }

    /**
     *  Update the documents and return a result status
     *
     *  - parameter updates: Tuple of (selector: BSON, update: BSON)
     *
     *  - returns: Result object with status of update
     *
     *  How to use it!
     *
     *  var updates: [(selector: BSON, update: BSON)] = []
     *  guard var users = collection.find(query: BSON()) else {
     *      response.status = HTTPResponseStatus.custom(code: 404, message: "Collection users cannot perform find().")
     *      response.completed()
     *  return
     }
     *  for user in users {
     *      let oldBson = BSON()
     *      oldBson.append(key: "_id", oid: user.oid!)
     *      let innerBson = BSON()
     *      innerBson.append(key: "firstname", string: "Ciccio")
     *      let newdBson = BSON()
     *      newdBson.append(key: "$set", document: innerBson)
     *      updates.append((selector: oldBson, update: newdBson))
     *  }
     *  if case .error = collection.update(updates: updates) {
     *      response.status = HTTPResponseStatus.custom(code: 404, message: "Collection users cannot perform multiple update().")
     *      response.completed()
     *  return
     }
     */
    public func update(updates: [(selector: BSON, update: BSON)]) -> Result {
        guard let ptr = self.ptr else {
            return .error(1, 1, "Invalid collection")
        }
        let bulk = mongoc_collection_create_bulk_operation_with_opts(ptr, nil)
        var error = bson_error_t()
        var reply = bson_t()
        defer {
            bson_destroy(toOpaque(&reply))
            mongoc_bulk_operation_destroy(bulk)
        }
        for update in updates {
            guard let sdoc = update.selector.doc else {
                return .error(1, 1, "Invalid selector document")
            }
            guard let udoc = update.update.doc else {
                return .error(1, 1, "Invalid update document")
            }
            mongoc_bulk_operation_update(bulk, toOpaque(sdoc), toOpaque(udoc), false)
            // mongoc_bulk_operation_update_one(bulk, sdoc, udoc, true)
            // mongoc_bulk_operation_update_one_with_opts(bulk, sdoc, udoc, nil, &error)
            //mongoc_bulk_operation_update_many_with_opts(bulk, sdoc, udoc, nil, &error)
            // mongoc_bulk_operation_replace_one(bulk, sdoc, udoc, false)
            // Remongoc_bulk_operation_replace_one_with_opts(bulk, sdoc, udoc, nil, &error)
            // no need to destroy because "public func close()" does it
            // bson_destroy(sdoc)
            // bson_destroy(udoc)
        }
        guard mongoc_bulk_operation_execute(bulk, toOpaque(&reply), &error) == 1 else {
            return Result.fromError(error)
        }
        return .success
    }

    /**
     *  Remove the document found using **selector** returning a result status
     *
     *  - parameter selector: BSON document with selection criteria
     *  - parameter flag: Optional MongoRemoveFlag defaults to .None
     *
     *  - returns: Result object with status of removal
    */
	public func remove(selector sel: BSON, flag: MongoRemoveFlag = .none) -> Result {
        guard let sdoc = sel.doc else {
            return .error(1, 1, "Invalid selector document")
        }
        guard let ptr = self.ptr else {
            return .error(1, 1, "Invalid collection")
        }
		var error = bson_error_t()
		let res = mongoc_collection_remove(ptr, flag.mongoFlag, toOpaque(sdoc), nil, &error)
		guard res == true else {
			return Result.fromError(error)
		}
		return .success
	}

    /**
     *  Updates **document** returning a result status
     *
     *  - parameter document: BSON document to be saved
     *
     *  - returns: Result object with status of save
    */
    public func save(document doc: BSON) -> Result {
        guard let sdoc = doc.doc else {
            return .error(1, 1, "Invalid document")
        }
        guard let ptr = self.ptr else {
            return .error(1, 1, "Invalid collection")
        }
		var error = bson_error_t()
		let res = _perfect_collection_save(ptr, toOpaque(sdoc), &error)
		guard res == true else {
			return Result.fromError(error)
		}
		return .success
	}

    /**
     *  Renames the collection using **newDbName** and **newCollectionName**, with option to drop existing collection immediately instead of after the move, returning a result status
     *
     *  - parameter newDbName: String name for db after move
     *  - parameter newCollectionName: String name for collection after move
     *  - parameter dropExisting: Bool option to drop existing collection immediately instead of after move
     *
     *  - returns: Result object with status of renaming
    */
    public func rename(newDbName: String, newCollectionName: String, dropExisting: Bool) -> Result {
        guard let ptr = self.ptr else {
            return .error(1, 1, "Invalid collection")
        }
		var error = bson_error_t()
		let res = mongoc_collection_rename(ptr, newDbName, newCollectionName, dropExisting, &error)
		guard res == true else {
			return Result.fromError(error)
		}
		return .success
	}

    /**
     *  The collection name as a String
     *
     *  - returns: String the name of the current collection
    */
	public func name() -> String {
        guard let ptr = self.ptr else {
            return ""
        }
		return String(validatingUTF8: mongoc_collection_get_name(ptr)) ?? ""
	}

    /**
     *  Validates a collection. The method scans a collection’s data structures for correctness and returns a single document that describes the relationship between the logical collection and the physical representation of the data.
     *
     *  - parameter full: Optional. Specify true to enable a full validation and to return full statistics. MongoDB disables full validation by default because it is a potentially resource-intensive operation.
     *
     *  - returns: BSON document describing the relationship between the collection and its physical representation
    */
    public func validate(full: Bool = false) -> Result {
		let command = BSON()
		defer {
			command.close()
		}
		command.append(key: "validate", string: self.name())
		command.append(key: "full", bool: full)
		return runCommand(command)
	}

	/// Runs **command** against this collection's database and returns the reply document.
	func runCommand(_ command: BSON) -> Result {
        guard let ptr = self.ptr else {
            return .error(1, 1, "Invalid collection")
        }
        guard let cdoc = command.doc else {
            return .error(1, 1, "Invalid command document")
        }
		var error = bson_error_t()
        let reply = BSON()
        guard let rdoc = reply.doc else {
            return .error(1, 1, "Invalid reply document")
        }
		let res = mongoc_collection_command_simple(ptr, toOpaque(cdoc), nil, toOpaque(rdoc), &error)
		guard res == true else {
			return Result.fromError(error)
		}
		return .replyDoc(reply)
	}

    /**
     *  Returns statistics about the collection formatted according to the options document.
     * 
     *  - parameter options: a BSON document defining the format of return.
     *  - **The options document can contain the following fields and values**:

     *  - **scale**:	*number*, Optional. The scale used in the output to display the sizes of items. By default, output displays sizes in bytes. To display kilobytes rather than bytes, specify a scale value of 1024.
     *  - **indexDetails**:	*boolean*, Optional. If true, **stats()** returns index details in addition to the collection stats. Only works for WiredTiger storage engine. Defaults to false.
     *  - **indexDetailsKey**:	*document*, Optional. If **indexDetails** is true, you can use **indexDetailsKey** to filter index details by specifying the index key specification. Only the index that exactly matches **indexDetailsKey** will be returned. If no match is found, **indexDetails** will display statistics for all indexes.
     *  - **indexDetailsName**:	*string*, Optional. If **indexDetails** is true, you can use **indexDetailsName** to filter index details by specifying the index name. Only the index name that exactly matches **indexDetailsName** will be returned. If no match is found, **indexDetails** will display statistics for all indexes.
     *
     *  - returns: BSON document with formatted statistics or Results error document
    */
    public func stats(options: BSON) -> Result {
        guard let odoc = options.doc else {
            return .error(1, 1, "Invalid options document")
        }
		let command = BSON()
		defer {
			command.close()
		}
		command.append(key: "collStats", string: self.name())
		guard let cdoc = command.doc, bson_concat(toOpaque(cdoc), toOpaque(odoc)) else {
			return .error(1, 1, "Invalid options document")
		}
		return runCommand(command)
	}

    /**
     *  Selects documents in a collection and returns a cursor to the selected documents.
     *
     *  - parameter query:    Specifies selection filter using query operators. To return all documents in a collection, omit this- Parameter or pass an empty document ({}).
     *  - parameter fields:   Optional. Specifies the fields to return in the documents that match the query filter. To return all fields in the matching documents, omit this parameter.
     *  - parameter flags:    Optional. set queryFlags for the current search
     *  - parameter skip:     Optional. Skip the supplied number of records.
     *  - parameter limit:    Optional. return no more than the supplied number of records.
     *  - parameter batchSize:    Optional. Change number of automatically iterated documents.
     *
     *  - returns:	A cursor to the documents that match the query criteria. When the find() method “returns documents,” the method is actually returning a cursor to the documents.
    */
    public func find(query: BSON = BSON(), fields: BSON? = nil, flags: MongoQueryFlag = MongoQueryFlag.none, skip: Int = 0, limit: Int = 0, batchSize: Int = 0) -> MongoCursor? {
		//	@available(*, deprecated, message: "Use find(filter: BSON, options: BSON?)")

		guard let ptr = self.ptr else {
            return nil
        }
        guard let qdoc = query.doc else {
            return nil
        }
		let cursor = _perfect_collection_find(ptr, flags.queryFlags, UInt32(skip), Int32(limit), UInt32(batchSize), toOpaque(qdoc), toOpaque(fields?.doc))
		guard cursor != nil else {
			return nil
		}
		return MongoCursor(rawPtr: cursor)
	}
	
	/**
	*  Selects documents in a collection and returns a cursor to the selected documents.
	*
	*  - parameter filter:    Specifies selection filter using query operators. To return all documents in a collection, omit this Parameter.
	*  - parameter options:   Optional. Specifies the query options, including sort order and which fields to return.
	*
	*  - returns:	A cursor to the documents that match the query criteria. When the find() method “returns documents,” the method is actually returning a cursor to the documents.
	*/
//	public func find(filter: BSON = BSON(), options: BSON? = nil) -> MongoCursor? {
//		guard let ptr = self.ptr else {
//			return nil
//		}
//		let cursor = mongoc_collection_find_with_opts(ptr, filter.doc, options?.doc, nil)
//		guard cursor != nil else {
//			return nil
//		}
//		return MongoCursor(rawPtr: cursor)
//	}

    /**
     *  Creates indexes on collections.
     *  
     *  - parameter keys:     A document that conains the field and value pairs where the field is the index key and the value describes the type of index for that field. For an ascending index on a field, specify a value of 1; for descending index, specify a value of -1.
     *  - parameter options:  Optional. A document that contains a set of options that controls the creation of the index. see MongoIndexOptions for details.
     *
     *  - returns: a Result status
    */
    public func createIndex(keys: BSON, options: MongoIndexOptions) -> Result {
        guard let ptr = self.ptr else {
            return .error(1, 1, "Invalid collection")
        }
        guard let kdoc = keys.doc else {
            return .error(1, 1, "Invalid keys document")
        }
		let opts = options.optionsDocument(keys: keys)
		defer {
			opts.close()
		}
		guard let model = mongoc_index_model_new(toOpaque(kdoc), toOpaque(opts.doc)) else {
			return .error(1, 1, "Invalid index options")
		}
		defer {
			mongoc_index_model_destroy(model)
		}
		var models: [OpaquePointer?] = [model]
		var error = bson_error_t()
		let res = mongoc_collection_create_indexes_with_opts(ptr, &models, 1, nil, nil, &error)
		guard res == true else {
			return Result.fromError(error)
		}
		return .success
	}

    /**
     *  Drops or removes the specified index from a collection.
     *  
     *  - parameter index: Specifies the index to drop, either by name or by the index specification document.
     *
     *  - returns: a Result status
    */
	public func dropIndex(name: String) -> Result {
        guard let ptr = self.ptr else {
            return .error(1, 1, "Invalid collection")
        }
		var error = bson_error_t()
		let res = mongoc_collection_drop_index(ptr, name, &error)
		guard res == true else {
			return Result.fromError(error)
		}
		return .success
	}

    /**
     *  Removes a collection from the database. The method also removes any indexes associated with the dropped collection.
     *
     *  - returns: a Result status
    */
	public func drop() -> Result {
        guard let ptr = self.ptr else {
            return .error(1, 1, "Invalid collection")
        }
		var error = bson_error_t()
		let res = mongoc_collection_drop(ptr, &error)
		guard res == true else {
			return Result.fromError(error)
		}
		return .success
	}

    /**
     *  The count of documents that would match a find() query.
     *
     *  - parameter query:    The query selection criteria.
     *  - parameter flags:    Optional. set queryFlags for the current search
     *  - parameter skip:     Optional. Skip the supplied number of records.
     *  - parameter limit:    Optional. return no more than the supplied number of records.
     *  - parameter batchSize:    Optional. Change number of automatically iterated documents.
     *
     *  - returns: the count of documents that would match a find() query. The count() method does not perform the find() operation but instead counts and returns the number of results that match a query.
     */
    public func count(query: BSON, flags: MongoQueryFlag = MongoQueryFlag.none, skip: Int = 0, limit: Int = 0, batchSize: Int = 0) -> Result {
        guard let ptr = self.ptr else {
            return .error(1, 1, "Invalid collection")
        }
        guard let qdoc = query.doc else {
            return .error(1, 1, "Invalid query document")
        }
		var error = bson_error_t()
		let ires = _perfect_collection_count(ptr, flags.queryFlags, toOpaque(qdoc), Int64(skip), Int64(limit), &error)
		guard ires != -1 else {
			return Result.fromError(error)
		}
		return .replyInt(Int(ires))
	}

    /**
     *  Modifies and returns a single document.
     *  
     *  - parameter query:    Optional. The selection criteria for the modification. The query field employs the same query selectors as used in the db.collection.find() method. Although the query may match multiple documents, findAndModify() will only select one document to modify.
     *  - parameter sort:     Optional. Determines which document the operation modifies if the query selects multiple documents. findAndModify() modifies the first document in the sort order specified by this argument.
     *  - parameter update:   Must specify either the remove or the update field. Performs an update of the selected document. The update field employs the same update operators or field: value specifications to modify the selected document.
     *  - parameter fields:   Optional. A subset of fields to return. The fields document specifies an inclusion of a field with 1, as in: fields: { <field1>: 1, <field2>: 1, ... }.
     *  - parameter remove:   Must specify either the remove or the update field. Removes the document specified in the query field. Set this to true to remove the selected document . The default is false.
     *  - parameter upsert:   Optional. Used in conjunction with the update field. When true, findAndModify() creates a new document if no document matches the query, or if documents match the query, findAndModify() performs an update. To avoid multiple upserts, ensure that the query fields are uniquely indexed. The default is false.
     *  - parameter new:      Optional. When true, returns the modified document rather than the original. The findAndModify() method ignores the new option for remove operations. The default is false.
     *
     *  - returns: Modifies and returns a single document. By default, the returned document does not include the modifications made on the update. To return the document with the modifications made on the update, use the new option.
    */
    public func findAndModify(query: BSON?, sort: BSON?, update: BSON?, fields: BSON?, remove: Bool, upsert: Bool, new: Bool) -> Result {
        guard let ptr = self.ptr else {
            return .error(1, 1, "Invalid collection")
        }
        if update == nil && !remove {
            return .error(1, 1, "Either update or remove must be given")
        }
		var error = bson_error_t()
        let reply = BSON()
        guard let rdoc = reply.doc else {
            return .error(1, 1, "Invalid reply document")
        }
		let res = mongoc_collection_find_and_modify(ptr, toOpaque(query?.doc), toOpaque(sort?.doc), toOpaque(update?.doc), toOpaque(fields?.doc), remove, upsert, new, toOpaque(rdoc), &error)
		guard res == true else {
			return Result.fromError(error)
		}
		return .replyDoc(reply)
	}

    /**
     *  A BSON document with description of last transaction status
     *
     *  - returns: BSON document with description of last transaction status
    */
	@available(*, deprecated, message: "libmongoc 2 no longer records the last error on the collection; this always returns an empty document. Check the Result returned by each operation instead.")
	public func getLastError() -> BSON {
		return BSON()
	}
    
    /**
     *  Finds the distinct values and returns a cursor for a specified field across a single collection.
     *
     *  - parameter key:    The field for which to return distinct values.
     *  - parameter query:    Optional. A query that specifies the documents from which to retrieve the distinct values.
     *  - parameter readConcern:    Optional. Specifies a level of isolation for read operations.
     *  - parameter flags:    Optional. set queryFlags for the current search
     *  - parameter skip:     Optional. Skip the supplied number of records.
     *  - parameter limit:    Optional. return no more than the supplied number of records.
     *  - parameter batchSize:    Optional. Change number of automatically iterated documents.
     *
     *  - returns:	BSON document with distinct document.
     */
    public func distinct(key: String, query: BSON? = nil, readConcern: BSON? = nil, flags: MongoQueryFlag = MongoQueryFlag.none, skip: Int = 0, limit: Int = 0, batchSize: Int = 0) -> BSON? {
        let command = BSON()
        defer { command.close() }
        
        command.append(key: "distinct", string: self.name())
        command.append(key: "key", string: key)
        if let query = query {
            command.append(key: "query", document: query)
        }
        if let readConcern = readConcern {
            command.append(key: "readConcern", document: readConcern)
        }
        let cursor = self.command(command: command, fields: nil, flags: flags, skip: skip, limit: limit, batchSize: batchSize)
        
        guard let result  = cursor?.next() else {
            return nil
        }
        
        return NoDestroyBSON(document: result)
    }
    
    /**
     *  Runs specified database command.
     *
     *  - parameter command:    Database command.
     *  - parameter fields:   Optional. Specifies the fields to return in the documents that match the query filter. To return all fields in the matching documents, omit this parameter.
     *  - parameter flags:    Optional. set queryFlags for the current search
     *  - parameter skip:     Optional. Skip the supplied number of records.
     *  - parameter limit:    Optional. return no more than the supplied number of records.
     *  - parameter batchSize:    Optional. Change number of automatically iterated documents.
     *
     *  - returns:	A cursor to the command execution result documents.
     */
    public func command(command: BSON, fields: BSON? = nil, flags: MongoQueryFlag = MongoQueryFlag.none, skip: Int = 0, limit: Int = 0, batchSize: Int = 0) -> MongoCursor? {
        // libmongoc 2 has no cursor-returning command API. The command's reply is returned
        // as a one-document cursor; fields, skip, limit and batchSize never applied to commands.
        guard self.ptr != nil, command.doc != nil else {
            return nil
        }
        guard case .replyDoc(let reply) = runCommand(command) else {
            return MongoCursor(documents: [])
        }
        return MongoCursor(documents: [reply])
    }
    
    /**
     *  Calculates aggregate values for the data in a collection.
     *
     *  - parameter pipeline:    A sequence of data aggregation operations or stages.
     *  - parameter flags:   Optional. set queryFlags for the current aggregation.
     *  - parameter options:    Optional. Additional options that aggregate() passes to the aggregate command.
     *
     *  - returns:	A cursor to the command execution result documents.
     */
    public func aggregate(pipeline: BSON, flags: MongoQueryFlag = MongoQueryFlag.none, options: BSON? = nil) -> MongoCursor? {
        guard let ptr = self.ptr else {
            return nil
        }
        guard let piplineDoc = pipeline.doc else {
            return nil
        }
        
        let cursor = mongoc_collection_aggregate(ptr, flags.queryFlags, toOpaque(piplineDoc), toOpaque(options?.doc), nil)
        
        guard cursor != nil else {
            return nil
        }
        
        return MongoCursor(rawPtr: cursor)
    }
}
