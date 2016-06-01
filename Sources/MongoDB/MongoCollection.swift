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

import libmongoc
/// Enum of flags for insertion options
public enum MongoInsertFlag: Int {
	case None
	case ContinueOnError
	case NoValidate

	private var mongoFlag: mongoc_insert_flags_t {
		switch self {
		case .None:
			return MONGOC_INSERT_NONE
		case .ContinueOnError:
			return MONGOC_INSERT_CONTINUE_ON_ERROR
		case .NoValidate:
			return mongoc_insert_flags_t(rawValue: MONGOC_INSERT_NO_VALIDATE)
		}
	}
}

/// Enum of flags for update options
public enum MongoUpdateFlag: Int {
	case None
	case Upsert
	case MultiUpdate
	case NoValidate

	private var mongoFlag: mongoc_update_flags_t {
		switch self {
		case .None:
			return MONGOC_UPDATE_NONE
		case .Upsert:
			return MONGOC_UPDATE_UPSERT
		case .MultiUpdate:
			return MONGOC_UPDATE_MULTI_UPDATE
		case .NoValidate:
			return mongoc_update_flags_t(rawValue: MONGOC_UPDATE_NO_VALIDATE)
		}
	}
}

/// Struct used to access Mongo Query options
public struct MongoQueryFlag: OptionSet {
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

	static let None				= MongoQueryFlag(MONGOC_QUERY_NONE)
	static let TailableCursor	= MongoQueryFlag(MONGOC_QUERY_TAILABLE_CURSOR)
	static let SlaveOk			= MongoQueryFlag(MONGOC_QUERY_SLAVE_OK)
	static let OpLogReplay		= MongoQueryFlag(MONGOC_QUERY_OPLOG_REPLAY)
	static let NoCursorTimeout	= MongoQueryFlag(MONGOC_QUERY_NO_CURSOR_TIMEOUT)
	static let AwaitData		= MongoQueryFlag(MONGOC_QUERY_AWAIT_DATA)
	static let Exhaust			= MongoQueryFlag(MONGOC_QUERY_EXHAUST)
	static let Partial			= MongoQueryFlag(MONGOC_QUERY_PARTIAL)
}

/// Enum of flags for remove options
public enum MongoRemoveFlag: Int {
	case None
	case SingleRemove

	private var mongoFlag: mongoc_remove_flags_t {
		switch self {
		case .None:
			return MONGOC_REMOVE_NONE
		case .SingleRemove:
			return MONGOC_REMOVE_SINGLE_REMOVE
		}
	}
}

/// class to manage Mongo Geospatial indexing options
public class MongoIndexOptionsGeo {
	var rawOpt = UnsafeMutablePointer<mongoc_index_opt_geo_t>.allocatingCapacity(1)

	public init(twodSphereVersion: UInt8? = nil, twodBitsPrecision: UInt8? = nil, twodLocationMin: Double? = nil, twodLocationMax: Double? = nil, haystackBucketSize: Double? = nil) {
		mongoc_index_opt_geo_init(self.rawOpt)
		if let twodSphereVersion = twodSphereVersion {
			self.rawOpt.pointee.twod_sphere_version = twodSphereVersion
		}
		if let twodBitsPrecision = twodBitsPrecision {
			self.rawOpt.pointee.twod_bits_precision = twodBitsPrecision
		}
		if let twodLocationMin = twodLocationMin {
			self.rawOpt.pointee.twod_location_min = twodLocationMin
		}
		if let twodLocationMax = twodLocationMax {
			self.rawOpt.pointee.twod_location_max = twodLocationMax
		}
		if let haystackBucketSize = haystackBucketSize {
			self.rawOpt.pointee.haystack_bucket_size = haystackBucketSize
		}
	}

	deinit {
		self.rawOpt.deinitialize(count: 1)
		self.rawOpt.deallocateCapacity(1)
	}
}

/// class to manage Mongo indexing options
public class MongoIndexOptions {

	var rawOpt = mongoc_index_opt_t()

	// who knows what the default options are.
	// guard against the case where these values were set to something in the defaults.
	// we don't want to free a pointer which isn't ours
	var nameNil: Bool, defLangNil: Bool, langOverNil: Bool
	var weightsDoc: BSON?
	var geoOptions: MongoIndexOptionsGeo?
	var storageOptions: UnsafeMutablePointer<mongoc_index_opt_storage_t>?

	public init(name: String? = nil, background: Bool? = nil, unique: Bool? = nil, dropDups: Bool? = nil, sparse: Bool? = nil,
				expireAfterSeconds: Int32? = nil, v: Int32? = nil, defaultLanguage: String? = nil, languageOverride: String? = nil,
		weights: BSON? = nil, geoOptions: MongoIndexOptionsGeo? = nil, storageOptions: MongoIndexStorageOptionType? = nil) {
		mongoc_index_opt_init(&self.rawOpt)

		self.nameNil = self.rawOpt.name == nil
		self.defLangNil = self.rawOpt.default_language == nil
		self.langOverNil = self.rawOpt.language_override == nil

		if let name = name {
			self.nameNil = true
			self.rawOpt.name = UnsafePointer<Int8>(strdup(name))
		}
		if let background = background {
			self.rawOpt.background = background
		}
		if let unique = unique {
			self.rawOpt.unique = unique
		}
		if let dropDups = dropDups {
			self.rawOpt.drop_dups = dropDups
		}
		if let sparse = sparse {
			self.rawOpt.sparse = sparse
		}
		if let expireAfterSeconds = expireAfterSeconds {
			self.rawOpt.expire_after_seconds = expireAfterSeconds
		}
		if let v = v {
			self.rawOpt.v = v
		}
		if let defaultLanguage = defaultLanguage {
			self.defLangNil = true
			self.rawOpt.default_language = UnsafePointer<Int8>(strdup(defaultLanguage))
		}
		if let languageOverride = languageOverride {
			self.langOverNil = true
			self.rawOpt.language_override = UnsafePointer<Int8>(strdup(languageOverride))
		}
		if let weights = weights {
			self.weightsDoc = weights // reference this so the ptr doesn't disappear beneath us
			self.rawOpt.weights = UnsafePointer<bson_t>(weights.doc!)
		}
		if let geoOptions = geoOptions {
			self.geoOptions = geoOptions
			self.rawOpt.geo_options = geoOptions.rawOpt
		}
		if let storageOptions = storageOptions {
			self.storageOptions = UnsafeMutablePointer<mongoc_index_opt_storage_t>.allocatingCapacity(1)
			self.storageOptions!.pointee.type = Int32(storageOptions.rawValue)
		}
	}

	deinit {
		if self.nameNil && self.rawOpt.name != nil {
			free(UnsafeMutablePointer<()>(self.rawOpt.name))
		}
		if self.defLangNil && self.rawOpt.default_language != nil {
			free(UnsafeMutablePointer<()>(self.rawOpt.default_language))
		}
		if self.langOverNil && self.rawOpt.language_override != nil {
			free(UnsafeMutablePointer<()>(self.rawOpt.language_override))
		}
		if self.storageOptions != nil {
			self.storageOptions!.deallocateCapacity(1)
		}
	}
}

/// Enum for storage options
public enum MongoIndexStorageOptionType: UInt32 {
	case MMapV1, WiredTiger

	var mongoType: UInt32 {
		switch self {
		case .MMapV1:
			return MONGOC_INDEX_STORAGE_OPT_MMAPV1.rawValue
		case .WiredTiger:
			return MONGOC_INDEX_STORAGE_OPT_WIREDTIGER.rawValue
		}
	}
}

/// The MongoCollection class
public class MongoCollection {

	var ptr: OpaquePointer? = OpaquePointer(bitPattern: 0)

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
		if self.ptr != nil {
			mongoc_collection_destroy(self.ptr!)
			self.ptr = nil
		}
	}

    /**
     *  Insert **document** into the current collection returning a result status
     *  
     *  - parameter document: BSON document to be inserted
     *  - parameter flag: Optional MongoInsertFlag defaults to .None
     *
     *  - returns: Result object with status of insert
    */
	public func insert(document doc: BSON, flag: MongoInsertFlag = .None) -> Result {
		var error = bson_error_t()
		let res = mongoc_collection_insert(self.ptr!, flag.mongoFlag, doc.doc!, nil, &error)
		guard res == true else {
			return Result.fromError(error)
		}
		return .Success
	}

    /**
     *  Update the document found using **selector** with the **update** document returning a result status
     *  
     *  - parameter update: BSON document to be used to update
     *  - parameter selector: BSON document with selection criteria
     *  - parameter flag: Optional MongoUpdateFlag defaults to .None
     *
     *  - returns: Result object with status of update
    */
	public func update(update: BSON, selector: BSON, flag: MongoUpdateFlag = .None) -> Result {
		var error = bson_error_t()
		let res = mongoc_collection_update(self.ptr!, flag.mongoFlag, selector.doc!, update.doc!, nil, &error)
		guard res == true else {
			return Result.fromError(error)
		}
		return .Success
	}

    /**
     *  Remove the document found using **selector** returning a result status
     *
     *  - parameter selector: BSON document with selection criteria
     *  - parameter flag: Optional MongoRemoveFlag defaults to .None
     *
     *  - returns: Result object with status of removal
    */
	public func remove(selector sel: BSON, flag: MongoRemoveFlag = .None) -> Result {
		var error = bson_error_t()
		let res = mongoc_collection_remove(self.ptr!, flag.mongoFlag, sel.doc!, nil, &error)
		guard res == true else {
			return Result.fromError(error)
		}
		return .Success
	}

    /**
     *  Updates **document** returning a result status
     *
     *  - parameter document: BSON document to be saved
     *
     *  - returns: Result object with status of save
    */
	public func save(document doc: BSON) -> Result {
		var error = bson_error_t()
		let res = mongoc_collection_save(self.ptr!, doc.doc!, nil, &error)
		guard res == true else {
			return Result.fromError(error)
		}
		return .Success
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
		var error = bson_error_t()
		let res = mongoc_collection_rename(self.ptr!, newDbName, newCollectionName, dropExisting, &error)
		guard res == true else {
			return Result.fromError(error)
		}
		return .Success
	}

    /**
     *  The collection name as a String
     *
     *  - returns: String the name of the current collection
    */
	public func name() -> String {
		return String(validatingUTF8: mongoc_collection_get_name(self.ptr!))!
	}

    /**
     *  Validates a collection. The method scans a collection’s data structures for correctness and returns a single document that describes the relationship between the logical collection and the physical representation of the data.
     *
     *  - parameter options: Optional. Specify true to enable a full validation and to return full statistics. MongoDB disables full validation by default because it is a potentially resource-intensive operation.
     *
     *  - returns: BSON document describing the relationship between the collection and its physical representation
    */
	public func validate(options: BSON) -> Result {
		var error = bson_error_t()
		let reply = BSON()
		let res = mongoc_collection_validate(self.ptr!, options.doc!, reply.doc!, &error)
		guard res == true else {
			return Result.fromError(error)
		}
		return .ReplyDoc(reply)
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
		var error = bson_error_t()
		let reply = BSON()
		let res = mongoc_collection_stats(self.ptr!, options.doc!, reply.doc!, &error)
		guard res == true else {
			return Result.fromError(error)
		}
		return .ReplyDoc(reply)
	}

    /**
     *  Selects documents in a collection and returns a cursor to the selected documents.
     *
     *  - parameter query:    Optional. Specifies selection filter using query operators. To return all documents in a collection, omit this- Parameter or pass an empty document ({}).
     *  - parameter fields:   Optional. Specifies the fields to return in the documents that match the query filter. To return all fields in the matching documents, omit this parameter.
     *  - parameter flags:    Optional. set queryFlags for the current search
     *  - parameter skip:     Optional. Skip the supplied number of records.
     *  - parameter limit:    Optional. return no more than the supplied number of records.
     *  - parameter batchSize:    Optional. Change number of automatically iterated documents.
     *
     *  - returns:	A cursor to the documents that match the query criteria. When the find() method “returns documents,” the method is actually returning a cursor to the documents.
    */
	public func find(query: BSON, fields: BSON? = nil, flags: MongoQueryFlag = MongoQueryFlag.None, skip: Int = 0, limit: Int = 0, batchSize: Int = 0) -> MongoCursor? {
		let cursor = mongoc_collection_find(self.ptr!, flags.queryFlags, UInt32(skip), UInt32(limit), UInt32(batchSize), query.doc!, (fields == nil ? nil : fields!.doc)!, nil)
		guard cursor != nil else {
			return nil
		}
		return MongoCursor(rawPtr: cursor)
	}

    /**
     *  Creates indexes on collections.
     *  
     *  - parameter keys:     A document that conains the field and value pairs where the field is the index key and the value describes the type of index for that field. For an ascending index on a field, specify a value of 1; for descending index, specify a value of -1.
     *  - parameter options:  Optional. A document that contains a set of options that controls the creation of the index. see MongoIndexOptions for details.
     *
     *  - returns: a Result status
    */
	public func createIndex(keys: BSON, options: MongoIndexOptions) -> Result {
		var error = bson_error_t()
		let res = mongoc_collection_create_index(self.ptr!, keys.doc!, &options.rawOpt, &error)
		guard res == true else {
			return Result.fromError(error)
		}
		return .Success
	}

    /**
     *  Drops or removes the specified index from a collection.
     *  
     *  - parameter index: Specifies the index to drop, either by name or by the index specification document.
     *
     *  - returns: a Result status
    */
	public func dropIndex(name: String) -> Result {
		var error = bson_error_t()
		let res = mongoc_collection_drop_index(self.ptr!, name, &error)
		guard res == true else {
			return Result.fromError(error)
		}
		return .Success
	}

    /**
     *  Removes a collection from the database. The method also removes any indexes associated with the dropped collection.
     *
     *  - returns: a Result status
    */
	public func drop() -> Result {
		var error = bson_error_t()
		let res = mongoc_collection_drop(self.ptr!, &error)
		guard res == true else {
			return Result.fromError(error)
		}
		return .Success
	}

    /**
     *  The count of documents that would match a find() query.
     *
     *  - parameter query:    The query selection criteria.
     *  - parameter fields:   Optional. Specifies the fields to return in the documents that match the query filter. To return all fields in the matching documents, omit this parameter.
     *  - parameter flags:    Optional. set queryFlags for the current search
     *  - parameter skip:     Optional. Skip the supplied number of records.
     *  - parameter limit:    Optional. return no more than the supplied number of records.
     *  - parameter batchSize:    Optional. Change number of automatically iterated documents.
     *
     *  - returns: the count of documents that would match a find() query. The count() method does not perform the find() operation but instead counts and returns the number of results that match a query.
     */
	public func count(query: BSON, fields: BSON? = nil, flags: MongoQueryFlag = MongoQueryFlag.None, skip: Int = 0, limit: Int = 0, batchSize: Int = 0) -> Result {
		var error = bson_error_t()
		let ires = mongoc_collection_count(self.ptr!, flags.queryFlags, query.doc!, Int64(skip), Int64(limit), nil, &error)
		guard ires != -1 else {
			return Result.fromError(error)
		}
		return .ReplyInt(Int(ires))
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
	public func findAndModify(query: BSON, sort: BSON, update: BSON, fields: BSON, remove: Bool, upsert: Bool, new: Bool) -> Result {
		var error = bson_error_t()
		let reply = BSON()
		let res = mongoc_collection_find_and_modify(self.ptr!, query.doc!, sort.doc!, update.doc!, fields.doc!, remove, upsert, new, reply.doc!, &error)
		guard res == true else {
			return Result.fromError(error)
		}
		return .ReplyDoc(reply)
	}

    /**
     *  A BSON document with description of last transaction status
     *
     *  - returns: BSON document with description of last transaction status
    */
	public func getLastError() -> BSON {
		let reply = mongoc_collection_get_last_error(self.ptr!)
		return NoDestroyBSON(rawBson: UnsafeMutablePointer(reply))
	}

}
