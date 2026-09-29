//
//  MongoCollectionCodable.swift
//  PerfectMongoDB
//
//  Typed, throwing collection operations built on BSONEncoder/BSONDecoder. These sit
//  alongside the original BSON/MongoResult API; filters and options are BSON documents
//  as in the MongoDB manual, e.g. `try BSON(json: #"{"age": {"$gt": 30}}"#)`.
//
//===----------------------------------------------------------------------===//
//
// This source file is part of the Perfect.org open source project
//
// Copyright (c) 2015 - 2026 PerfectlySoft Inc. and the Perfect project authors
// Licensed under Apache License v2.0
//
// See http://perfect.org/licensing.html for license information
//
//===----------------------------------------------------------------------===//
//

import PerfectCMongo

public extension MongoCollection {
	/// Inserts **value** as a new document.
	func insert<T: Encodable>(_ value: T, encoder: BSONEncoder = BSONEncoder()) throws {
		let ptr = try validPointer()
		let document = try encoder.encode(value)
		var error = bson_error_t()
		guard mongoc_collection_insert_one(ptr, toOpaque(document.doc), nil, nil, &error) else {
			throw MongoError(error)
		}
	}

	/// Inserts **values** in one ordered bulk write.
	func insert<T: Encodable>(contentsOf values: [T], encoder: BSONEncoder = BSONEncoder()) throws {
		guard !values.isEmpty else {
			return
		}
		let ptr = try validPointer()
		let documents = try values.map { try encoder.encode($0) }
		var pointers: [UnsafePointer<bson_t>?] = documents.map { toOpaque($0.doc) }
		var error = bson_error_t()
		guard mongoc_collection_insert_many(ptr, &pointers, pointers.count, nil, nil, &error) else {
			throw MongoError(error)
		}
		withExtendedLifetime(documents) {}
	}

	/// Returns every document matching **filter**, decoded as **type**.
	/// **options** takes find options such as `sort`, `projection`, `skip` and `limit`.
	func find<T: Decodable>(_ type: T.Type, filter: BSON = BSON(), options: BSON? = nil, decoder: BSONDecoder = BSONDecoder()) throws -> [T] {
		let ptr = try validPointer()
		guard let cursor = mongoc_collection_find_with_opts(ptr, toOpaque(filter.doc), toOpaque(options?.doc), nil) else {
			throw MongoError("find failed")
		}
		defer {
			mongoc_cursor_destroy(cursor)
		}
		var results: [T] = []
		var document: UnsafePointer<bson_t>? = nil
		while mongoc_cursor_next(cursor, &document), let document {
			results.append(try decoder.decode(type, from: document))
		}
		var error = bson_error_t()
		if mongoc_cursor_error(cursor, &error) {
			throw MongoError(error)
		}
		return results
	}

	/// Returns the first document matching **filter**, decoded as **type**, or nil if none match.
	func findOne<T: Decodable>(_ type: T.Type, filter: BSON = BSON(), options: BSON? = nil, decoder: BSONDecoder = BSONDecoder()) throws -> T? {
		let limited = BSON()
		defer {
			limited.close()
		}
		if let options, let odoc = options.doc {
			bson_concat(toOpaque(limited.doc), toOpaque(odoc))
		}
		limited.append(key: "limit", int: 1)
		return try find(type, filter: filter, options: limited, decoder: decoder).first
	}

	/// Replaces the first document matching **filter** with **value**, inserting it when
	/// **upsert** is true and nothing matches. Returns the number of documents matched.
	@discardableResult
	func replaceOne<T: Encodable>(filter: BSON, with value: T, upsert: Bool = false, encoder: BSONEncoder = BSONEncoder()) throws -> Int {
		let ptr = try validPointer()
		let replacement = try encoder.encode(value)
		let opts = BSON()
		defer {
			opts.close()
		}
		opts.append(key: "upsert", bool: upsert)
		return try writeCount("matchedCount") { reply, error in
			mongoc_collection_replace_one(ptr, toOpaque(filter.doc), toOpaque(replacement.doc), toOpaque(opts.doc), reply, error)
		}
	}

	/// Applies the update operators in **update** (e.g. `{"$set": {...}}`) to the first
	/// document matching **filter**. Returns the number of documents matched.
	@discardableResult
	func updateOne(filter: BSON, update: BSON, upsert: Bool = false) throws -> Int {
		let ptr = try validPointer()
		let opts = BSON()
		defer {
			opts.close()
		}
		opts.append(key: "upsert", bool: upsert)
		return try writeCount("matchedCount") { reply, error in
			mongoc_collection_update_one(ptr, toOpaque(filter.doc), toOpaque(update.doc), toOpaque(opts.doc), reply, error)
		}
	}

	/// Applies the update operators in **update** to every document matching **filter**.
	/// Returns the number of documents matched.
	@discardableResult
	func updateMany(filter: BSON, update: BSON, upsert: Bool = false) throws -> Int {
		let ptr = try validPointer()
		let opts = BSON()
		defer {
			opts.close()
		}
		opts.append(key: "upsert", bool: upsert)
		return try writeCount("matchedCount") { reply, error in
			mongoc_collection_update_many(ptr, toOpaque(filter.doc), toOpaque(update.doc), toOpaque(opts.doc), reply, error)
		}
	}

	/// Deletes the first document matching **filter**. Returns the number deleted (0 or 1).
	@discardableResult
	func deleteOne(filter: BSON) throws -> Int {
		let ptr = try validPointer()
		return try writeCount("deletedCount") { reply, error in
			mongoc_collection_delete_one(ptr, toOpaque(filter.doc), nil, reply, error)
		}
	}

	/// Deletes every document matching **filter**. Returns the number deleted.
	@discardableResult
	func deleteMany(filter: BSON) throws -> Int {
		let ptr = try validPointer()
		return try writeCount("deletedCount") { reply, error in
			mongoc_collection_delete_many(ptr, toOpaque(filter.doc), nil, reply, error)
		}
	}

	/// The number of documents matching **filter**.
	func countDocuments(filter: BSON = BSON()) throws -> Int {
		let ptr = try validPointer()
		var error = bson_error_t()
		let count = mongoc_collection_count_documents(ptr, toOpaque(filter.doc), nil, nil, nil, &error)
		guard count >= 0 else {
			throw MongoError(error)
		}
		return Int(count)
	}
}

extension MongoCollection {
	func validPointer() throws -> OpaquePointer {
		guard let ptr = self.ptr else {
			throw MongoError("Invalid collection")
		}
		return ptr
	}

	/// Runs a write that fills a reply document and returns the reply's integer **field**.
	func writeCount(_ field: String, _ write: (UnsafeMutablePointer<bson_t>, UnsafeMutablePointer<bson_error_t>) -> Bool) throws -> Int {
		let reply = UnsafeMutablePointer<bson_t>.allocate(capacity: 1)
		defer {
			reply.deallocate()
		}
		var error = bson_error_t()
		let ok = write(reply, &error)
		// libmongoc always initializes the reply, even on failure.
		defer {
			bson_destroy(reply)
		}
		guard ok else {
			throw MongoError(error)
		}
		var iter = bson_iter_t()
		guard bson_iter_init_find(&iter, reply, field) else {
			return 0
		}
		return Int(bson_iter_as_int64(&iter))
	}
}
