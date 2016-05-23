//
//  BSON.swift
//  BSON
//
//  Created by Kyle Jessup on 2015-11-18.
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

/// BSON error enum
public enum BSONError: ErrorProtocol {
	/// The JSON data was malformed.
	case SyntaxError(String)
}

/// BSON class 
public class BSON: CustomStringConvertible {
	var doc: UnsafeMutablePointer<bson_t>

    /// Return JSON representation of current BSON contents as a String
	public var description: String {
		return self.asString
	}

    /**
     * init():
     *
     * Allocates a new doc structure. Call the various append()
     * functions to add fields to the bson. You can iterate the doc at any
     * time using a bson_iter_t and bson_iter_init().
     *
     * Returns: A newly allocated doc that should be freed with deinit().
     */
	public init() {
		self.doc = bson_new()
	}
    
    /**
     * init(bytes):
     * parameter bytes: A byte array containing a serialized bson document.
     *
     * Creates a new doc structure using the data provided. bytes should contain 
     * bytes that can be copied into the new doc structure.
     *
     * Returns: A newly allocated doc that should be freed with deinit().
     */
	public init(bytes: [UInt8]) {
		self.doc = bson_new_from_data(bytes, bytes.count)
	}

    /**
     * init(json):
     * parameter json: A string containing a json data.
     *
     * Creates a new doc structure using the data provided. json should contain
     * bytes that can be copied into the new doc structure.
     *
     * Returns: A newly allocated doc that should be freed with deinit().
     */
	public init(json: String) throws {
		var error = bson_error_t()
		self.doc = bson_new_from_json(json, json.utf8.count, &error)
		if self.doc == nil {
			let message = withUnsafePointer(&error.message) {
				String(validatingUTF8: UnsafePointer($0))!
			}
			throw BSONError.SyntaxError(message)
		}
	}
    
    /**
     * init(document):
     * parameter document: An existing bson document.
     *
     * Creates a new doc by copying the provided bson doc.
     *
     * Returns: A newly allocated doc that should be freed with deinit().
     */
	public init(document: BSON) {
		self.doc = bson_copy(document.doc)
	}

	init(rawBson: UnsafeMutablePointer<bson_t>) {
		self.doc = rawBson
	}
    
    deinit {
        close()
    }

    /// close, destroy and release the current BSON document
	public func close() {
		if self.doc != nil {
			bson_destroy(self.doc)
			self.doc = nil
		}
	}

    /**
     * asString:
     * Creates a new string containing current document in extended JSON format. 
     *
     * See http://docs.mongodb.org/manual/reference/mongodb-extended-json/ for
     * more information on extended JSON.
     *
     * Returns: String
     */
	public var asString: String {
		var length = 0
		let data = bson_as_json(self.doc, &length)
		defer {
			bson_free(data)
		}
		return String(validatingUTF8: data)!
	}
    
    /** like asString() but for outermost arrays. */
	public var asArrayString: String {
		var length = 0
		let data = bson_array_as_json(self.doc, &length)
		defer {
			bson_free(data)
		}
		return String(validatingUTF8: data)!
	}

    /**
     * asBytes:
     *
     * Returns: A byte array from current BSON document
     */
	public var asBytes: [UInt8] {
		let length = Int(self.doc.pointee.len)
		let data = bson_get_data(self.doc)
		var ret = [UInt8]()
		for i in 0..<length {
			ret.append(data[i])
		}
		return ret
	}

    /**
     * append(key, document):
     * Parameter key: The key for the field.
     * Parameter document: Existing BSON document.
     *
     * Appends a new field to self.doc of the type BSON_TYPE_DOCUMENT.
     * The documents contents will be copied into self.doc.
     *
     * Returns: true if successful; false if append would overflow max size.
     */
	public func append(key: String, document: BSON) -> Bool {
		return bson_append_document(self.doc, key, -1, document.doc)
	}

    /**
     * append(key):
     * Parameter key: The key for the field.
     *
     * Appends a new field to self.doc with NULL for the value.
     *
     * Returns: true if successful; false if append would overflow max size.
     */
	public func append(key: String) -> Bool {
		return bson_append_null(self.doc, key, -1)
	}
    
    /**
     * append(key, oid):
     * Parameter key: The key for the field.
     * Parameter oid: bson_oid_t.
     *
     * Appends a new field to the self.doc of type BSON_TYPE_OID using the contents of
     *  oid.
     *
     * Returns: true if successful; false if append would overflow max size.
     */
	public func append(key: String, oid: bson_oid_t) -> Bool {
		var cpy = oid
		return bson_append_oid(self.doc, key, -1, &cpy)
	}

    /**
     * append(key, int):
     * Parameter key: The key for the field.
     * Parameter value: The Int 64-bit integer value.
     *
     * Appends a new field of type BSON_TYPE_INT64 to self.doc .
     *
     * Returns: true if successful; false if append would overflow max size.
     */
	public func append(key: String, int: Int) -> Bool {
		return bson_append_int64(self.doc, key, -1, Int64(int))
	}

    /**
     * append(key, int32):
     * Parameter key: The key for the field.
     * Parameter value: The Int32 32-bit integer value.
     *
     * Appends a new field of type BSON_TYPE_INT32 to self.doc .
     *
     * Returns: true if successful; false if append would overflow max size.
     */
	public func append(key: String, int32: Int32) -> Bool {
		return bson_append_int32(self.doc, key, -1, int32)
	}

    /**
     * append(key, dateTime):
     * Parameter key: The key for the field.
     * Parameter value: The number of milliseconds elapsed since UNIX epoch.
     *
     * Appends a new field to self.doc of type BSON_TYPE_DATE_TIME.
     *
     * Returns: true if sucessful; otherwise false.
     */
	public func append(key: String, dateTime: Int64) -> Bool {
		return bson_append_date_time(self.doc, key, -1, dateTime)
	}

    /**
     * append(key, time):
     * Parameter key: The key for the field.
     * Parameter value: A time_t.
     *
     * Appends a BSON_TYPE_DATE_TIME field to self.doc using the time_t @value for the
     * number of seconds since UNIX epoch in UTC.
     *
     * Returns: true if successful; false if append would overflow max size.
     */
	public func append(key: String, time: time_t) -> Bool {
		return bson_append_time_t(self.doc, key, -1, time)
	}

    /**
     * append(key, double):
     * Parameter key: The key for the field.
     *
     * Appends a new field to self.doc of the type BSON_TYPE_DOUBLE.
     *
     * Returns: true if successful; false if append would overflow max size.
     */
	public func append(key: String, double: Double) -> Bool {
		return bson_append_double(self.doc, key, -1, double)
	}

    /**
     * append(key, bool):
     * Parameter key: The key for the field.
     * Parameter value: The boolean value.
     *
     * Appends a new field to self.doc of type BSON_TYPE_BOOL.
     *
     * Returns: true if successful; false if append would overflow max size.
     */
	public func append(key: String, bool: Bool) -> Bool {
		return bson_append_bool(self.doc, key, -1, bool)
	}

    /**
     * append(key, string):
     * Parameter key: The key for the field.
     * Parameter value: A UTF-8 encoded string.
     *
     * Appends a new field to self.doc using @key as the key and @value as the UTF-8
     * encoded value.
     *
     * Returns: true if successful; false if append would overflow max size.
     */
	public func append(key: String, string: String) -> Bool {
		return bson_append_utf8(self.doc, key, -1, string, -1)
	}

	public func append(key: String, bytes: [UInt8]) -> Bool {
		return bson_append_binary(self.doc, key, -1, BSON_SUBTYPE_BINARY, bytes, UInt32(bytes.count))
	}

	public func append(key: String, regex: String, options: String) -> Bool {
		return bson_append_regex(self.doc, key, -1, regex, options)
	}

	public func countKeys() -> Int {
		return Int(bson_count_keys(self.doc))
	}

	public func hasField(key: String) -> Bool {
		return bson_has_field(self.doc, key)
	}

	public func appendArrayBegin(key: String, child: BSON) -> Bool {
		return bson_append_array_begin(self.doc, key, -1, child.doc)
	}

	public func appendArrayEnd(child: BSON) -> Bool {
		return bson_append_array_end(self.doc, child.doc)
	}

	public func appendArray(key: String, array: BSON) -> Bool {
		return bson_append_array(self.doc, key, -1, array.doc)
	}

	public func concat(src: BSON) -> Bool {
		return bson_concat(self.doc, src.doc)
	}
}

public func ==(lhs: BSON, rhs: BSON) -> Bool {
	let cmp = bson_compare(lhs.doc, rhs.doc)
	return cmp == 0
}

public func <(lhs: BSON, rhs: BSON) -> Bool {
	let cmp = bson_compare(lhs.doc, rhs.doc)
	return cmp < 0
}

extension BSON: Comparable {}

class NoDestroyBSON: BSON {

	override func close() {
		self.doc = nil
	}

}
