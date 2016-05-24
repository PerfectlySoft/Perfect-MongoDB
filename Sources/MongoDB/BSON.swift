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
	var doc: UnsafeMutablePointer<bson_t>?

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
		self.doc = bson_copy(document.doc!)
	}

	init(rawBson: UnsafeMutablePointer<bson_t>?) {
		self.doc = rawBson
	}
    
    deinit {
        close()
    }

    /// close, destroy and release the current BSON document
	public func close() {
		if self.doc != nil {
			bson_destroy(self.doc!)
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
	#if swift(>=3.0)
		guard let data = bson_as_json(self.doc!, &length) else {
			return ""
		}
	#else
		let data = bson_as_json(self.doc!, &length)
		guard nil != data else {
			return ""
		}
	#endif
		defer {
			bson_free(data)
		}
		return String(validatingUTF8: data) ?? ""
	}
    
    /** like asString() but for outermost arrays. */
	public var asArrayString: String {
		var length = 0
	#if swift(>=3.0)
		guard let data = bson_array_as_json(self.doc!, &length) else {
			return ""
		}
	#else
		let data = bson_array_as_json(self.doc!, &length)
		guard nil != data else {
			return ""
		}
	#endif
		defer {
			bson_free(data)
		}
		return String(validatingUTF8: data) ?? ""
	}

    /**
     * asBytes:
     *
     * Returns: A byte array from current BSON document
     */
	public var asBytes: [UInt8] {
		var ret = [UInt8]()
	#if swift(>=3.0)
		guard let doc = self.doc, data = bson_get_data(doc) else {
			return ret
		}
	#else
		guard let doc = self.doc else {
			return ret
		}
		let data = bson_get_data(doc)
	#endif
		let length = Int(doc.pointee.len)
		
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
    public func append(key key: String, document: BSON) -> Bool {
        return bson_append_document(self.doc!, key, -1, document.doc!)
	}

    /**
     * append(key):
     * Parameter key: The key for the field.
     *
     * Appends a new field to self.doc with NULL for the value.
     *
     * Returns: true if successful; false if append would overflow max size.
     */
    public func append(key key: String) -> Bool {
        return bson_append_null(self.doc!, key, -1)
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
	public func append(key key: String, oid: bson_oid_t) -> Bool {
		var cpy = oid
		return bson_append_oid(self.doc!, key, -1, &cpy)
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
    public func append(key key: String, int: Int) -> Bool {
        return bson_append_int64(self.doc!, key, -1, Int64(int))
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
    public func append(key key: String, int32: Int32) -> Bool {
        return bson_append_int32(self.doc!, key, -1, int32)
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
    public func append(key key: String, dateTime: Int64) -> Bool {
        return bson_append_date_time(self.doc!, key, -1, dateTime)
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
    public func append(key key: String, time: time_t) -> Bool {
        return bson_append_time_t(self.doc!, key, -1, time)
	}

    /**
     * append(key, double):
     * Parameter key: The key for the field.
     *
     * Appends a new field to self.doc of the type BSON_TYPE_DOUBLE.
     *
     * Returns: true if successful; false if append would overflow max size.
     */
    public func append(key key: String, double: Double) -> Bool {
        return bson_append_double(self.doc!, key, -1, double)
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
    public func append(key key: String, bool: Bool) -> Bool {
        return bson_append_bool(self.doc!, key, -1, bool)
	}

    /**
     * append(key, string):
     * Parameter key: The key for the field.
     * Parameter string: A UTF-8 encoded string.
     *
     * Appends a new field to self.doc using @key as the key and @string as the UTF-8
     * encoded value.
     *
     * Returns: true if successful; false if append would overflow max size.
     */
    public func append(key key: String, string: String) -> Bool {
        return bson_append_utf8(self.doc!, key, -1, string, -1)
	}
    
    /**
     * append(key, bytes):
     * Parameter key: The key for the field.
     * Parameter bytes: The bytes to append
     * 
     * Appends a bytes buffer to the BSON document.
     *
     * Returns: true if successful; false if append would overflow max size.
     */
	public func append(key key: String, bytes: [UInt8]) -> Bool {
		return bson_append_binary(self.doc!, key, -1, BSON_SUBTYPE_BINARY, bytes, UInt32(bytes.count))
	}

    /**
     * append(key, regex, options):
     * Parameter key: The key of the field.
     * Parameter regex: The regex to append to the bson.
     * Parameter options: Options for @regex.
     *
     * Appends a new field to self.doc of type BSON_TYPE_REGEX. @regex should
     * be the regex string. @options should contain the options for the regex.
     *
     * Valid options for @options are:
     *
     *   'i' for case-insensitive.
     *   'm' for multiple matching.
     *   'x' for verbose mode.
     *   'l' to make \w and \W locale dependent.
     *   's' for dotall mode ('.' matches everything)
     *   'u' to make \w and \W match unicode.
     *
     * For more information on what comprimises a BSON regex, see bsonspec.org.
     *
     * Returns: true if successful; false if append would overflow max size.
     */
	public func append(key key: String, regex: String, options: String) -> Bool {
		return bson_append_regex(self.doc!, key, -1, regex, options)
	}

    /**
     * countKeys():
     *
     * Counts the number of elements found in self.doc.
     */
	public func countKeys() -> Int {
		return Int(bson_count_keys(self.doc!))
	}
    
    /**
     * hasField(key):
     * Parameter key: The key to lookup.
     *
     * Checks to see if self.doc contains a field named @key.
     *
     * This function is case-sensitive.
     *
     * Returns: true if @key exists in self.doc; otherwise false.
     */
	public func hasField(key key: String) -> Bool {
		return bson_has_field(self.doc!, key)
	}

    /**
     * appendArrayBegin(key, child):
     * Parameter key: The key for the field.
     * Parameter child: A location to an uninitialized bson_t.
     *
     * Appends a new field named @key to self.doc, the field is, however,
     * incomplete. @child will be initialized so that you may add fields to the
     * child array. Child will use a memory buffer owned by self.doc and
     * therefore grow the parent buffer as additional space is used. This allows
     * a single malloc'd buffer to be used when building arrays which can help
     * reduce memory fragmentation.
     *
     * The type of @child will be BSON_TYPE_ARRAY and therefore the keys inside
     * of it MUST be "0", "1", etc.
     *
     * Returns: true if successful; false if append would overflow max size.
     */
	public func appendArrayBegin(key key: String, child: BSON) -> Bool {
		return bson_append_array_begin(self.doc!, key, -1, child.doc!)
	}

    /**
     * appendArrayEnd(child):
     * Parameter child: A bson document supplied to appendArrayBegin().
     *
     * Finishes the appending of a array to self.doc. @child is considered
     * disposed after this call and should not be used any further.
     *
     * Returns: true if successful; false if append would overflow max size.
     */
	public func appendArrayEnd(child: BSON) -> Bool {
		return bson_append_array_end(self.doc!, child.doc!)
	}

    /**
     * appendArray(key, array):
     * Parameter key: The key for the field.
     * Parameter array: A bson document containing the array.
     *
     * Appends a BSON array to self.doc. BSON arrays are like documents where the
     * key is the string version of the index. For example, the first item of the
     * array would have the key "0". The second item would have the index "1".
     *
     * Returns: true if successful; false if append would overflow max size.
     */
	public func appendArray(key key: String, array: BSON) -> Bool {
		return bson_append_array(self.doc!, key, -1, array.doc!)
	}

    /**
     * concat(src):
     * Parameter src: BSON doc to be concatenated.
     *
     * Concatenate src with self.doc
     *
     * Returns: true if successful; false if append would overflow max size.
     */
	public func concat(src: BSON) -> Bool {
		return bson_concat(self.doc!, src.doc!)
	}
}

/**
 * ==:
 * compare two BSON documents for equality
 *
 * Returns: BOOL.
 */
public func ==(lhs: BSON, rhs: BSON) -> Bool {
	let cmp = bson_compare(lhs.doc!, rhs.doc!)
	return cmp == 0
}

/**
 * <:
 * compare two BSON documents for sort priority
 *
 * Returns: true if lhs sorts above rhs, false otherwise.
 */
public func <(lhs: BSON, rhs: BSON) -> Bool {
	let cmp = bson_compare(lhs.doc!, rhs.doc!)
	return cmp < 0
}

extension BSON: Comparable {}

class NoDestroyBSON: BSON {

	override func close() {
		self.doc = nil
	}

}
