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
	case syntaxError(String)
}

/// BSON class 
public class BSON: CustomStringConvertible {
	var doc: UnsafeMutablePointer<bson_t>?

    /// Return JSON representation of current BSON contents as a String
	public var description: String {
		return self.asString
	}

    /**
    *   Allocates a new doc structure. Call the various append() functions to add fields to the bson. You can iterate the doc at any time using a bson_iter_t and bson_iter_init().
    */
	public init() {
		self.doc = bson_new()
	}
    
    /** Creates a new doc structure using the data provided. bytes should contain bytes that can be copied into the new doc structure.
     *
     *- parameter bytes: A byte array containing a serialized bson document.
    */
	public init(bytes: [UInt8]) {
		self.doc = bson_new_from_data(bytes, bytes.count)
	}

    /**
     * Creates a new doc structure using the data provided. json should contain bytes that can be copied into the new doc structure.
     *
     * - parameter json: A string containing a json data.
    */
	public init(json: String) throws {
		var error = bson_error_t()
        guard let doc = bson_new_from_json(json, json.utf8.count, &error) else {
            let message = withUnsafePointer(&error.message) {
                String(validatingUTF8: UnsafePointer($0)) ?? "Unknown error while parsing JSON"
            }
            throw BSONError.syntaxError(message)
        }
		self.doc = doc
	}
    
    /**
     * Creates a new doc by copying the provided bson doc.
     *
     * - parameter document: An existing bson document.
     *
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
        guard let doc = self.doc else {
            return
        }
		bson_destroy(doc)
        self.doc = nil
	}

    /**
     * Creates a new string containing current document in extended JSON format.
     *
     * See http://docs.mongodb.org/manual/reference/mongodb-extended-json/ for
     * more information on extended JSON.
     *
     * - returns: String
     */
	public var asString: String {
		var length = 0
		guard let doc = self.doc, data = bson_as_json(doc, &length) else {
			return ""
		}
		defer {
			bson_free(data)
		}
		return String(validatingUTF8: data) ?? ""
	}
    
    /** like asString() but for outermost arrays. */
	public var asArrayString: String {
		var length = 0
		guard let doc = self.doc, data = bson_array_as_json(doc, &length) else {
			return ""
		}
		defer {
			bson_free(data)
		}
		return String(validatingUTF8: data) ?? ""
	}

    /**
     * asBytes:
     *
     * - returns: A byte array from current BSON document
     */
	public var asBytes: [UInt8] {
		var ret = [UInt8]()
		guard let doc = self.doc, data = bson_get_data(doc) else {
			return ret
		}
		let length = Int(doc.pointee.len)
		
		for i in 0..<length {
			ret.append(data[i])
		}
		return ret
	}

    /**
     * Appends a new field to self.doc of the type BSON_TYPE_DOCUMENT. The documents contents will be copied into self.doc.
     *
     * - parameter key: The key for the field.
     * - parameter document: Existing BSON document.
     * - returns: true if successful; false if append would overflow max size.
     *
     */
	@discardableResult
		public func append(key k: String, document: BSON) -> Bool {
			guard let sDoc = self.doc, dDoc = document.doc else {
				return false
			}
			return bson_append_document(sDoc, k, -1, dDoc)
		}

    /**
     * Appends a new field to self.doc with NULL for the value.
     *
     * - parameter key: The key for the field.
     *
     * - returns: true if successful; false if append would overflow max size.
     */
	@discardableResult
		public func append(key k: String) -> Bool {
			guard let doc = self.doc else {
				return false
			}
			return bson_append_null(doc, k, -1)
		}

    /**
     * Appends a new field to the self.doc of type BSON_TYPE_OID using the contents of
     *  oid.
     *
     * - parameter key: The key for the field.
     * - parameter oid: bson_oid_t.
     *
     * - returns: true if successful; false if append would overflow max size.
     */
	@discardableResult
		public func append(key k: String, oid: bson_oid_t) -> Bool {
			guard let doc = self.doc else {
				return false
			}
			var cpy = oid
			return bson_append_oid(doc, k, -1, &cpy)
		}

    /**
     * Appends a new field of type BSON_TYPE_INT64 to self.doc .
     *
     * - parameter key: The key for the field.
     * - parameter int: The Int 64-bit integer value.
     *
     *
     * - returns: true if successful; false if append would overflow max size.
     */
	@discardableResult
		public func append(key k: String, int: Int) -> Bool {
			guard let doc = self.doc else {
				return false
			}
			return bson_append_int64(doc, k, -1, Int64(int))
		}

    /**
     * Appends a new field of type BSON_TYPE_INT32 to self.doc .
     *
     * - parameter key: The key for the field.
     * - parameter int32: The Int32 32-bit integer value.
     *
     *
     * - returns: true if successful; false if append would overflow max size.
     */
	@discardableResult
		public func append(key k: String, int32: Int32) -> Bool {
			guard let doc = self.doc else {
				return false
			}
			return bson_append_int32(doc, k, -1, int32)
		}

    /**
     * Appends a new field to self.doc of type BSON_TYPE_DATE_TIME.
     *
     * - parameter key: The key for the field.
     * - parameter dateTime: The number of milliseconds elapsed since UNIX epoch.
     *
     *
     * - returns: true if sucessful; otherwise false.
     */
	@discardableResult
		public func append(key k: String, dateTime: Int64) -> Bool {
			guard let doc = self.doc else {
				return false
			}
			return bson_append_date_time(doc, k, -1, dateTime)
		}

    /**
     * Appends a BSON_TYPE_DATE_TIME field to self.doc using the time_t @value for the
     * number of seconds since UNIX epoch in UTC.
     *
     * - parameter key: The key for the field.
     * - parameter time: A time_t.
     *
     *
     * - returns: true if successful; false if append would overflow max size.
     */
	@discardableResult
		public func append(key k: String, time: time_t) -> Bool {
			guard let doc = self.doc else {
				return false
			}
			return bson_append_time_t(doc, k, -1, time)
		}

    /**
     * Appends a new field to self.doc of the type BSON_TYPE_DOUBLE.
     *
     * - parameter key: The key for the field.
     * - parameter double: The double to be appended
     *
     * - returns: true if successful; false if append would overflow max size.
     */
	@discardableResult
		public func append(key k: String, double: Double) -> Bool {
			guard let doc = self.doc else {
				return false
			}
			return bson_append_double(doc, k, -1, double)
		}

    /**
     * Appends a new field to self.doc of type BSON_TYPE_BOOL.
     *
     * - parameter key: The key for the field.
     * - parameter bool: The boolean value.
     *
     * - returns: true if successful; false if append would overflow max size.
     */
	@discardableResult
		public func append(key k: String, bool: Bool) -> Bool {
			guard let doc = self.doc else {
				return false
			}
			return bson_append_bool(doc, k, -1, bool)
		}

    /**
     * Appends a new field to self.doc using @key as the key and @string as the UTF-8
     * encoded value.
     *
     * - parameter key: The key for the field.
     * - parameter string: A UTF-8 encoded string.
     *
     * - returns: true if successful; false if append would overflow max size.
     */
	@discardableResult
		public func append(key k: String, string: String) -> Bool {
			guard let doc = self.doc else {
				return false
			}
			return bson_append_utf8(doc, k, -1, string, -1)
		}
    
    /**
     * Appends a bytes buffer to the BSON document.
     *
     * - parameter key: The key for the field.
     * - parameter bytes: The bytes to append
     *
     * - returns: true if successful; false if append would overflow max size.
     */
	@discardableResult
		public func append(key k: String, bytes: [UInt8]) -> Bool {
			guard let doc = self.doc else {
				return false
			}
			return bson_append_binary(doc, k, -1, BSON_SUBTYPE_BINARY, bytes, UInt32(bytes.count))
		}

    /**
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
     * - parameter key: The key of the field.
     * - parameter regex: The regex to append to the bson.
     * - parameter options: Options for @regex.
     *
     * - returns: true if successful; false if append would overflow max size.
     */
	@discardableResult
		public func append(key k: String, regex: String, options: String) -> Bool {
			guard let doc = self.doc else {
				return false
			}
			return bson_append_regex(doc, k, -1, regex, options)
		}

    /**
     * Counts the number of elements found in self.doc.
     * - returns: Int value of keys count
     */
    public func countKeys() -> Int {
        guard let doc = self.doc else {
            return 0
        }
		return Int(bson_count_keys(doc))
	}
    
    /**
     * Checks to see if self.doc contains a field named @key.
     *
     * This function is case-sensitive.
     *
     * - parameter key: The key to lookup.
     *
     * - returns: true if @key exists in self.doc; otherwise false.
     */
    public func hasField(key k: String) -> Bool {
        guard let doc = self.doc else {
            return false
        }
		return bson_has_field(doc, k)
	}

    /**
     * Appends a new field named key to self.doc, the field is, however,
     * incomplete. @child will be initialized so that you may add fields to the
     * child array. Child will use a memory buffer owned by self.doc and
     * therefore grow the parent buffer as additional space is used. This allows
     * a single malloc'd buffer to be used when building arrays which can help
     * reduce memory fragmentation.
     *
     * The type of @child will be BSON_TYPE_ARRAY and therefore the keys inside
     * of it MUST be "0", "1", etc.
     *
     * - parameter key: The key for the field.
     * - parameter child: A location to an uninitialized bson_t.
     *
     * - returns: true if successful; false if append would overflow max size.
     */
    public func appendArrayBegin(key k: String, child: BSON) -> Bool {
        guard let doc = self.doc, cdoc = child.doc else {
            return false
        }
		return bson_append_array_begin(doc, k, -1, cdoc)
	}

    /**
     * Finishes the appending of an array to self.doc. child is considered
     * disposed after this call and should not be used any further.
     *
     * - parameter child: A bson document supplied to appendArrayBegin().
     *
     * - returns: true if successful; false if append would overflow max size.
     */
    public func appendArrayEnd(child: BSON) -> Bool {
        guard let doc = self.doc, cdoc = child.doc else {
            return false
        }
		return bson_append_array_end(doc, cdoc)
	}

    /**
     * Appends a BSON array to self.doc. BSON arrays are like documents where the
     * key is the string version of the index. For example, the first item of the
     * array would have the key "0". The second item would have the index "1".
     *
     * - parameter key: The key for the field.
     * - parameter array: A bson document containing the array.
     *
     * - returns: true if successful; false if append would overflow max size.
     */
    public func appendArray(key k: String, array: BSON) -> Bool {
        guard let doc = self.doc, adoc = array.doc else {
            return false
        }
		return bson_append_array(doc, k, -1, adoc)
	}

    /**
     * Concatenate src with self.doc
     *
     * - parameter src: BSON doc to be concatenated.
     *
     * - returns: true if successful; false if append would overflow max size.
     */
    public func concat(src: BSON) -> Bool {
        guard let doc = self.doc, sdoc = src.doc else {
            return false
        }
		return bson_concat(doc, sdoc)
	}
}

/**
 * compare two BSON documents for equality
 *
 * - returns: BOOL.
 */
public func ==(lhs: BSON, rhs: BSON) -> Bool {
    guard let ldoc = lhs.doc, rdoc = rhs.doc else {
        return false
    }
	let cmp = bson_compare(ldoc, rdoc)
	return cmp == 0
}

/**
 * compare two BSON documents for sort priority
 *
 * - returns: true if lhs sorts above rhs, false otherwise.
 */
public func <(lhs: BSON, rhs: BSON) -> Bool {
    guard let ldoc = lhs.doc, rdoc = rhs.doc else {
        return false
    }
    let cmp = bson_compare(ldoc, rdoc)
	return cmp < 0
}

extension BSON: Comparable {}

class NoDestroyBSON: BSON {
	override func close() {
		self.doc = nil
	}
}
