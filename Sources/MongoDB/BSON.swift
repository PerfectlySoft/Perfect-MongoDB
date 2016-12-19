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
public enum BSONError: Error {
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
			let message = withUnsafePointer(to: &error.message) {
				$0.withMemoryRebound(to: CChar.self, capacity: 0) {
					String(validatingUTF8: $0) ?? "Unknown error while parsing JSON"
				}
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
		guard let doc = self.doc, let data = bson_as_json(doc, &length) else {
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
		guard let doc = self.doc, let data = bson_array_as_json(doc, &length) else {
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
		guard let doc = self.doc, let data = bson_get_data(doc) else {
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
			guard let sDoc = self.doc, let dDoc = document.doc else {
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
        guard let doc = self.doc, let cdoc = child.doc else {
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
        guard let doc = self.doc, let cdoc = child.doc else {
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
        guard let doc = self.doc, let adoc = array.doc else {
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
        guard let doc = self.doc, let sdoc = src.doc else {
            return false
        }
		return bson_concat(doc, sdoc)
	}
	
	/// Represents a BSON OID.
	public struct OID: CustomStringConvertible {
		var oid: bson_oid_t
		public var description: String {
			let up = UnsafeMutablePointer<Int8>.allocate(capacity: 25)
			defer {
				up.deallocate(capacity: 25)
			}
			var oid = self.oid
			bson_oid_to_string(&oid, up)
			return ptr2Str(up, length: 25) ?? ""
		}
		init(oid: bson_oid_t) {
			self.oid = oid
		}
		public init(_ string: String) {
			var oid = bson_oid_t()
			bson_oid_init_from_string(&oid, string)
			self.oid = oid
		}
	}
	
	/// Add the OID with the given key.
	/// Key defaults to "_id"
	@discardableResult
	public func append(key: String = "_id", oid: OID) -> Bool {
		guard let doc = self.doc else {
			return false
		}
		var oid = oid.oid
		bson_append_oid(doc, key, -1, &oid)
		return true
	}
}

/**
 * compare two BSON documents for equality
 *
 * - returns: BOOL.
 */
public func ==(lhs: BSON, rhs: BSON) -> Bool {
    guard let ldoc = lhs.doc, let rdoc = rhs.doc else {
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
    guard let ldoc = lhs.doc, let rdoc = rhs.doc else {
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

private func ptr2Str(_ ptr: UnsafeMutablePointer<Int8>!, length: Int) -> String? {
	var ary = Array(UnsafeBufferPointer(start: ptr, count: Int(length)))
	ary.append(0)
	return String(validatingUTF8: ary)
}

extension BSON {
	
	/// An underlying BSON value type.
	public enum BSONType: UInt32 {
		case
		eod           = 0x00,
		double        = 0x01,
		utf8          = 0x02,
		document      = 0x03,
		array         = 0x04,
		binary        = 0x05,
		undefined     = 0x06,
		oid           = 0x07,
		bool          = 0x08,
		dateTime      = 0x09,
		null          = 0x0A,
		regex         = 0x0B,
		dbpointer     = 0x0C,
		code          = 0x0D,
		symbol        = 0x0E,
		codewscope    = 0x0F,
		int32         = 0x10,
		timestamp     = 0x11,
		int64         = 0x12,
		maxKey        = 0x7F,
		minKey        = 0xFF
	}
	
	/// A BSONValue produced by iterating a document's keys.
	public struct BSONValue {
		private enum Base {
			case double(Double), string(String), bytes([UInt8])
		}
		/// The Mongo type for the value.
		public let type: BSONType
		
		public let double: Double
		public let string: String?
		public let bytes: [UInt8]?
		public let oid: OID?
		public let doc: BSON?
		
		/// The value as an int, if possible.
		public var int: Int? {
			return Int(double)
		}
		
		/// The value as an bool, if possible.
		public var bool: Bool {
			return double != 0.0
		}
		
		// these *bson_value_t are not to be saved or modified
		// the data is saved off into 1 or more base types depending on the value's type
		init?(value: UnsafePointer<bson_value_t>, iter: UnsafePointer<bson_iter_t>) {
			guard let type = BSONType(rawValue: value.pointee.value_type.rawValue) else {
				return nil
			}
			self.type = type
			switch type {
			case .eod,
					.undefined, .null, .dbpointer,
					.maxKey, .minKey:
				return nil
			case .double:
				double = value.pointee.value.v_double
				string = String(double)
				bytes = nil
				oid = nil
				doc = nil
			case .utf8:
				let utf8 = value.pointee.value.v_utf8
				bytes = nil
				string = ptr2Str(utf8.str, length: Int(utf8.len))
				double = Double(string ?? "0.0") ?? 0.0
				oid = nil
				doc = nil
			case .array:
				double = 0.0
				string = nil
				bytes = nil
				oid = nil
				doc = nil
			case .document:
				var data = UnsafePointer<UInt8>(bitPattern: 0)
				var len = 0 as UInt32
				bson_iter_document(iter, &len, &data)
				let bson = bson_new_from_data(data, Int(len))
				self.doc = BSON(rawBson: bson)
				bytes = nil
				string = nil
				double = 0.0
				oid = nil
			case .binary:
				let b = value.pointee.value.v_binary
				guard BSON_SUBTYPE_BINARY.rawValue == b.subtype.rawValue else {
					return nil
				}
				bytes = Array(UnsafeBufferPointer(start: b.data, count: Int(b.data_len)))
				string = nil
				double = 0.0
				oid = nil
				doc = nil
			case .oid:
				let oid = value.pointee.value.v_oid
				self.oid = OID(oid: oid)
				string = self.oid?.description
				double = 0.0
				bytes = []
				doc = nil
			case .bool:
				double = value.pointee.value.v_bool ? 1.0 : 0.0
				string = nil
				bytes = nil
				oid = nil
				doc = nil
			case .dateTime:
				double = Double(value.pointee.value.v_datetime)
				string = nil
				bytes = nil
				oid = nil
				doc = nil
			case .regex:
				let regex = value.pointee.value.v_regex
				
				let rstr = String(validatingUTF8: regex.regex)
				let ostr = String(validatingUTF8: regex.options)
				
				double = 0.0
				string = "/\(rstr ?? "")/\(ostr ?? "")"
				bytes = nil
				oid = nil
				doc = nil
			case .code:
				let code = value.pointee.value.v_code
				bytes = nil
				string = ptr2Str(code.code, length: Int(code.code_len))
				double = 0.0
				oid = nil
				doc = nil
			case .symbol:
				let symbol = value.pointee.value.v_symbol
				bytes = nil
				string = ptr2Str(symbol.symbol, length: Int(symbol.len))
				double = 0.0
				oid = nil
				doc = nil
			case .codewscope:
				double = 0.0
				string = nil
				bytes = nil
				oid = nil
				doc = nil
			case .int32:
				double = Double(value.pointee.value.v_int32)
				string = String(Int32(double))
				bytes = nil
				oid = nil
				doc = nil
			case .timestamp:
				double = 0.0
				string = nil
				bytes = nil
				oid = nil
				doc = nil
			case .int64:
				double = Double(value.pointee.value.v_int64)
				string = String(Int64(double))
				bytes = nil
				oid = nil
				doc = nil
			}
		}
	}
	
	/// An iterator for BSON keys and values.
	public struct Iterator {
		var iter = bson_iter_t()
		/// The type of the current value.
		public var currentType: BSONType? {
			var cpy = iter
			return BSONType(rawValue: bson_iter_type(&cpy).rawValue)
		}
		/// The key for the current value.
		public var currentKey: String? {
			var cpy = iter
			guard let c = bson_iter_key(&cpy) else {
				return nil
			}
			return String(validatingUTF8: c)
		}
		/// If the current value is an narray or document, this returns an iterator
		/// which can be used to walk it.
		public var currentChildIterator: Iterator? {
			guard let currentType = self.currentType else {
				return nil
			}
			switch currentType {
			case .array, .document:
				return Iterator(recursing: self.iter)
			default:
				return nil
			}
		}
		/// The BSON value for the current element.
		public var currentValue: BSONValue? {
			var cpy = iter
			guard let b = bson_iter_value(&cpy) else {
				return nil
			}
			return BSONValue(value: b, iter: &cpy)
		}
		
		private init() {}
		
		init?(bson: BSON) {
			guard bson_iter_init(&self.iter, bson.doc) else {
				return nil
			}
		}
		
		init?(recursing: bson_iter_t) {
			var c1 = recursing
			guard bson_iter_recurse(&c1, &iter) else {
				return nil
			}
		}
		
		/// Advance to the next element.
		/// Note that all iterations must begin by first calling next.
		public mutating func next() -> Bool {
			return bson_iter_next(&iter)
		}
		/// Located the key and advance the iterator to point at it.
		/// If `withCase` is false then the search will be case in-sensitive.
		public mutating func find(key: String, withCase: Bool = true) -> Bool {
			return withCase ? bson_iter_find(&iter, key) : bson_iter_find_case(&iter, key)
		}
		/// Follow standard MongoDB dot notation to recurse into subdocuments.
		/// Returns nil if the descendant is not found.
		public mutating func findDescendant(key: String) -> Iterator? {
			var subit = Iterator()
			guard bson_iter_find_descendant(&iter, key, &subit.iter) else {
				return nil
			}
			return subit
		}
	}
	/// Return a new iterator for this document.
	public func iterator() -> Iterator? {
		return Iterator(bson: self)
	}
}
