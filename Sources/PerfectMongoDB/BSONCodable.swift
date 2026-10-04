//
//  BSONCodable.swift
//  PerfectMongoDB
//
//  Codable support: BSONEncoder turns an Encodable value into a BSON document and
//  BSONDecoder turns a document back into a Decodable value, reading and writing
//  libbson directly so BSON types survive the round trip:
//
//    Date       <-> datetime (millisecond precision)
//    Data       <-> binary (subtype 0)
//    UUID       <-> binary (subtype 4)
//    BSON.OID   <-> ObjectId
//    Int, Int64, UInt32 -> int64; smaller integers -> int32; Float, Double -> double
//
//  Decoding accepts any BSON number for any Swift numeric type, as long as the value
//  converts exactly. Both sides go through an intermediate tree (`BSONNode`), which
//  keeps nested containers and superEncoder/superDecoder simple.
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

import Foundation
import PerfectCBSON

// MARK: - Intermediate tree

enum BSONPrimitive {
	case null
	case bool(Bool)
	case int32(Int32)
	case int64(Int64)
	case double(Double)
	case string(String)
	case dateTime(Int64)
	case binary(Data, bson_subtype_t)
	case oid(bson_oid_t)
	/// A BSON type with no Codable mapping (decimal128, regex, timestamp, ...), by type name.
	case unsupported(String)
}

final class BSONDocumentStorage {
	private(set) var keys: [String] = []
	private var boxes: [String: BSONNodeBox] = [:]

	/// Returns the box for **key**, adding it if new. Re-encoding a key replaces its value in place.
	func box(forKey key: String) -> BSONNodeBox {
		if let existing = boxes[key] {
			return existing
		}
		let box = BSONNodeBox()
		keys.append(key)
		boxes[key] = box
		return box
	}

	var entries: [(String, BSONNodeBox)] {
		keys.map { ($0, boxes[$0]!) }
	}
}

final class BSONArrayStorage {
	private(set) var boxes: [BSONNodeBox] = []

	func append() -> BSONNodeBox {
		let box = BSONNodeBox()
		boxes.append(box)
		return box
	}
}

enum BSONEncodedNode {
	case primitive(BSONPrimitive)
	case document(BSONDocumentStorage)
	case array(BSONArrayStorage)
}

final class BSONNodeBox {
	var node: BSONEncodedNode?
}

/// A decoded document, array or value.
indirect enum BSONNode {
	case primitive(BSONPrimitive)
	case document([(String, BSONNode)])
	case array([BSONNode])
}

// MARK: - Errors

/// An error reported by MongoDB or libmongoc from the throwing (Codable and async) API.
public struct MongoError: Error, CustomStringConvertible, Sendable {
	public let domain: UInt32
	public let code: UInt32
	public let message: String

	public init(domain: UInt32, code: UInt32, message: String) {
		self.domain = domain
		self.code = code
		self.message = message
	}

	init(_ error: bson_error_t) {
		var error = error
		let message = withUnsafePointer(to: &error.message) {
			$0.withMemoryRebound(to: CChar.self, capacity: 1) {
				String(validatingCString: $0) ?? "unknown error"
			}
		}
		self.init(domain: error.domain, code: error.code, message: message)
	}

	init(_ message: String) {
		self.init(domain: 1, code: 1, message: message)
	}

	public var description: String {
		"MongoError(\(domain), \(code)): \(message)"
	}
}

extension MongoResult {
	/// Throws the error case; returns the result otherwise.
	func get() throws -> MongoResult {
		if case .error(let domain, let code, let message) = self {
			throw MongoError(domain: domain, code: code, message: message)
		}
		return self
	}
}

// MARK: - ObjectId

extension BSON.OID: Hashable, Codable {
	public static func == (lhs: BSON.OID, rhs: BSON.OID) -> Bool {
		var l = lhs.oid, r = rhs.oid
		return bson_oid_equal(&l, &r)
	}

	public func hash(into hasher: inout Hasher) {
		hasher.combine(description)
	}

	/// An ObjectId from its 24-character hex string, or nil if **string** isn't one.
	public init?(validating string: String) {
		guard bson_oid_is_valid(string, string.utf8.count) else {
			return nil
		}
		self.init(string)
	}

	/// Outside BSONEncoder/BSONDecoder (for example with JSONEncoder), an ObjectId is its hex string.
	public init(from decoder: Decoder) throws {
		let container = try decoder.singleValueContainer()
		let string = try container.decode(String.self)
		guard let oid = BSON.OID(validating: string) else {
			throw DecodingError.dataCorruptedError(in: container, debugDescription: "'\(string)' is not an ObjectId")
		}
		self = oid
	}

	public func encode(to encoder: Encoder) throws {
		var container = encoder.singleValueContainer()
		try container.encode(description)
	}
}

// MARK: - Encoder

/// Encodes Encodable values as BSON documents.
public struct BSONEncoder: Sendable {
	/// Contextual information for custom `encode(to:)` implementations.
	public var userInfo: [CodingUserInfoKey: any Sendable] = [:]

	public init() {}

	/// Encodes **value**, which must encode as a keyed container (a struct, class or dictionary).
	public func encode<T: Encodable>(_ value: T) throws -> BSON {
		let box = BSONNodeBox()
		try _BSONEncoder.encodeValue(value, into: box, codingPath: [], userInfo: userInfo)
		guard case .document(let storage)? = box.node else {
			throw EncodingError.invalidValue(value, .init(codingPath: [], debugDescription: "Top-level \(T.self) did not encode as a document."))
		}
		guard let doc = bson_new() else {
			throw MongoError("bson_new failed")
		}
		let bson = BSON(rawBson: fromOpaque(doc))
		for (key, child) in storage.entries {
			try Self.write(child.node, key: key, into: doc)
		}
		return bson
	}

	private static func write(_ node: BSONEncodedNode?, key: String, into doc: UnsafeMutablePointer<bson_t>) throws {
		let ok: Bool
		switch node {
		case .none:
			ok = bson_append_null(doc, key, -1)
		case .primitive(let primitive)?:
			ok = write(primitive, key: key, into: doc)
		case .document(let storage)?:
			let child = UnsafeMutablePointer<bson_t>.allocate(capacity: 1)
			defer { child.deallocate() }
			guard bson_append_document_begin(doc, key, -1, child) else {
				throw MongoError("could not start document '\(key)'")
			}
			for (childKey, box) in storage.entries {
				try write(box.node, key: childKey, into: child)
			}
			ok = bson_append_document_end(doc, child)
		case .array(let storage)?:
			let child = UnsafeMutablePointer<bson_t>.allocate(capacity: 1)
			defer { child.deallocate() }
			guard _perfect_bson_append_array_begin(doc, key, -1, child) else {
				throw MongoError("could not start array '\(key)'")
			}
			for (index, box) in storage.boxes.enumerated() {
				try write(box.node, key: String(index), into: child)
			}
			ok = bson_append_array_end(doc, child)
		}
		guard ok else {
			throw MongoError("could not append '\(key)': document exceeds the maximum BSON size")
		}
	}

	private static func write(_ primitive: BSONPrimitive, key: String, into doc: UnsafeMutablePointer<bson_t>) -> Bool {
		switch primitive {
		case .null:
			return bson_append_null(doc, key, -1)
		case .bool(let value):
			return bson_append_bool(doc, key, -1, value)
		case .int32(let value):
			return bson_append_int32(doc, key, -1, value)
		case .int64(let value):
			return bson_append_int64(doc, key, -1, value)
		case .double(let value):
			return bson_append_double(doc, key, -1, value)
		case .string(let value):
			return bson_append_utf8(doc, key, -1, value, Int32(value.utf8.count))
		case .dateTime(let millis):
			return bson_append_date_time(doc, key, -1, millis)
		case .binary(let data, let subtype):
			return data.withUnsafeBytes {
				bson_append_binary(doc, key, -1, subtype, $0.bindMemory(to: UInt8.self).baseAddress, UInt32(data.count))
			}
		case .oid(var oid):
			return bson_append_oid(doc, key, -1, &oid)
		case .unsupported:
			return false
		}
	}
}

final class _BSONEncoder: Encoder {
	let box: BSONNodeBox
	let codingPath: [CodingKey]
	let sendableUserInfo: [CodingUserInfoKey: any Sendable]
	var userInfo: [CodingUserInfoKey: Any] { sendableUserInfo }

	init(box: BSONNodeBox, codingPath: [CodingKey], userInfo: [CodingUserInfoKey: any Sendable]) {
		self.box = box
		self.codingPath = codingPath
		self.sendableUserInfo = userInfo
	}

	/// The BSON value for types with a native BSON representation, or nil to use their Encodable conformance.
	static func specialPrimitive(_ value: Any) -> BSONPrimitive? {
		switch value {
		case let date as Date:
			return .dateTime(Int64((date.timeIntervalSince1970 * 1000).rounded(.down)))
		case let data as Data:
			return .binary(data, BSON_SUBTYPE_BINARY)
		case let uuid as UUID:
			let data = withUnsafeBytes(of: uuid.uuid) { Data($0) }
			return .binary(data, BSON_SUBTYPE_UUID)
		case let oid as BSON.OID:
			return .oid(oid.oid)
		default:
			return nil
		}
	}

	static func encodeValue<T: Encodable>(_ value: T, into box: BSONNodeBox, codingPath: [CodingKey], userInfo: [CodingUserInfoKey: any Sendable]) throws {
		if let primitive = specialPrimitive(value) {
			box.node = .primitive(primitive)
			return
		}
		try value.encode(to: _BSONEncoder(box: box, codingPath: codingPath, userInfo: userInfo))
		if box.node == nil {
			// A type that encoded nothing at all, e.g. an empty struct.
			box.node = .document(BSONDocumentStorage())
		}
	}

	func documentStorage() -> BSONDocumentStorage {
		if case .document(let storage)? = box.node {
			return storage
		}
		let storage = BSONDocumentStorage()
		box.node = .document(storage)
		return storage
	}

	func arrayStorage() -> BSONArrayStorage {
		if case .array(let storage)? = box.node {
			return storage
		}
		let storage = BSONArrayStorage()
		box.node = .array(storage)
		return storage
	}

	func container<Key: CodingKey>(keyedBy type: Key.Type) -> KeyedEncodingContainer<Key> {
		KeyedEncodingContainer(BSONKeyedEncodingContainer<Key>(encoder: self, storage: documentStorage(), codingPath: codingPath))
	}

	func unkeyedContainer() -> UnkeyedEncodingContainer {
		BSONUnkeyedEncodingContainer(encoder: self, storage: arrayStorage(), codingPath: codingPath)
	}

	func singleValueContainer() -> SingleValueEncodingContainer {
		BSONSingleValueEncodingContainer(encoder: self)
	}
}

/// Integer and floating-point encodings shared by the three container types.
private enum BSONNumber {
	static func integer<T: FixedWidthInteger>(_ value: T, codingPath: [CodingKey]) throws -> BSONPrimitive {
		if T.bitWidth < 32 || T.self == Int32.self {
			return .int32(Int32(value))
		}
		guard let value = Int64(exactly: value) else {
			throw EncodingError.invalidValue(value, .init(codingPath: codingPath, debugDescription: "\(value) does not fit in a BSON int64."))
		}
		return .int64(value)
	}
}

struct BSONKeyedEncodingContainer<Key: CodingKey>: KeyedEncodingContainerProtocol {
	let encoder: _BSONEncoder
	let storage: BSONDocumentStorage
	let codingPath: [CodingKey]

	private func set(_ primitive: BSONPrimitive, _ key: Key) {
		storage.box(forKey: key.stringValue).node = .primitive(primitive)
	}

	mutating func encodeNil(forKey key: Key) throws { set(.null, key) }
	mutating func encode(_ value: Bool, forKey key: Key) throws { set(.bool(value), key) }
	mutating func encode(_ value: String, forKey key: Key) throws { set(.string(value), key) }
	mutating func encode(_ value: Double, forKey key: Key) throws { set(.double(value), key) }
	mutating func encode(_ value: Float, forKey key: Key) throws { set(.double(Double(value)), key) }
	mutating func encode(_ value: Int, forKey key: Key) throws { set(try BSONNumber.integer(value, codingPath: codingPath + [key]), key) }
	mutating func encode(_ value: Int8, forKey key: Key) throws { set(try BSONNumber.integer(value, codingPath: codingPath + [key]), key) }
	mutating func encode(_ value: Int16, forKey key: Key) throws { set(try BSONNumber.integer(value, codingPath: codingPath + [key]), key) }
	mutating func encode(_ value: Int32, forKey key: Key) throws { set(try BSONNumber.integer(value, codingPath: codingPath + [key]), key) }
	mutating func encode(_ value: Int64, forKey key: Key) throws { set(try BSONNumber.integer(value, codingPath: codingPath + [key]), key) }
	mutating func encode(_ value: UInt, forKey key: Key) throws { set(try BSONNumber.integer(value, codingPath: codingPath + [key]), key) }
	mutating func encode(_ value: UInt8, forKey key: Key) throws { set(try BSONNumber.integer(value, codingPath: codingPath + [key]), key) }
	mutating func encode(_ value: UInt16, forKey key: Key) throws { set(try BSONNumber.integer(value, codingPath: codingPath + [key]), key) }
	mutating func encode(_ value: UInt32, forKey key: Key) throws { set(try BSONNumber.integer(value, codingPath: codingPath + [key]), key) }
	mutating func encode(_ value: UInt64, forKey key: Key) throws { set(try BSONNumber.integer(value, codingPath: codingPath + [key]), key) }

	mutating func encode<T: Encodable>(_ value: T, forKey key: Key) throws {
		try _BSONEncoder.encodeValue(value, into: storage.box(forKey: key.stringValue), codingPath: codingPath + [key], userInfo: encoder.sendableUserInfo)
	}

	mutating func nestedContainer<NestedKey: CodingKey>(keyedBy keyType: NestedKey.Type, forKey key: Key) -> KeyedEncodingContainer<NestedKey> {
		superEncoder(forKey: key).container(keyedBy: keyType)
	}

	mutating func nestedUnkeyedContainer(forKey key: Key) -> UnkeyedEncodingContainer {
		superEncoder(forKey: key).unkeyedContainer()
	}

	mutating func superEncoder() -> Encoder {
		_BSONEncoder(box: storage.box(forKey: "super"), codingPath: codingPath + [BSONKey(stringValue: "super")], userInfo: encoder.sendableUserInfo)
	}

	mutating func superEncoder(forKey key: Key) -> Encoder {
		_BSONEncoder(box: storage.box(forKey: key.stringValue), codingPath: codingPath + [key], userInfo: encoder.sendableUserInfo)
	}
}

struct BSONUnkeyedEncodingContainer: UnkeyedEncodingContainer {
	let encoder: _BSONEncoder
	let storage: BSONArrayStorage
	let codingPath: [CodingKey]
	var count: Int { storage.boxes.count }

	private var nextKey: CodingKey { BSONKey(intValue: count) }

	private func append(_ primitive: BSONPrimitive) {
		storage.append().node = .primitive(primitive)
	}

	mutating func encodeNil() throws { append(.null) }
	mutating func encode(_ value: Bool) throws { append(.bool(value)) }
	mutating func encode(_ value: String) throws { append(.string(value)) }
	mutating func encode(_ value: Double) throws { append(.double(value)) }
	mutating func encode(_ value: Float) throws { append(.double(Double(value))) }
	mutating func encode(_ value: Int) throws { append(try BSONNumber.integer(value, codingPath: codingPath + [nextKey])) }
	mutating func encode(_ value: Int8) throws { append(try BSONNumber.integer(value, codingPath: codingPath + [nextKey])) }
	mutating func encode(_ value: Int16) throws { append(try BSONNumber.integer(value, codingPath: codingPath + [nextKey])) }
	mutating func encode(_ value: Int32) throws { append(try BSONNumber.integer(value, codingPath: codingPath + [nextKey])) }
	mutating func encode(_ value: Int64) throws { append(try BSONNumber.integer(value, codingPath: codingPath + [nextKey])) }
	mutating func encode(_ value: UInt) throws { append(try BSONNumber.integer(value, codingPath: codingPath + [nextKey])) }
	mutating func encode(_ value: UInt8) throws { append(try BSONNumber.integer(value, codingPath: codingPath + [nextKey])) }
	mutating func encode(_ value: UInt16) throws { append(try BSONNumber.integer(value, codingPath: codingPath + [nextKey])) }
	mutating func encode(_ value: UInt32) throws { append(try BSONNumber.integer(value, codingPath: codingPath + [nextKey])) }
	mutating func encode(_ value: UInt64) throws { append(try BSONNumber.integer(value, codingPath: codingPath + [nextKey])) }

	mutating func encode<T: Encodable>(_ value: T) throws {
		let key = nextKey
		try _BSONEncoder.encodeValue(value, into: storage.append(), codingPath: codingPath + [key], userInfo: encoder.sendableUserInfo)
	}

	mutating func nestedContainer<NestedKey: CodingKey>(keyedBy keyType: NestedKey.Type) -> KeyedEncodingContainer<NestedKey> {
		superEncoder().container(keyedBy: keyType)
	}

	mutating func nestedUnkeyedContainer() -> UnkeyedEncodingContainer {
		superEncoder().unkeyedContainer()
	}

	mutating func superEncoder() -> Encoder {
		let key = nextKey
		return _BSONEncoder(box: storage.append(), codingPath: codingPath + [key], userInfo: encoder.sendableUserInfo)
	}
}

struct BSONSingleValueEncodingContainer: SingleValueEncodingContainer {
	let encoder: _BSONEncoder
	var codingPath: [CodingKey] { encoder.codingPath }

	private func set(_ primitive: BSONPrimitive) {
		encoder.box.node = .primitive(primitive)
	}

	mutating func encodeNil() throws { set(.null) }
	mutating func encode(_ value: Bool) throws { set(.bool(value)) }
	mutating func encode(_ value: String) throws { set(.string(value)) }
	mutating func encode(_ value: Double) throws { set(.double(value)) }
	mutating func encode(_ value: Float) throws { set(.double(Double(value))) }
	mutating func encode(_ value: Int) throws { set(try BSONNumber.integer(value, codingPath: codingPath)) }
	mutating func encode(_ value: Int8) throws { set(try BSONNumber.integer(value, codingPath: codingPath)) }
	mutating func encode(_ value: Int16) throws { set(try BSONNumber.integer(value, codingPath: codingPath)) }
	mutating func encode(_ value: Int32) throws { set(try BSONNumber.integer(value, codingPath: codingPath)) }
	mutating func encode(_ value: Int64) throws { set(try BSONNumber.integer(value, codingPath: codingPath)) }
	mutating func encode(_ value: UInt) throws { set(try BSONNumber.integer(value, codingPath: codingPath)) }
	mutating func encode(_ value: UInt8) throws { set(try BSONNumber.integer(value, codingPath: codingPath)) }
	mutating func encode(_ value: UInt16) throws { set(try BSONNumber.integer(value, codingPath: codingPath)) }
	mutating func encode(_ value: UInt32) throws { set(try BSONNumber.integer(value, codingPath: codingPath)) }
	mutating func encode(_ value: UInt64) throws { set(try BSONNumber.integer(value, codingPath: codingPath)) }

	mutating func encode<T: Encodable>(_ value: T) throws {
		try _BSONEncoder.encodeValue(value, into: encoder.box, codingPath: codingPath, userInfo: encoder.sendableUserInfo)
	}
}

struct BSONKey: CodingKey {
	let stringValue: String
	let intValue: Int?

	init(stringValue: String) {
		self.stringValue = stringValue
		self.intValue = nil
	}

	init(intValue: Int) {
		self.stringValue = String(intValue)
		self.intValue = intValue
	}
}

// MARK: - Decoder

/// Decodes Decodable values from BSON documents.
public struct BSONDecoder: Sendable {
	/// Contextual information for custom `init(from:)` implementations.
	public var userInfo: [CodingUserInfoKey: any Sendable] = [:]

	public init() {}

	public func decode<T: Decodable>(_ type: T.Type, from document: BSON) throws -> T {
		guard let doc = document.doc else {
			throw DecodingError.dataCorrupted(.init(codingPath: [], debugDescription: "The BSON document is closed."))
		}
		return try decode(type, from: UnsafePointer(doc))
	}

	func decode<T: Decodable>(_ type: T.Type, from doc: UnsafePointer<bson_t>) throws -> T {
		var iter = bson_iter_t()
		guard bson_iter_init(&iter, doc) else {
			throw DecodingError.dataCorrupted(.init(codingPath: [], debugDescription: "Invalid BSON document."))
		}
		let root = BSONNode.document(try Self.readDocument(&iter))
		return try _BSONDecoder.decodeValue(type, from: root, codingPath: [], userInfo: userInfo)
	}

	private static func readDocument(_ iter: inout bson_iter_t) throws -> [(String, BSONNode)] {
		var entries: [(String, BSONNode)] = []
		while bson_iter_next(&iter) {
			let key = String(cString: bson_iter_key(&iter))
			entries.append((key, try readValue(&iter)))
		}
		return entries
	}

	private static func readValue(_ iter: inout bson_iter_t) throws -> BSONNode {
		let type = bson_iter_type(&iter)
		switch type {
		case BSON_TYPE_DOCUMENT, BSON_TYPE_ARRAY:
			var child = bson_iter_t()
			guard bson_iter_recurse(&iter, &child) else {
				throw DecodingError.dataCorrupted(.init(codingPath: [], debugDescription: "Invalid nested BSON."))
			}
			let entries = try readDocument(&child)
			return type == BSON_TYPE_ARRAY ? .array(entries.map { $0.1 }) : .document(entries)
		case BSON_TYPE_NULL, BSON_TYPE_UNDEFINED:
			return .primitive(.null)
		case BSON_TYPE_BOOL:
			return .primitive(.bool(bson_iter_bool(&iter)))
		case BSON_TYPE_INT32:
			return .primitive(.int32(bson_iter_int32(&iter)))
		case BSON_TYPE_INT64:
			return .primitive(.int64(bson_iter_int64(&iter)))
		case BSON_TYPE_DOUBLE:
			return .primitive(.double(bson_iter_double(&iter)))
		case BSON_TYPE_UTF8:
			var length: UInt32 = 0
			let pointer = bson_iter_utf8(&iter, &length)
			let bytes = UnsafeRawBufferPointer(start: pointer, count: Int(length))
			return .primitive(.string(String(decoding: bytes, as: UTF8.self)))
		case BSON_TYPE_DATE_TIME:
			return .primitive(.dateTime(bson_iter_date_time(&iter)))
		case BSON_TYPE_OID:
			return .primitive(.oid(bson_iter_oid(&iter).pointee))
		case BSON_TYPE_BINARY:
			var subtype = BSON_SUBTYPE_BINARY
			var length: UInt32 = 0
			var pointer: UnsafePointer<UInt8>? = nil
			bson_iter_binary(&iter, &subtype, &length, &pointer)
			let data = pointer.map { Data(bytes: $0, count: Int(length)) } ?? Data()
			return .primitive(.binary(data, subtype))
		default:
			let name = typeName(type)
			return .primitive(.unsupported(name))
		}
	}

	private static func typeName(_ type: bson_type_t) -> String {
		switch type {
		case BSON_TYPE_DECIMAL128: return "decimal128"
		case BSON_TYPE_TIMESTAMP: return "timestamp"
		case BSON_TYPE_REGEX: return "regex"
		case BSON_TYPE_CODE, BSON_TYPE_CODEWSCOPE: return "JavaScript code"
		case BSON_TYPE_SYMBOL: return "symbol"
		case BSON_TYPE_DBPOINTER: return "DBPointer"
		case BSON_TYPE_MINKEY: return "minKey"
		case BSON_TYPE_MAXKEY: return "maxKey"
		default: return "BSON type 0x\(String(type.rawValue, radix: 16))"
		}
	}
}

final class _BSONDecoder: Decoder {
	let node: BSONNode
	let codingPath: [CodingKey]
	let sendableUserInfo: [CodingUserInfoKey: any Sendable]
	var userInfo: [CodingUserInfoKey: Any] { sendableUserInfo }

	init(node: BSONNode, codingPath: [CodingKey], userInfo: [CodingUserInfoKey: any Sendable]) {
		self.node = node
		self.codingPath = codingPath
		self.sendableUserInfo = userInfo
	}

	static func decodeValue<T: Decodable>(_ type: T.Type, from node: BSONNode, codingPath: [CodingKey], userInfo: [CodingUserInfoKey: any Sendable]) throws -> T {
		if let special = try specialValue(type, from: node, codingPath: codingPath) {
			return special
		}
		return try T(from: _BSONDecoder(node: node, codingPath: codingPath, userInfo: userInfo))
	}

	/// Decodes types with a native BSON representation; nil means use their Decodable conformance.
	private static func specialValue<T>(_ type: T.Type, from node: BSONNode, codingPath: [CodingKey]) throws -> T? {
		func mismatch(_ expected: String) -> DecodingError {
			.typeMismatch(T.self, .init(codingPath: codingPath, debugDescription: "Expected \(expected), found \(node.typeDescription)."))
		}
		if T.self == Date.self {
			guard case .primitive(.dateTime(let millis)) = node else { throw mismatch("a BSON datetime") }
			return (Date(timeIntervalSince1970: Double(millis) / 1000) as! T)
		}
		if T.self == Data.self {
			guard case .primitive(.binary(let data, _)) = node else { throw mismatch("BSON binary data") }
			return (data as! T)
		}
		if T.self == UUID.self {
			guard case .primitive(.binary(let data, let subtype)) = node,
				  subtype == BSON_SUBTYPE_UUID || subtype == BSON_SUBTYPE_UUID_DEPRECATED, data.count == 16 else {
				throw mismatch("a BSON UUID (binary subtype 4)")
			}
			var uuid: uuid_t = (0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0)
			withUnsafeMutableBytes(of: &uuid) { $0.copyBytes(from: data) }
			return (UUID(uuid: uuid) as! T)
		}
		if T.self == BSON.OID.self {
			switch node {
			case .primitive(.oid(let oid)):
				return (BSON.OID(oid: oid) as! T)
			case .primitive(.string(let string)):
				guard let oid = BSON.OID(validating: string) else { throw mismatch("an ObjectId") }
				return (oid as! T)
			default:
				throw mismatch("an ObjectId")
			}
		}
		return nil
	}

	func container<Key: CodingKey>(keyedBy type: Key.Type) throws -> KeyedDecodingContainer<Key> {
		guard case .document(let entries) = node else {
			throw DecodingError.typeMismatch([String: Any].self, .init(codingPath: codingPath, debugDescription: "Expected a document, found \(node.typeDescription)."))
		}
		return KeyedDecodingContainer(BSONKeyedDecodingContainer<Key>(decoder: self, entries: entries))
	}

	func unkeyedContainer() throws -> UnkeyedDecodingContainer {
		guard case .array(let values) = node else {
			throw DecodingError.typeMismatch([Any].self, .init(codingPath: codingPath, debugDescription: "Expected an array, found \(node.typeDescription)."))
		}
		return BSONUnkeyedDecodingContainer(decoder: self, values: values)
	}

	func singleValueContainer() throws -> SingleValueDecodingContainer {
		BSONSingleValueDecodingContainer(decoder: self)
	}
}

extension BSONNode {
	var typeDescription: String {
		switch self {
		case .document: return "a document"
		case .array: return "an array"
		case .primitive(let primitive):
			switch primitive {
			case .null: return "null"
			case .bool: return "a boolean"
			case .int32: return "an int32"
			case .int64: return "an int64"
			case .double: return "a double"
			case .string: return "a string"
			case .dateTime: return "a datetime"
			case .binary: return "binary data"
			case .oid: return "an ObjectId"
			case .unsupported(let name): return "\(name) (not supported by BSONDecoder)"
			}
		}
	}

	var isNull: Bool {
		if case .primitive(.null) = self {
			return true
		}
		return false
	}

	func decodePrimitive<T>(_ type: T.Type, codingPath: [CodingKey]) throws -> T {
		func mismatch() -> DecodingError {
			if isNull {
				return .valueNotFound(T.self, .init(codingPath: codingPath, debugDescription: "Expected \(T.self), found null."))
			}
			return .typeMismatch(T.self, .init(codingPath: codingPath, debugDescription: "Expected \(T.self), found \(typeDescription)."))
		}
		func inexact(_ value: Any) -> DecodingError {
			.dataCorrupted(.init(codingPath: codingPath, debugDescription: "\(value) does not fit exactly in \(T.self)."))
		}
		guard case .primitive(let primitive) = self else {
			throw mismatch()
		}
		switch T.self {
		case is Bool.Type:
			guard case .bool(let value) = primitive else { throw mismatch() }
			return value as! T
		case is String.Type:
			guard case .string(let value) = primitive else { throw mismatch() }
			return value as! T
		case is Double.Type, is Float.Type:
			let value: Double
			switch primitive {
			case .double(let v): value = v
			case .int32(let v): value = Double(v)
			case .int64(let v):
				guard let v = Double(exactly: v) else { throw inexact(v) }
				value = v
			default: throw mismatch()
			}
			if T.self == Float.self {
				return Float(value) as! T
			}
			return value as! T
		default:
			guard let integerType = T.self as? any FixedWidthInteger.Type else {
				throw mismatch()
			}
			func convert<I: FixedWidthInteger>(_ target: I.Type) throws -> I {
				switch primitive {
				case .int32(let v):
					guard let result = I(exactly: v) else { throw inexact(v) }
					return result
				case .int64(let v):
					guard let result = I(exactly: v) else { throw inexact(v) }
					return result
				case .double(let v):
					guard let result = I(exactly: v) else { throw inexact(v) }
					return result
				default:
					throw mismatch()
				}
			}
			return try convert(integerType) as! T
		}
	}
}

struct BSONKeyedDecodingContainer<Key: CodingKey>: KeyedDecodingContainerProtocol {
	let decoder: _BSONDecoder
	let values: [String: BSONNode]
	let allKeys: [Key]
	var codingPath: [CodingKey] { decoder.codingPath }

	init(decoder: _BSONDecoder, entries: [(String, BSONNode)]) {
		self.decoder = decoder
		var values: [String: BSONNode] = [:]
		for (key, value) in entries where values[key] == nil {
			values[key] = value
		}
		self.values = values
		self.allKeys = entries.compactMap { Key(stringValue: $0.0) }
	}

	func contains(_ key: Key) -> Bool {
		values[key.stringValue] != nil
	}

	private func node(_ key: Key) throws -> BSONNode {
		guard let node = values[key.stringValue] else {
			throw DecodingError.keyNotFound(key, .init(codingPath: codingPath, debugDescription: "No value for key '\(key.stringValue)'."))
		}
		return node
	}

	func decodeNil(forKey key: Key) throws -> Bool {
		try node(key).isNull
	}

	func decode(_ type: Bool.Type, forKey key: Key) throws -> Bool { try node(key).decodePrimitive(type, codingPath: codingPath + [key]) }
	func decode(_ type: String.Type, forKey key: Key) throws -> String { try node(key).decodePrimitive(type, codingPath: codingPath + [key]) }
	func decode(_ type: Double.Type, forKey key: Key) throws -> Double { try node(key).decodePrimitive(type, codingPath: codingPath + [key]) }
	func decode(_ type: Float.Type, forKey key: Key) throws -> Float { try node(key).decodePrimitive(type, codingPath: codingPath + [key]) }
	func decode(_ type: Int.Type, forKey key: Key) throws -> Int { try node(key).decodePrimitive(type, codingPath: codingPath + [key]) }
	func decode(_ type: Int8.Type, forKey key: Key) throws -> Int8 { try node(key).decodePrimitive(type, codingPath: codingPath + [key]) }
	func decode(_ type: Int16.Type, forKey key: Key) throws -> Int16 { try node(key).decodePrimitive(type, codingPath: codingPath + [key]) }
	func decode(_ type: Int32.Type, forKey key: Key) throws -> Int32 { try node(key).decodePrimitive(type, codingPath: codingPath + [key]) }
	func decode(_ type: Int64.Type, forKey key: Key) throws -> Int64 { try node(key).decodePrimitive(type, codingPath: codingPath + [key]) }
	func decode(_ type: UInt.Type, forKey key: Key) throws -> UInt { try node(key).decodePrimitive(type, codingPath: codingPath + [key]) }
	func decode(_ type: UInt8.Type, forKey key: Key) throws -> UInt8 { try node(key).decodePrimitive(type, codingPath: codingPath + [key]) }
	func decode(_ type: UInt16.Type, forKey key: Key) throws -> UInt16 { try node(key).decodePrimitive(type, codingPath: codingPath + [key]) }
	func decode(_ type: UInt32.Type, forKey key: Key) throws -> UInt32 { try node(key).decodePrimitive(type, codingPath: codingPath + [key]) }
	func decode(_ type: UInt64.Type, forKey key: Key) throws -> UInt64 { try node(key).decodePrimitive(type, codingPath: codingPath + [key]) }

	func decode<T: Decodable>(_ type: T.Type, forKey key: Key) throws -> T {
		try _BSONDecoder.decodeValue(type, from: try node(key), codingPath: codingPath + [key], userInfo: decoder.sendableUserInfo)
	}

	func nestedContainer<NestedKey: CodingKey>(keyedBy type: NestedKey.Type, forKey key: Key) throws -> KeyedDecodingContainer<NestedKey> {
		try _BSONDecoder(node: try node(key), codingPath: codingPath + [key], userInfo: decoder.sendableUserInfo).container(keyedBy: type)
	}

	func nestedUnkeyedContainer(forKey key: Key) throws -> UnkeyedDecodingContainer {
		try _BSONDecoder(node: try node(key), codingPath: codingPath + [key], userInfo: decoder.sendableUserInfo).unkeyedContainer()
	}

	func superDecoder() throws -> Decoder {
		let key = BSONKey(stringValue: "super")
		return _BSONDecoder(node: values["super"] ?? .primitive(.null), codingPath: codingPath + [key], userInfo: decoder.sendableUserInfo)
	}

	func superDecoder(forKey key: Key) throws -> Decoder {
		_BSONDecoder(node: values[key.stringValue] ?? .primitive(.null), codingPath: codingPath + [key], userInfo: decoder.sendableUserInfo)
	}
}

struct BSONUnkeyedDecodingContainer: UnkeyedDecodingContainer {
	let decoder: _BSONDecoder
	let values: [BSONNode]
	var currentIndex = 0
	var codingPath: [CodingKey] { decoder.codingPath }
	var count: Int? { values.count }
	var isAtEnd: Bool { currentIndex >= values.count }

	init(decoder: _BSONDecoder, values: [BSONNode]) {
		self.decoder = decoder
		self.values = values
	}

	private var currentKey: CodingKey { BSONKey(intValue: currentIndex) }

	private mutating func next<T>(_ type: T.Type) throws -> BSONNode {
		guard !isAtEnd else {
			throw DecodingError.valueNotFound(type, .init(codingPath: codingPath + [currentKey], debugDescription: "Unkeyed container is at end."))
		}
		defer { currentIndex += 1 }
		return values[currentIndex]
	}

	private mutating func primitive<T>(_ type: T.Type) throws -> T {
		let path = codingPath + [currentKey]
		return try next(type).decodePrimitive(type, codingPath: path)
	}

	mutating func decodeNil() throws -> Bool {
		guard !isAtEnd else {
			throw DecodingError.valueNotFound(Any?.self, .init(codingPath: codingPath + [currentKey], debugDescription: "Unkeyed container is at end."))
		}
		if values[currentIndex].isNull {
			currentIndex += 1
			return true
		}
		return false
	}

	mutating func decode(_ type: Bool.Type) throws -> Bool { try primitive(type) }
	mutating func decode(_ type: String.Type) throws -> String { try primitive(type) }
	mutating func decode(_ type: Double.Type) throws -> Double { try primitive(type) }
	mutating func decode(_ type: Float.Type) throws -> Float { try primitive(type) }
	mutating func decode(_ type: Int.Type) throws -> Int { try primitive(type) }
	mutating func decode(_ type: Int8.Type) throws -> Int8 { try primitive(type) }
	mutating func decode(_ type: Int16.Type) throws -> Int16 { try primitive(type) }
	mutating func decode(_ type: Int32.Type) throws -> Int32 { try primitive(type) }
	mutating func decode(_ type: Int64.Type) throws -> Int64 { try primitive(type) }
	mutating func decode(_ type: UInt.Type) throws -> UInt { try primitive(type) }
	mutating func decode(_ type: UInt8.Type) throws -> UInt8 { try primitive(type) }
	mutating func decode(_ type: UInt16.Type) throws -> UInt16 { try primitive(type) }
	mutating func decode(_ type: UInt32.Type) throws -> UInt32 { try primitive(type) }
	mutating func decode(_ type: UInt64.Type) throws -> UInt64 { try primitive(type) }

	mutating func decode<T: Decodable>(_ type: T.Type) throws -> T {
		let path = codingPath + [currentKey]
		return try _BSONDecoder.decodeValue(type, from: try next(type), codingPath: path, userInfo: decoder.sendableUserInfo)
	}

	mutating func nestedContainer<NestedKey: CodingKey>(keyedBy type: NestedKey.Type) throws -> KeyedDecodingContainer<NestedKey> {
		try superDecoder().container(keyedBy: type)
	}

	mutating func nestedUnkeyedContainer() throws -> UnkeyedDecodingContainer {
		try superDecoder().unkeyedContainer()
	}

	mutating func superDecoder() throws -> Decoder {
		let path = codingPath + [currentKey]
		return _BSONDecoder(node: try next(Any.self), codingPath: path, userInfo: decoder.sendableUserInfo)
	}
}

struct BSONSingleValueDecodingContainer: SingleValueDecodingContainer {
	let decoder: _BSONDecoder
	var codingPath: [CodingKey] { decoder.codingPath }

	func decodeNil() -> Bool { decoder.node.isNull }
	func decode(_ type: Bool.Type) throws -> Bool { try decoder.node.decodePrimitive(type, codingPath: codingPath) }
	func decode(_ type: String.Type) throws -> String { try decoder.node.decodePrimitive(type, codingPath: codingPath) }
	func decode(_ type: Double.Type) throws -> Double { try decoder.node.decodePrimitive(type, codingPath: codingPath) }
	func decode(_ type: Float.Type) throws -> Float { try decoder.node.decodePrimitive(type, codingPath: codingPath) }
	func decode(_ type: Int.Type) throws -> Int { try decoder.node.decodePrimitive(type, codingPath: codingPath) }
	func decode(_ type: Int8.Type) throws -> Int8 { try decoder.node.decodePrimitive(type, codingPath: codingPath) }
	func decode(_ type: Int16.Type) throws -> Int16 { try decoder.node.decodePrimitive(type, codingPath: codingPath) }
	func decode(_ type: Int32.Type) throws -> Int32 { try decoder.node.decodePrimitive(type, codingPath: codingPath) }
	func decode(_ type: Int64.Type) throws -> Int64 { try decoder.node.decodePrimitive(type, codingPath: codingPath) }
	func decode(_ type: UInt.Type) throws -> UInt { try decoder.node.decodePrimitive(type, codingPath: codingPath) }
	func decode(_ type: UInt8.Type) throws -> UInt8 { try decoder.node.decodePrimitive(type, codingPath: codingPath) }
	func decode(_ type: UInt16.Type) throws -> UInt16 { try decoder.node.decodePrimitive(type, codingPath: codingPath) }
	func decode(_ type: UInt32.Type) throws -> UInt32 { try decoder.node.decodePrimitive(type, codingPath: codingPath) }
	func decode(_ type: UInt64.Type) throws -> UInt64 { try decoder.node.decodePrimitive(type, codingPath: codingPath) }

	func decode<T: Decodable>(_ type: T.Type) throws -> T {
		try _BSONDecoder.decodeValue(type, from: decoder.node, codingPath: codingPath, userInfo: decoder.sendableUserInfo)
	}
}
