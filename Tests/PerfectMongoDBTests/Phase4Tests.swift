//
//  Phase4Tests.swift
//  PerfectMongoDBTests
//
//  Codable BSON, the typed collection API, and async/await.
//

import Foundation
import XCTest
@testable import PerfectMongoDB

private struct Address: Codable, Equatable, Sendable {
	var city: String
	var zip: String?
}

private enum Role: String, Codable, Sendable {
	case admin, member
}

private struct Everything: Codable, Equatable, Sendable {
	var id: BSON.OID
	var name: String
	var flag: Bool
	var int: Int
	var int8: Int8
	var int16: Int16
	var int32: Int32
	var int64: Int64
	var uint8: UInt8
	var uint32: UInt32
	var uint64: UInt64
	var float: Float
	var double: Double
	var date: Date
	var data: Data
	var uuid: UUID
	var role: Role
	var address: Address
	var tags: [String]
	var matrix: [[Int]]
	var scores: [String: Int]
	var nickname: String?
	var missing: String?
	var history: [Address]

	enum CodingKeys: String, CodingKey {
		case id = "_id"
		case name, flag, int, int8, int16, int32, int64, uint8, uint32, uint64, float, double
		case date, data, uuid, role, address, tags, matrix, scores, nickname, missing, history
	}

	static func sample(_ n: Int = 1) -> Everything {
		Everything(id: BSON.OID(), name: "Ada \(n)", flag: true, int: n, int8: -8, int16: 1600, int32: -32,
				   int64: 1 << 40, uint8: 255, uint32: 4_000_000_000, uint64: 1 << 50, float: 1.5, double: 2.25,
				   date: Date(timeIntervalSince1970: 1_700_000_000.123), data: Data([0, 1, 2, 255]), uuid: UUID(),
				   role: .admin, address: Address(city: "Telluride", zip: nil), tags: ["a", "b"],
				   matrix: [[1, 2], [3]], scores: ["math": 90], nickname: "A", missing: nil,
				   history: [Address(city: "Ouray", zip: "81427")])
	}
}

private struct Person: Codable, Equatable, Sendable {
	var name: String
	var age: Int
}

final class Phase4Tests: XCTestCase {

	// MARK: Codable

	func testCodableRoundTrip() throws {
		let value = Everything.sample()
		let document = try BSONEncoder().encode(value)
		let decoded = try BSONDecoder().decode(Everything.self, from: document)
		XCTAssertEqual(decoded, value)
	}

	func testEncodesNativeBSONTypes() throws {
		let value = Everything.sample()
		let json = try BSONEncoder().encode(value).asString
		XCTAssert(json.contains("\"_id\" : { \"$oid\" : \"\(value.id)\" }"), json)
		XCTAssert(json.contains("\"date\" : { \"$date\" : 1700000000123 }"), json)
		XCTAssert(json.contains("\"$type\" : \"04\""), "UUID should be binary subtype 4: \(json)")
		XCTAssert(json.contains("\"int64\" : 1099511627776"), json)
		XCTAssertFalse(json.contains("\"missing\""), "nil optionals are omitted: \(json)")
		XCTAssert(json.contains("\"address\" : { \"city\" : \"Telluride\" }"), json)
	}

	func testDecodeNumericConversions() throws {
		let doc = try BSON(json: #"{"a": {"$numberInt": "7"}, "b": 3.0, "c": {"$numberLong": "12"}}"#)
		struct Numbers: Decodable { var a: Int64; var b: Int; var c: Double }
		let numbers = try BSONDecoder().decode(Numbers.self, from: doc)
		XCTAssertEqual(numbers.a, 7)
		XCTAssertEqual(numbers.b, 3)
		XCTAssertEqual(numbers.c, 12)

		struct Small: Decodable { var b: Int }
		XCTAssertThrowsError(try BSONDecoder().decode(Small.self, from: try BSON(json: #"{"b": 3.5}"#))) { error in
			guard case DecodingError.dataCorrupted = error else { return XCTFail("\(error)") }
		}
		struct Tiny: Decodable { var b: UInt8 }
		XCTAssertThrowsError(try BSONDecoder().decode(Tiny.self, from: try BSON(json: #"{"b": 300}"#)))
	}

	func testDecodingErrors() throws {
		XCTAssertThrowsError(try BSONDecoder().decode(Person.self, from: try BSON(json: #"{"name": "x"}"#))) { error in
			guard case DecodingError.keyNotFound(let key, _) = error else { return XCTFail("\(error)") }
			XCTAssertEqual(key.stringValue, "age")
		}
		XCTAssertThrowsError(try BSONDecoder().decode(Person.self, from: try BSON(json: #"{"name": "x", "age": "old"}"#))) { error in
			guard case DecodingError.typeMismatch = error else { return XCTFail("\(error)") }
		}
		XCTAssertThrowsError(try BSONDecoder().decode(Person.self, from: try BSON(json: #"{"name": null, "age": 1}"#))) { error in
			guard case DecodingError.valueNotFound = error else { return XCTFail("\(error)") }
		}
		XCTAssertThrowsError(try BSONEncoder().encode(42), "a bare Int is not a document")
	}

	func testObjectIdOutsideBSON() throws {
		let oid = BSON.OID()
		let json = try JSONEncoder().encode([oid])
		XCTAssertEqual(String(decoding: json, as: UTF8.self), "[\"\(oid)\"]")
		XCTAssertEqual(try JSONDecoder().decode([BSON.OID].self, from: json), [oid])
		XCTAssertNil(BSON.OID(validating: "not an id"))
	}

	// MARK: Typed collection API

	private func typedCollection(_ name: String) throws -> (MongoClient, MongoCollection) {
		let client = try MongoClient(uri: testURI)
		let collection = client.getCollection(databaseName: "test", collectionName: name)
		_ = collection.drop()
		return (client, collection)
	}

	func testTypedCollectionOperations() throws {
		let (client, collection) = try typedCollection("phase4typed")
		defer { withExtendedLifetime(client) {} }
		defer { _ = collection.drop() }

		let sample = Everything.sample()
		try collection.insert(sample)
		XCTAssertEqual(try collection.findOne(Everything.self), sample)

		try collection.insert(contentsOf: (1...5).map { Person(name: "p\($0)", age: $0 * 10) })
		let byAgeDesc = try collection.find(Person.self, filter: try BSON(json: #"{"age": {"$gte": 20}}"#), options: try BSON(json: #"{"sort": {"age": -1}}"#))
		XCTAssertEqual(byAgeDesc.map(\.age), [50, 40, 30, 20])
		XCTAssertEqual(try collection.countDocuments(filter: try BSON(json: #"{"age": {"$exists": true}}"#)), 5)

		XCTAssertEqual(try collection.updateMany(filter: try BSON(json: #"{"age": {"$lt": 30}}"#), update: try BSON(json: #"{"$inc": {"age": 1}}"#)), 2)
		XCTAssertEqual(try collection.findOne(Person.self, filter: try BSON(json: #"{"name": "p1"}"#))?.age, 11)
		XCTAssertEqual(try collection.updateOne(filter: try BSON(json: #"{"name": "p2"}"#), update: try BSON(json: #"{"$set": {"age": 99}}"#)), 1)

		XCTAssertEqual(try collection.replaceOne(filter: try BSON(json: #"{"name": "p9"}"#), with: Person(name: "p9", age: 9)), 0)
		try collection.replaceOne(filter: try BSON(json: #"{"name": "p9"}"#), with: Person(name: "p9", age: 9), upsert: true)
		XCTAssertEqual(try collection.findOne(Person.self, filter: try BSON(json: #"{"name": "p9"}"#)), Person(name: "p9", age: 9))

		XCTAssertEqual(try collection.deleteOne(filter: try BSON(json: #"{"name": "p9"}"#)), 1)
		XCTAssertEqual(try collection.deleteMany(filter: try BSON(json: #"{"age": {"$exists": true}}"#)), 5)
		XCTAssertNil(try collection.findOne(Person.self, filter: try BSON(json: #"{"name": "p1"}"#)))
	}

	func testTypedWriteErrorsThrow() throws {
		let (client, collection) = try typedCollection("phase4errors")
		defer { withExtendedLifetime(client) {} }
		defer { _ = collection.drop() }

		let sample = Everything.sample()
		try collection.insert(sample)
		XCTAssertThrowsError(try collection.insert(sample)) { error in
			guard let error = error as? MongoError else { return XCTFail("\(error)") }
			XCTAssertEqual(error.code, 11000, "duplicate key: \(error)")
		}
		XCTAssertThrowsError(try collection.find(Person.self)) { error in
			guard case DecodingError.keyNotFound = error else { return XCTFail("\(error)") }
		}
	}

	// MARK: async

	private func poolURI(maxPoolSize: Int) -> String {
		testURI + (testURI.contains("?") ? "&" : "/?") + "maxPoolSize=\(maxPoolSize)"
	}

	func testWithClientConcurrently() async throws {
		let pool = MongoClientPool(uri: poolURI(maxPoolSize: 4))
		try await pool.withClient { client in
			_ = client.getCollection(databaseName: "test", collectionName: "phase4async").drop()
		}
		try await withThrowingTaskGroup(of: Void.self) { group in
			for n in 1...20 {
				group.addTask {
					try await pool.withClient { client in
						try client.getCollection(databaseName: "test", collectionName: "phase4async")
							.insert(Person(name: "task\(n)", age: n))
					}
				}
			}
			try await group.waitForAll()
		}
		let count = try await pool.withClient { client in
			try client.getCollection(databaseName: "test", collectionName: "phase4async").countDocuments()
		}
		XCTAssertEqual(count, 20)
		try await pool.withClient { client in
			_ = client.getCollection(databaseName: "test", collectionName: "phase4async").drop()
		}
	}

	func testFindSequence() async throws {
		let pool = MongoClientPool(uri: poolURI(maxPoolSize: 2))
		try await pool.withClient { client in
			let collection = client.getCollection(databaseName: "test", collectionName: "phase4sequence")
			_ = collection.drop()
			try collection.insert(contentsOf: (0..<250).map { Person(name: "n\($0)", age: $0) })
		}

		var ages: [Int] = []
		for try await person in pool.find(Person.self, database: "test", collection: "phase4sequence",
										  options: try BSON(json: #"{"sort": {"age": 1}}"#), batchSize: 100) {
			ages.append(person.age)
		}
		XCTAssertEqual(ages, Array(0..<250))

		// Leaving a loop early releases the iterator and returns its client to the pool.
		for _ in 0..<5 {
			for try await _ in pool.find(Person.self, database: "test", collection: "phase4sequence", batchSize: 10) {
				break
			}
		}
		let first = pool.tryPopClient()
		let second = pool.tryPopClient()
		XCTAssertNotNil(first, "a pooled client leaked")
		XCTAssertNotNil(second, "a pooled client leaked")
		first.map(pool.pushClient)
		second.map(pool.pushClient)

		// Decoding errors surface from the loop.
		struct Wrong: Decodable, Sendable { var missing: String }
		do {
			for try await _ in pool.find(Wrong.self, database: "test", collection: "phase4sequence") {}
			XCTFail("expected a decoding error")
		} catch is DecodingError {
		}

		try await pool.withClient { client in
			_ = client.getCollection(databaseName: "test", collectionName: "phase4sequence").drop()
		}
	}
}
