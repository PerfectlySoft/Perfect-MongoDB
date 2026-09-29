//
//  MongoAsync.swift
//  PerfectMongoDB
//
//  async/await on top of the blocking libmongoc API.
//
//  libmongoc calls block on network I/O, so they must never run on Swift's cooperative
//  thread pool. As in Perfect-CRUD's AsyncExecution.swift, blocking work is dispatched to
//  a dedicated concurrent queue behind a checked continuation: the caller's task stays
//  suspended, and no extra unstructured Task is created per call.
//
//  Thread-safety model: MongoClientPool is the only object shared across tasks. A client,
//  and every database, collection and cursor made from it, is used by one piece of work at
//  a time -- inside one `withClient` body, or by one async iterator whose `next()` calls
//  never overlap. Nothing in the type system stops a caller from capturing the client out
//  of `withClient` and using it afterwards; don't.
//
//  Cancellation is best-effort, as in Perfect-CRUD: a blocking libmongoc call already in
//  flight can't be interrupted. `MongoFindSequence` checks for cancellation between batches.
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

import Dispatch
import PerfectCMongo

enum MongoBlockingQueue {
	static let shared = DispatchQueue(label: "PerfectMongoDB.blocking", qos: .userInitiated, attributes: .concurrent)
}

func withMongoExecutor<T: Sendable>(_ body: @Sendable @escaping () throws -> T) async throws -> T {
	try await withCheckedThrowingContinuation { continuation in
		MongoBlockingQueue.shared.async {
			do {
				continuation.resume(returning: try body())
			} catch {
				continuation.resume(throwing: error)
			}
		}
	}
}

public extension MongoClientPool {
	/// Pops a client, runs **body** with it off the cooperative thread pool, and pushes the
	/// client back, even if **body** throws. Waits for a free client when the pool is at its
	/// maximum size.
	///
	/// ```swift
	/// let adults = try await pool.withClient { client in
	///     try client.getCollection(databaseName: "app", collectionName: "users")
	///         .find(User.self, filter: try BSON(json: #"{"age": {"$gte": 18}}"#))
	/// }
	/// ```
	func withClient<T: Sendable>(_ body: @Sendable @escaping (MongoClient) throws -> T) async throws -> T {
		try await withMongoExecutor {
			let client = self.popClient()
			defer {
				self.pushClient(client)
			}
			return try body(client)
		}
	}

	/// Streams the documents in **collection** matching **filter**, decoded as **type**.
	/// One pooled client is held for the whole iteration and returned to the pool when the
	/// sequence ends, throws, or its iterator is released.
	///
	/// ```swift
	/// for try await user in pool.find(User.self, database: "app", collection: "users") {
	///     print(user.name)
	/// }
	/// ```
	///
	/// - parameter batchSize: documents fetched and decoded per hop to the blocking queue.
	func find<T: Decodable & Sendable>(_ type: T.Type,
									   database: String,
									   collection: String,
									   filter: BSON = BSON(),
									   options: BSON? = nil,
									   batchSize: Int = 100,
									   decoder: BSONDecoder = BSONDecoder()) -> MongoFindSequence<T> {
		MongoFindSequence(pool: self,
						  database: database,
						  collection: collection,
						  filter: filter.asBytes,
						  options: options?.asBytes,
						  batchSize: max(1, batchSize),
						  decoder: decoder)
	}
}

/// An `AsyncSequence` of decoded documents from a find on a pooled client.
/// Create one with `MongoClientPool.find(_:database:collection:filter:options:batchSize:decoder:)`.
/// Each iteration runs its own query.
public struct MongoFindSequence<Element: Decodable & Sendable>: AsyncSequence, Sendable {
	let pool: MongoClientPool
	let database: String
	let collection: String
	let filter: [UInt8]
	let options: [UInt8]?
	let batchSize: Int
	let decoder: BSONDecoder

	public func makeAsyncIterator() -> Iterator {
		Iterator(state: MongoFindCursorState(sequence: self))
	}

	public struct Iterator: AsyncIteratorProtocol {
		let state: MongoFindCursorState<Element>
		var buffer: [Element] = []
		var index = 0

		public mutating func next() async throws -> Element? {
			if index == buffer.count {
				try Task.checkCancellation()
				let state = self.state
				buffer = try await withMongoExecutor { try state.fetchBatch() }
				index = 0
				if buffer.isEmpty {
					return nil
				}
			}
			defer {
				index += 1
			}
			return buffer[index]
		}
	}
}

/// The libmongoc cursor behind one `MongoFindSequence` iteration.
///
/// `@unchecked Sendable` because it moves between blocking work items, but it is only ever
/// touched by one at a time: an async iterator's `next()` calls never overlap.
final class MongoFindCursorState<Element: Decodable & Sendable>: @unchecked Sendable {
	private let sequence: MongoFindSequence<Element>
	private var client: MongoClient?
	private var collection: MongoCollection?
	private var cursor: OpaquePointer?
	private var finished = false

	init(sequence: MongoFindSequence<Element>) {
		self.sequence = sequence
	}

	deinit {
		finish()
	}

	/// The next batch of up to `batchSize` documents; empty when the cursor is exhausted.
	func fetchBatch() throws -> [Element] {
		guard !finished else {
			return []
		}
		do {
			let cursor = try openCursor()
			var batch: [Element] = []
			var document: UnsafePointer<bson_t>? = nil
			while batch.count < sequence.batchSize, mongoc_cursor_next(cursor, &document), let document {
				batch.append(try sequence.decoder.decode(Element.self, from: document))
			}
			if batch.count < sequence.batchSize {
				var error = bson_error_t()
				if mongoc_cursor_error(cursor, &error) {
					throw MongoError(error)
				}
				finish()
			}
			return batch
		} catch {
			finish()
			throw error
		}
	}

	private func openCursor() throws -> OpaquePointer {
		if let cursor {
			return cursor
		}
		let client = sequence.pool.popClient()
		self.client = client
		let collection = client.getCollection(databaseName: sequence.database, collectionName: sequence.collection)
		self.collection = collection
		let filter = BSON(bytes: sequence.filter)
		let options = sequence.options.map { BSON(bytes: $0) }
		guard let ptr = collection.ptr,
			  let cursor = mongoc_collection_find_with_opts(ptr, toOpaque(filter.doc), toOpaque(options?.doc), nil) else {
			throw MongoError("find failed")
		}
		self.cursor = cursor
		return cursor
	}

	private func finish() {
		finished = true
		if let cursor {
			mongoc_cursor_destroy(cursor)
			self.cursor = nil
		}
		collection = nil
		// Released pooled clients return to the pool.
		client = nil
	}
}
