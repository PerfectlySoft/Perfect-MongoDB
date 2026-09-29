//
//  MongoClientPool.swift
//  MongoClientPool
//
//  Created by Kyle Petr Pavlik on 2016-03-15.
//  Copyright © 2016 PerfectlySoft. All rights reserved.
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

/// Allows connection pooling. This class is thread-safe: libmongoc's client pool
/// synchronizes popping and pushing, so one pool can be shared across tasks and threads.
/// The clients it hands out are not; use each from one task at a time.
public final class MongoClientPool: @unchecked Sendable {
    
    var ptr = OpaquePointer(bitPattern: 0)
    /**
     *  create new ClientPool with provided String uri
     *
     *  - parameter uri: String uri to connect client pool
    */
    /// Traps with the error if **uri** is not a valid MongoDB connection string.
    /// Use `init(validatingURI:)` to handle that case instead.
    public convenience init(uri: String) {
        do {
            try self.init(validatingURI: uri)
        } catch {
            fatalError("MongoClientPool: \(error)")
        }
    }

    /// Creates a pool, throwing `MongoError` if **uri** is not a valid MongoDB connection
    /// string or its options (for example TLS settings) are rejected. Connecting happens
    /// later, when a client is first used.
    public init(validatingURI uri: String) throws {
        mongocInitialized
        var error = bson_error_t()
        guard let uriPointer = mongoc_uri_new_with_error(uri, &error) else {
            throw MongoError(error)
        }
        defer {
            mongoc_uri_destroy(uriPointer)
        }
        guard let pool = mongoc_client_pool_new_with_error(uriPointer, &error) else {
            throw MongoError(error)
        }
        ptr = pool
    }
    
    deinit {
        if ptr != nil {
            mongoc_client_pool_destroy(ptr)
        }
    }
    
    /**
     *  Try to pop a client connection from the connection pool.
     *
     *  - returns: nil if no client connection is currently queued for reuse.
     */
    public func tryPopClient() -> MongoClient? {
        guard let clientPointer = mongoc_client_pool_try_pop(ptr) else {
            return nil
        }
        return MongoClient(pointer: clientPointer, pool: self)
    }

    /**
     *  Pop a client connection from the connection pool.
     *
     *  A popped client goes back to the pool when you push it or when it is released.
     *
     * - returns: MongoClient from connection pool
    */
    public func popClient() -> MongoClient {
        return MongoClient(pointer: mongoc_client_pool_pop(ptr), pool: self)
    }

    /**
     *  Pushes back popped client connection.
     *
     *  - parameter client: MongoClient to be pushed back into pool
     */
    public func pushClient(_ client: MongoClient) {
        guard let clientPointer = client.ptr else {
            return
        }
        mongoc_client_pool_push(ptr, clientPointer)
        client.ptr = nil
        client.pool = nil
    }
    
    /**
     *  Automatically pops a client, makes it available within the block and pushes it back.
     *
     *  - parameter block: block to be executed with popped client
     */
	public func executeBlock(_ block: (_ client: MongoClient) -> Void) {
        let client = popClient()
        block(client)
        pushClient(client)
    }
}

