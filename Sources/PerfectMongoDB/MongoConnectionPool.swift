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

/// Allows connection pooling. This class is thread-safe.
public class MongoClientPool {
    
    var ptr = OpaquePointer(bitPattern: 0)
    /**
     *  create new ClientPool with provided String uri
     *
     *  - parameter uri: String uri to connect client pool
    */
    /// Traps with the parse error if **uri** is not a valid MongoDB connection string.
    public init(uri: String) {
        mongocInitialized
        var error = bson_error_t()
        guard let uriPointer = mongoc_uri_new_with_error(uri, &error) else {
            guard case .error(_, _, let message) = MongoResult.fromError(error) else {
                fatalError("MongoClientPool: invalid URI '\(uri)'")
            }
            fatalError("MongoClientPool: invalid URI '\(uri)': \(message)")
        }
        defer {
            mongoc_uri_destroy(uriPointer)
        }
        ptr = mongoc_client_pool_new(uriPointer)
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

