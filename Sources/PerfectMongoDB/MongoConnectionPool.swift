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

import libmongoc

/// Allows connection pooling. This class is thread-safe.
public class MongoClientPool {
    
    var ptr = OpaquePointer(bitPattern: 0)
    /**
     *  create new ClientPool with provided String uri
     *
     *  - parameter uri: String uri to connect client pool
    */
    public init(uri: String) {
        
        let uriPointer = mongoc_uri_new(uri)
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
        let clientPointer = mongoc_client_pool_try_pop(ptr)
        if clientPointer != nil {
            return MongoClient(pointer: mongoc_client_pool_pop(clientPointer))
        }
        return nil
    }
    
    /**
     *  Pop a client connection from the connection pool.
     *
     * - returns: MongoClient from connection pool
    */
    public func popClient() -> MongoClient {
        return MongoClient(pointer: mongoc_client_pool_pop(ptr))
    }

    /**
     *  Pushes back popped client connection.
     *
     *  - parameter client: MongoClient to be pushed back into pool
     */
    public func pushClient(_ client: MongoClient) {
        mongoc_client_pool_push(ptr, client.ptr)
        client.ptr = nil
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

