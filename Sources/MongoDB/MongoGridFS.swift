//
//  MongoGridFS.swift
//  MongoDB
//
//  Created by Rockford Wei on 2016-12-14.
//  Copyright © 2016 PerfectlySoft. All rights reserved.
//
//===----------------------------------------------------------------------===//
//
// This source file is part of the Perfect.org open source project
//
// Copyright (c) 2016 - 2017 PerfectlySoft Inc. and the Perfect project authors
// Licensed under Apache License v2.0
//
// See http://perfect.org/licensing.html for license information
//
//===----------------------------------------------------------------------===//
//

import libmongoc

public class GridFS {

  private var handle: OpaquePointer?
  var error = bson_error_t()

  private func _PTR(_ of: UnsafePointer<Int8>) -> UnsafePointer<Int8> {
    return of
  }//end _PTR

  public init(client: MongoClient, db: String, prefix: String? = nil) throws {
    handle = mongoc_client_get_gridfs(client.ptr, db, prefix, &error)
    guard handle != nil else {
      throw MongoClientError.initError("gridfs.init() = [\(error.code), \(error.domain)]")
    }//end guard
  }//end init

  public func close() {
    mongoc_gridfs_destroy(handle)
  }//end close

  @discardableResult
  public func list() throws -> [String]{
    var query = bson_t()
    var child = bson_t()
    bson_init(&query)
    bson_append_document_begin (&query, "$orderby", -1, &child);
    bson_append_int32 (&child, "filename", -1, 1);
    bson_append_document_end (&query, &child);
    bson_append_document_begin (&query, "$query", -1, &child);
    bson_append_document_end (&query, &child);
    guard let list = mongoc_gridfs_find_with_opts(handle, &query, nil) else {
      bson_destroy(&query)
      throw MongoClientError.initError("gridfs.list()")
    }//end guard
    bson_destroy(&query)
    var file: OpaquePointer?
    var ret:[String] = []
    repeat {
      file = mongoc_gridfs_file_list_next(list)
      if (file == nil) {
        break
      }//end if
      let cstr = mongoc_gridfs_file_get_filename(file)
      let name = String.init(cString: unsafeBitCast(cstr, to: UnsafePointer<CChar>.self))
      ret.append(name)
      mongoc_gridfs_file_destroy(file)
    }while(file != nil)
    if (ret.count > 0) {
      mongoc_gridfs_file_list_destroy(list)
    }//end if
    return ret
  }//end list

  public func upload(from: String, to: String) throws {
    guard let stream = mongoc_stream_file_new_for_path(from, O_RDONLY, 0) else {
      throw MongoClientError.initError("gridfs.upload.from(\(from))")
    }//end guard
    var opt = mongoc_gridfs_file_opt_t()
    opt.filename = _PTR(to)
    guard let file = mongoc_gridfs_create_file_from_stream(handle, stream, &opt) else {
      throw MongoClientError.initError("gridfs.upload.open(\(to)")
    }//end guard
    mongoc_gridfs_file_save(file)
    mongoc_gridfs_file_destroy(file)
  }//end upload

  @discardableResult
  public func download(from: String, to: String) throws -> Int {
    guard let file = mongoc_gridfs_find_one_by_filename(handle, from, &error) else{
      throw MongoClientError.initError("gridfs.download.find(\(from)) = \(error.code)")
    }//end file
    let fp = fopen(to, "wb")
    let stream = mongoc_stream_gridfs_new(file)
    var r = 0
    var write_ok = true
    var iov = mongoc_iovec_t()
    iov.iov_len = 4096
    iov.iov_base = malloc(iov.iov_len)
    var total = 0
    repeat {
      r = mongoc_stream_readv (stream, &iov, 1, -1, 0)
      if (r > 0) {
        let w = fwrite(iov.iov_base, 1, r, fp)
        write_ok = r == w
        total += w
      }//end if
    }while(r != 0 && write_ok)
    free(iov.iov_base)
    mongoc_stream_destroy(stream)
    mongoc_gridfs_file_destroy(file)
    fclose(fp)

    if write_ok {
      return total
    }//end if
    throw MongoClientError.initError("gridfs.download.write(\(to))")
  }//end download

  public func delete(remoteFile: String) throws {
    guard let file = mongoc_gridfs_find_one_by_filename(handle, remoteFile, &error) else{
      throw MongoClientError.initError("gridfs.delete.find(\(remoteFile)) = \(error.code)")
    }//end file
    if mongoc_gridfs_file_remove(file, &error) {
      return
    }//end if
    throw MongoClientError.initError("gridfs.deleted(\(remoteFile))")
  }//end delete
}//end class

extension MongoClient {
  public func gridFS(db: String, prefix: String? = nil) throws -> GridFS {
    return try GridFS(client: self, db: db, prefix: prefix)
  }//end gridFS
}//end MongoClient


