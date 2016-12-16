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


public class GridFile {
  private var _fp: OpaquePointer?
  var error = bson_error_t()

  public init(_ from: OpaquePointer?) throws {
    guard from != nil else {
      throw MongoClientError.initError("gridfs.file.init(nil)")
    }//end guard
    _fp = from
  }//end init

  public init(gridFS:OpaquePointer?, from: String ) throws {
    guard let file = mongoc_gridfs_find_one_by_filename(gridFS, from, &error) else{
      throw MongoClientError.initError("gridfs.file.init(\(from)) = \(error.code)")
    }//end file
    _fp = file
  }//end init

  public func close() {
    mongoc_gridfs_file_destroy(_fp);
  }//end close

  private func _STR(_ u: UnsafePointer<bson_value_t>) -> String{
    var v = u.pointee
    if v.value_type == BSON_TYPE_UTF8 {
      let p = unsafeBitCast(v.value.v_utf8, to: UnsafePointer<CChar>.self)
      return String.init(cString: p)
    }else {
      return ""
    }//end if
  }//end str

  private func _STR(_ u: UnsafePointer<Int8>) -> String{
    let p = unsafeBitCast(u, to: UnsafePointer<CChar>.self)
    return String.init(cString: p)
  }//end str

  public var id: String {
    get { return _STR(mongoc_gridfs_file_get_id(_fp)) }
  }//end id

  public var md5: String {
    get {
      let m = mongoc_gridfs_file_get_md5(_fp)
      if m == nil {
        return ""
      }
      return _STR(m!)
    }//end get
    /// LINK BUG TO FIX: unresolved symbol mongoc_gridfs_file_set_md5
    /*
    set {
      mongoc_gridfs_file_set_md5(_fp, md5)
    }//end set
    */
  }//end md5

  public var aliases: BSON {
    get {
      let a = BSON()
      a.doc = unsafeBitCast(mongoc_gridfs_file_get_aliases(_fp), to:UnsafeMutablePointer<bson_t>.self)
      return a
    }//end get
  }//end aliases

  public var contentType: String {
    get {
      guard let t = mongoc_gridfs_file_get_content_type(_fp) else {
        return ""
      }
      return _STR(t)
    }
  }//end contentType

  public var length: Int64 {
    get { return mongoc_gridfs_file_get_length(_fp) }
  }//end length

  public var uploadDate: Int64 {
    get { return mongoc_gridfs_file_get_upload_date(_fp) }
  }//end uploadDate

  public var fileName: String {
    get { return _STR(mongoc_gridfs_file_get_filename(_fp)) }
  }//end fileName

  public var metaData: BSON {
    get {
      let m = BSON()
      m.doc = unsafeBitCast(mongoc_gridfs_file_get_metadata(_fp), to:UnsafeMutablePointer<bson_t>.self)
      return m
    }//end get
  }//end meta

  private func _download(_ to: UnsafeRawPointer) -> UnsafeRawPointer {
    let pName = unsafeBitCast(to, to: UnsafePointer<Int8>.self)
    let fp = fopen(pName, "wb")
    let stream = mongoc_stream_gridfs_new(_fp)
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
    fclose(fp)
    return to
  }//

  public func download(to: String) throws -> Int {
    let fp = fopen(to, "wb")
    let stream = mongoc_stream_gridfs_new(_fp)
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
    fclose(fp)

    if write_ok {
      return total
    }//end if
    throw MongoClientError.initError("gridfs.download.write(\(to))")
  }//end download

  public func delete() throws {
    if mongoc_gridfs_file_remove(_fp, &error) {
      return
    }//end if
    throw MongoClientError.initError("gridfs.delete() = \(error.code)")
  }
}//end File

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
  public func list() throws -> [GridFile]{
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
    var ret:[GridFile] = []
    var err: MongoClientError? = nil
    repeat {
      file = mongoc_gridfs_file_list_next(list)
      if (file == nil) {
        break
      }//end if
      do {
        let f = try GridFile(file)
        ret.append(f)
      }catch (let e){
        file = nil
        err = MongoClientError.initError("gridfs.list() = \(e)")
      }//end do
    }while(file != nil)
    if (ret.count > 0) {
      mongoc_gridfs_file_list_destroy(list)
    }//end if
    if err != nil {
      throw err!
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

  public func search(name: String) throws -> GridFile {
    return try GridFile(gridFS: handle, from: name)
  }//end search
}//end class

extension MongoClient {
  public func gridFS(db: String, prefix: String? = nil) throws -> GridFS {
    return try GridFS(client: self, db: db, prefix: prefix)
  }//end gridFS
}//end MongoClient


