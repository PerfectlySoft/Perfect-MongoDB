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

/// File class of GridFS
/// - Author:
/// Rockford Wei
public class GridFile {

  /// inner pointer of gridfs file handle
  private var _fp: OpaquePointer?

  /// error info for internal usage
  var error = bson_error_t()

  /// constructor of GridFile class object
  /// - parameters:
  ///   - from: a mongoc_gridfs_file_t for handle the file, not nullable
  /// - throws:
  /// MongoClientError
  public init(_ from: OpaquePointer?) throws {
    // validate the imported file handler
    guard from != nil else {
      throw MongoClientError.initError("gridfs.file.init(nil)")
    }//end guard
    // store the file handler
    _fp = from
  }//end init

  /// deconstructor of GridFile class object
  deinit {
    close()
  }//end deinit

  /// constructor of a GridFile class object
  /// - parameters:
  ///   - gridFS: mongo_gridfs_t for gridfs handle, not nullable
  ///   - from: a mongoc_gridfs_file_t for handle the file, not nullable
  /// - throws:
  /// MongoClientError
  public init(gridFS:OpaquePointer?, from: String ) throws {
    // validate pointer first
    guard gridFS != nil else {
      throw MongoClientError.initError("gridfs.file.init(fsHandler) = \(error.code)")
    }//end file
    // search for the file and turn it into a file handle
    guard let file = mongoc_gridfs_find_one_by_filename(gridFS, from, &error) else{
      throw MongoClientError.initError("gridfs.file.init(\(from)) = \(error.code)")
    }//end file
    _fp = file
  }//end init

  /// destructor of a GridFile class object
  /// defer it as the most preferable practice
  public func close() {
    if _fp == nil {
      return
    }//end if
    mongoc_gridfs_file_destroy(_fp)
    _fp = nil
  }//end close

  /// id (oid) property of GridFile, readonly.
  public var id: String {
    // property get
    get {
      // get the pointer
      let p = mongoc_gridfs_file_get_id(_fp)
      // validate the pointer
      guard p != nil else { return "" }
      // get the value structure
      let ID = BSON.BSONValue(value: p!)
      // return the final value
      return ID?.string ?? ""
    }//end get
  }//end id

  /// md5 property of GridFile, readonly.
  public var md5: String {
    // property get
    get {
      // get the pointer
      let s = mongoc_gridfs_file_get_md5(_fp)
      // validate the pointer
      guard s != nil else { return "" }
      // return the final value
      return String(cString: unsafeBitCast(s, to: UnsafePointer<CChar>.self))
    }//end get
  }//end md5

  /// aliases property of GridFile, readonly.
  public var aliases: BSON {
    // property get
    get {
      // get the pointer first
      let b = mongoc_gridfs_file_get_aliases(_fp)
      // validate the pointer
      guard b != nil else { return BSON() }
      // then turn the pointer into acceptable bson_t pointer
      let a = unsafeBitCast(b, to:UnsafeMutablePointer<bson_t>.self)
      // return the result
      return BSON(rawBson: a)
    }//end get
  }//end aliases

  /// content type property of GridFile, readonly.
  public var contentType: String {
    // property get
    get {
      // get the pointer
      let s = mongoc_gridfs_file_get_content_type(_fp)
      // validate the pointer
      guard s != nil else { return "" }
      // return the final value
      return String(cString: unsafeBitCast(s, to: UnsafePointer<CChar>.self))
    }//end get
  }//end contentType

  /// length (in bytes) property of GridFile, readonly.
  public var length: Int64 {
    get { return mongoc_gridfs_file_get_length(_fp) }
  }//end length

  /// upload date property of GridFile, in unix epoch time format, readonly.
  public var uploadDate: Int64 {
    get { return mongoc_gridfs_file_get_upload_date(_fp) }
  }//end uploadDate

  /// name of the grid file object, readonly
  public var fileName: String {
    // property get
    get {
      // get the pointer
      let s = mongoc_gridfs_file_get_filename(_fp)
      // validate the pointer
      guard s != nil else { return "" }
      // return the final value
      return String(cString: unsafeBitCast(s, to: UnsafePointer<CChar>.self))
    }//end get
  }//end fileName

  /// meta data of the grid file object, in bson format, readonly
  public var metaData: BSON {
    // property get
    get {
      // get the pointer first
      let b = mongoc_gridfs_file_get_metadata(_fp)
      // validate the pointer
      guard b != nil else { return BSON() }
      // then turn the pointer into acceptable bson_t pointer
      let a = unsafeBitCast(b, to:UnsafeMutablePointer<bson_t>.self)
      // return the result
      return BSON(rawBson: a)
    }//end get
  }//end meta

  /// download a file.
  /// - parameters:
  ///   - to: the destinated file name on local drive
  /// - throws:
  /// MongoClientError if failed to write
  /// - returns:
  /// Bytes that written
  @discardableResult
  public func download(to: String) throws -> Int {
    // open the local file to write in binary
    let fp = fopen(to, "wb")
    // create a new file on gridfs
    let stream = mongoc_stream_gridfs_new(_fp)
    // check result
    var r = 0
    // setup the read/write controller
    var iov = mongoc_iovec_t()
    // set transfer buffer to 4k, as default in network traffic
    iov.iov_len = 4096
    // safely alloc a well managed 4k buffer without worrying about GC
    var bytes = [UInt8](repeating:0, count: iov.iov_len)
    // assign the buffer to iov structur
    let _ = bytes.withUnsafeMutableBufferPointer {
      iov.iov_base = unsafeBitCast($0.baseAddress, to: UnsafeMutableRawPointer.self)
    }//end pointer
    // bytes to go
    var total = 0
    // verify the R/W operation
    var good = true
    // loop until done
    repeat {
      // read buffer from server
      r = mongoc_stream_readv (stream, &iov, 1, -1, 0)
      if (r > 0) {
        // write to local destination
        let w = fwrite(iov.iov_base, 1, r, fp)
        // test writing
        good = w == r
        // caculate the total bytes
        total += w
      }//end if
      // exit loop once done or fault
    }while(r != 0 && good)
    // close download stream
    mongoc_stream_destroy(stream)
    // close local saving
    fclose(fp)
    // if nothing wrong
    if good { return total }
    // otherwise throw out an error
    throw MongoClientError.initError("gridfs.file.write(\(to)) failed")
  }//end download

  /// Offset measurement reference for seek() method
  public enum Whence {
    // offset from starting point of file
    case begin
    // offset from current file cursor
    case current
    // offset from the last byte of the file
    case end
  }//end whence

  /// get the current file cursor position
  /// - returns
  /// UInt64 stands for the current file cursor positon
  @discardableResult
  public func tell() -> UInt64 {
    return mongoc_gridfs_file_tell(_fp)
  }//end tell

  /// set the current file position
  /// - parameters:
  ///   - cursor: new position
  ///   - whence: whence of new position, i.e., file begin, current or end of file.
  /// - throws
  /// MongoClientError if failed to seek
  public func seek(cursor: Int64, whence:Whence = .begin) throws {
    var w = Int32(0)
    switch whence {
    case .begin:
      w = Int32(SEEK_SET)
    case .end:
      w = Int32(SEEK_END)
    default:
      w = Int32(SEEK_CUR)
    }//end case
    let res = mongoc_gridfs_file_seek(_fp, cursor, Int32(w))
    if res == 0 {
      return
    }//end if
    throw MongoClientError.initError("gridfs.file.seek(\(cursor)) failed")
  }//end seek

  /// partially read some bytes from the remote file
  /// - parameters:
  ///   - amount: bytes count to read
  ///   - timeout: milliseconds to wait. default 0 to return immediately
  /// - returns:
  /// an array of bytes as outcome
  /// - throws:
  /// MongoClientError if failed to read
  @discardableResult
  public func partiallyRead(amount: UInt32, timeout:UInt32 = 0) throws -> [UInt8] {
    // prepare a buffer to read
    var iov = mongoc_iovec_t()
    iov.iov_len = Int(amount)
    // safely alloc a well managed buffer without worrying about GC
    var bytes = [UInt8](repeating:0, count: iov.iov_len)
    // assign the buffer to iov structur
    let _ = bytes.withUnsafeMutableBufferPointer {
      iov.iov_base = unsafeBitCast($0.baseAddress, to: UnsafeMutableRawPointer.self)
    }//end assign pointer
    // perform a reading
    let res = mongoc_gridfs_file_readv(_fp, &iov, 1, iov.iov_len, timeout)
    // check the reading outcome
    if res < 0 {
      throw MongoClientError.initError("gridfs.file.read(\(amount)) = \(res) in \(timeout) ms")
    }//end if
    return bytes
  }//end read

  /// partially write some bytes to the remote file
  /// - parameters:
  ///   - bytes: an array of bytes to write
  ///   - timeout: milliseconds to wait. default 0 to return immediately
  /// - returns:
  /// bytes totally written
  /// - throws:
  /// MongoClientError if failed to read
  @discardableResult
  public func partiallyWrite(bytes:[UInt8], timeout:UInt32 = 0) throws -> Int {
    var iov = mongoc_iovec_t()
    iov.iov_len = Int(bytes.count)
    // assign the buffer to iov structure
    let _ = bytes.withUnsafeBufferPointer {
      iov.iov_base = unsafeBitCast($0.baseAddress, to: UnsafeMutableRawPointer.self)
    }//end assign pointer
    // perform writing
    let res = mongoc_gridfs_file_writev(_fp, &iov, 1, timeout)
    // check the writing outcome
    if res < 0 {
      throw MongoClientError.initError("gridfs.file.write(\(bytes.count)) = \(res) in \(timeout) ms")
    }//end if
    return res
  }//end read

  /// remove the file from server
  /// - throws:
  /// MongoClientError
  public func delete() throws {
    if mongoc_gridfs_file_remove(_fp, &error) {
      return
    }//end if
    throw MongoClientError.initError("gridfs.delete() = \(error.code)")
  }//end delete
}//end File


/// GridFS class for MongoDB
/// - Author:
/// Rockford Wei
public class GridFS {

  /// mongoc_gridfs_t for handle the api
  private var handle: OpaquePointer?

  /// error structure for internal usage
  var error = bson_error_t()

  /// constructor of gridfs
  /// - parameters:
  ///   - client: MongoClient
  ///   - database: database name of gridfs
  ///   - prefix: prefix of the file system
  /// - throws:
  ///	MongoClientError, if failed to get the expected handle
  public init(client: MongoClient, database: String, prefix: String? = nil) throws {
    /// get gridfs handle from a mongo client
    handle = mongoc_client_get_gridfs(client.ptr, database, prefix, &error)
    guard handle != nil else {
      throw MongoClientError.initError("gridfs.init() = [\(error.code), \(error.domain)]")
    }//end guard
  }//end init

  /// destructor of gridfs
  deinit {
    close()
  }//end deinit

  /// destuctor of gridfs, a defer is suggested to use this method.
  public func close() {
    if handle == nil {
      return
    }//end if
    mongoc_gridfs_destroy(handle)
    handle = nil
  }//end close

  /// list all files on the gridfs
  /// - parameters:
  ///   - filter: a bson to determine which kind of files and how to list, such as order by upload date, or by size. nil for all files.
  /// - throws:
  ///	MongoClientError if failed
  /// - returns:
  /// [GridFile]: array to hold a list of GridFile objects
  @discardableResult
  public func list(filter: BSON? = nil) throws -> [GridFile]{
    // query content
    var query = bson_t()
    // context element for building the query
    var child = bson_t()
    // start a new query
    bson_init(&query)
    bson_init(&child)
    // declare to list all files in an alphabetic order
    bson_append_document_begin (&query, "$orderby", -1, &child);
    bson_append_int32 (&child, "filename", -1, 1);
    bson_append_document_end (&query, &child);
    bson_append_document_begin (&query, "$query", -1, &child);
    bson_append_document_end (&query, &child);

    // perform actually query
    var plist: OpaquePointer?
    if filter == nil {
      plist = mongoc_gridfs_find(handle, &query)
    }else {
      plist = mongoc_gridfs_find(handle, filter?.doc)
    }//end if

    // release the query resource
    bson_destroy(&child)
    bson_destroy(&query)

    guard plist != nil else {
      throw MongoClientError.initError("gridfs.list()")
    }//end guard

    // iterate the query result
    // handler of each file in the list
    var file: OpaquePointer?
    // prepare an empty array to hold all files
    var ret:[GridFile] = []
    // prepare an error holder
    var err: MongoClientError? = nil
    // iterate the query result
    repeat {
      // retrieve the next element from the query list
      file = mongoc_gridfs_file_list_next(plist)
      if (file == nil) {
        break
      }//end if
      // construct a grid file object from the mongoc_grid_file_t pointer
      do {
        let f = try GridFile(file)
        // add the new file object to the array
        ret.append(f)
      }catch (let e){
        // if anything wrong, terminate the loop by setting the next element to a nil pointer
        file = nil
        // and declare an error
        err = MongoClientError.initError("gridfs.list() = \(e)")
      }//end do
      // loop until out of elements
    }while(file != nil)
    // release the mongoc_gridfs_file_list
    mongoc_gridfs_file_list_destroy(plist)
    // if there is an error, throw it out.
    if err != nil {
      throw err!
    }//end if
    // if not, safely return
    return ret
  }//end list

  /// grid file uploader. 
  /// NOTE:for macOS, mongoc library MUST fix the mongoc-gridfs-file.h line 34-41 and add BSON_API to the file_set methods
  /// - parameters:
  ///   - from: local file name to upload, string
  ///   - to: remote file name as expected, string
  ///   - contentType: content type of the file as a string, optional. Default is "text/plain"
  ///   - md5: MD5 hash of the file as a string, optional.
  ///   - metaData: meta data of the file in BSON format, optiona.
  ///   - aliases: aliases of the file in BSON format, optional.
  @discardableResult
  public func upload(from: String, to: String, contentType:String = "text/plain", md5:String = "", metaData: BSON? = nil, aliases:BSON? = nil) throws -> GridFile {
    // open a stream for reading
    guard let stream = mongoc_stream_file_new_for_path(from, O_RDONLY, 0) else {
      throw MongoClientError.initError("gridfs.upload(\(from)): file is not readable")
    }//end guard
    // set the reading option with local file name to upload
    var opt = mongoc_gridfs_file_opt_t()
    to.withCString { opt.filename = $0 }
    if !contentType.isEmpty {
      let _ = contentType.withCString { opt.content_type = $0 }
    }//end if
    if !md5.isEmpty {
      let _ = md5.withCString{ opt.md5 = $0 }
    }//end if
    if metaData != nil {
      opt.metadata = unsafeBitCast(metaData?.doc, to: UnsafePointer<bson_t>.self)
    }//end if
    if aliases != nil {
      opt.aliases = unsafeBitCast(aliases?.doc, to: UnsafePointer<bson_t>.self)
    }//end if
    // create remote file handler
    let file = mongoc_gridfs_create_file_from_stream(handle, stream, &opt)
    guard file != nil else {
      throw MongoClientError.initError("gridfs.upload(\(from)): destination \(to) failed to create")
    }//end guard
    mongoc_gridfs_file_set_filename(file, to)
    if !contentType.isEmpty {
      mongoc_gridfs_file_set_content_type(file, contentType)
    }//end if
    if !md5.isEmpty {
      mongoc_gridfs_file_set_md5(file, md5)
    }//end if
    if metaData != nil {
      mongoc_gridfs_file_set_metadata(file, metaData?.doc ?? nil)
    }//end if
    if aliases != nil {
      mongoc_gridfs_file_set_aliases(file, aliases?.doc ?? nil)
    }//end if
    // upload the file
    let save = mongoc_gridfs_file_save(file)
    if save {
      return try GridFile(file)
    }else{
      mongoc_gridfs_file_destroy(file)
      throw MongoClientError.initError("gridfs.upload(\(from)): destination \(to) failed to save")
    }//end
  }//end _upload

  /// download a file by its name on server
  /// - parameters:
  ///   - from: file name on server
  ///   - to: local path to save the downloaded file
  /// - throws:
  /// MongoClientError if not file found or failed to download
  /// - returns:
  /// bytes that downloaded
  @discardableResult
  public func download(from: String, to: String) throws -> Int{
    // find the file first
    let file = try search(name: from)
    // download it then
    return try file.download(to: to)
  }//end download

  /// search for a file on the gridfs
  /// - parameters:
  ///   - name: name of file to find
  /// - returns:
  /// a grid file object if found
  /// - throws:
  /// MongoClientError if failed or not found
  @discardableResult
  public func search(name: String) throws -> GridFile {
    return try GridFile(gridFS: handle, from: name)
  }//end search

  /// delete a file from the server
  /// - parameters:
  ///   - name: name of the file to delete
  /// - throws:
  /// MongoClientError if failed or not found
  public func delete(name: String) throws {
    // find the file first
    let file = try search(name: name)
    try file.delete()
  }//end delete

  /// Requests that an entire GridFS be dropped, including all files associated with
  /// - throws:
  /// MongoClientError if failed
  public func drop() throws {
    if mongoc_gridfs_drop(handle, &error) {
      return
    }//end if
    throw MongoClientError.initError("gridfs.drop() = \(error.code)")
  }//end drop
}//end class

extension MongoClient {
  /// an express way of calling gridfs. Please note to defer a close() immediately.
  /// parameters:
  /// - database: database name of the gridfs
  /// - prefix: name of starting with of the gridfs, nullable
  /// returns:
  /// - gridfs handle if success
  /// throws:
  /// - MongoClientError if failed to open such a handle
  @discardableResult
  public func gridFS(database: String, prefix: String? = nil) throws -> GridFS {
    return try GridFS(client: self, database: database, prefix: prefix)
  }//end gridFS
}//end MongoClient

