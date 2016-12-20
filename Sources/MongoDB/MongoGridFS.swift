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

/// private param structure for non-blocking downloads, *NOT FOR API END USERS*
struct _DOWNPARAM{
  /// mongoc file handler
  var file: OpaquePointer
  /// download destination filename in local drive
  var to: String
  /// callback once downloaded with reporting total bytes number as int
  var completion: (Int)->()
}//end DOWNPARAM

/// private param structure for non-blocking uploads, *NOT FOR API END USERS*
struct _UPPARAM{
  /// mongoc gridfs handler
  var fs: OpaquePointer
  /// local file name
  var from: String
  /// remote file name
  var to: String
  /// callback once upload with reporting whether succeed as bool
  var completion: (GridFile?)->()
}//end UPPARAM

#if os(Linux)
  typealias THREADPARAM = UnsafeMutableRawPointer?
#else
  typealias THREADPARAM = UnsafeMutableRawPointer
#endif
typealias THREADPROC = @convention(c) (THREADPARAM) -> UnsafeMutableRawPointer?

/// private thread function for downloading, *NOT FOR API END USERS*
/// - paramters:
///   - pointerParam: pointer of _DOWNPARAM
/// - returns:
/// nil
func _EXEC_DOWNLOAD(_ pointerParam:THREADPARAM) -> UnsafeMutableRawPointer? {
  // convert raw pointer to accessible pointer
  let param = unsafeBitCast(pointerParam, to: UnsafeMutablePointer<_DOWNPARAM>.self)
  // get the param structure
  let p = param.pointee
  // call the real download
  GridFile._download(file: p.file, to:p.to , completion: p.completion)
  // release the param pointer
  param.deallocate(capacity: 1)
  return nil
}//end _EXEC_DOWNLOAD

/// private thread function for uploading, *NOT FOR API END USERS*
/// - paramters:
///   - pointerParam: pointer of _UPPARAM
/// - returns:
/// nil
func _EXEC_UPLOAD(_ pointerParam:THREADPARAM) -> UnsafeMutableRawPointer? {
  // convert raw pointer to accessible pointer
  let param = unsafeBitCast(pointerParam, to: UnsafeMutablePointer<_UPPARAM>.self)
  // get the param structure
  let p = param.pointee
  // call the real upload
  GridFS._upload(fsHandle: p.fs, from: p.from, to: p.to, completion: p.completion)
  // release the param pointer
  param.deallocate(capacity: 1)
  return nil
}//end _EXEC_UPLOAD

/// private bson_value_t converter macro *NOT FOR API END USERS*
/// - parameters:
///   - u: pointer of bson_value_t
/// - returns:
/// string value of the input
func _STR(_ u: UnsafePointer<bson_value_t>) -> String{
  // get the value of bson_value_t
  var v = u.pointee
  // check value type
  if v.value_type == BSON_TYPE_UTF8 {
    // convert the value to a utf pointer
    let p = unsafeBitCast(v.value.v_utf8, to: UnsafePointer<CChar>.self)
    // conver the pointer to a string
    return String.init(cString: p)
  }else {
    // not a string, return empty
    return ""
  }//end if
}//end str

/// private string converter macro *NOT FOR API END USERS*
/// - parameters:
///   - u: pointer of a string
/// returns:
/// string value of the input
func _STR(_ u: UnsafePointer<Int8>) -> String{
  // convert the value to a utf pointer
  let p = unsafeBitCast(u, to: UnsafePointer<CChar>.self)
  // conver the pointer to a string
  return String.init(cString: p)
}//end str

/// a quick dirty macro of get a pointer from a swift string *NOT FOR API END USERS*
func _PTR(_ of: UnsafePointer<Int8>) -> UnsafePointer<Int8> { return of }

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
    guard gridFS != nil else {
      throw MongoClientError.initError("gridfs.file.init(fsHandler) = \(error.code)")
    }//end file
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
    get { return _STR(mongoc_gridfs_file_get_id(_fp)) }
  }//end id

  /// md5 property of GridFile, readonly.
  public var md5: String {
    get {
      // get the bson value of md5
      let m = mongoc_gridfs_file_get_md5(_fp)
      if m == nil {
        return ""
      }//end if
      // return the string value
      return _STR(m!)
    }//end get
    /// LINK BUG TO FIX: unresolved symbol mongoc_gridfs_file_set_md5
    /*
    set {
      mongoc_gridfs_file_set_md5(_fp, md5)
    }//end set
    */
  }//end md5

  /// aliases property of GridFile, readonly.
  public var aliases: BSON {
    get {
      let a = unsafeBitCast(mongoc_gridfs_file_get_aliases(_fp), to:UnsafeMutablePointer<bson_t>.self)
      return BSON(rawBson: a)
    }//end get
    // set { mongoc_gridfs_file_set_aliases(_fp, aliases.doc }
  }//end aliases

  /// content type property of GridFile, readonly.
  public var contentType: String {
    get {
      guard let t = mongoc_gridfs_file_get_content_type(_fp) else {
        return ""
      }
      return _STR(t)
    }
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
    get { return _STR(mongoc_gridfs_file_get_filename(_fp)) }
    // set { mongoc_gridfs_file_set_filename(_fp, fileName) }
  }//end fileName

  /// meta data of the grid file object, in bson format, readonly
  public var metaData: BSON {
    get {
      let m = unsafeBitCast(mongoc_gridfs_file_get_metadata(_fp), to:UnsafeMutablePointer<bson_t>.self)
      return BSON(rawBson: m)
    }//end get
  }//end meta

  /// internal download method, private function. *NOT FOR API END USERS*
  /// - parameters:
  ///   - file: grid file handler, not nullable
  ///   - to: downloading destination file name on local drive
  ///   - completion: callback once downloaded with bytes number as integer
  static func _download(file: OpaquePointer?, to: String, completion:@escaping (Int)->()){
    // open the local file to write in binary
    let fp = fopen(to, "wb")
    // create a new file on gridfs
    let stream = mongoc_stream_gridfs_new(file)
    // check result 
    var r = 0
    // setup the read/write controller
    var iov = mongoc_iovec_t()
    // set transfer buffer to 4k, as default in network traffic
    iov.iov_len = 4096
    iov.iov_base = malloc(iov.iov_len)
    // bytes to go
    var total = 0
    // loop until done
    repeat {
      // read buffer from server
      r = mongoc_stream_readv (stream, &iov, 1, -1, 0)
      if (r > 0) {
        // write to local destination
        let w = fwrite(iov.iov_base, 1, r, fp)
        // caculate the total bytes
        total += w
      }//end if
    // exit loop once done or fault
    }while(r != 0)
    // release buffer
    free(iov.iov_base)
    // close download stream
    mongoc_stream_destroy(stream)
    // close local saving
    fclose(fp)
    // call the callback function
    completion(total)
  }//end inner download

  /// download a file directly. Not suggest because it will block the thread before completion
  /// - parameters:
  ///   - to: the destinated file name on local drive
  public func download(to: String) -> Int {
    var totalBytes = 0
    // call the internal static download method
    GridFile._download(file:_fp, to: to) { totalBytes = $0 }
    return totalBytes
  }//end download

  /// download a file and call the user customized callback function once completed.
  /// - parameters:
  ///   - to: download destination file name on local drive
  ///   - completion: callback function once downloaded. Int is the number downloaded.
  public func download(to: String, completion:@escaping (Int)->()) {
    // prepare a parameter for download procedure
    let param = _DOWNPARAM(file:_fp!, to: to, completion: completion)
    // prepare a pointer to hold these parameters
    let pParam = UnsafeMutablePointer<_DOWNPARAM>.allocate(capacity: 1)
    // memory copy, from the parameter to the pointer
    pParam.initialize(to: param)
    // convert the paramter pointer to a thread param pointer
    let pRaw = unsafeBitCast(pParam, to: UnsafeMutableRawPointer.self)
    // load the thread execution function
    let downloader: THREADPROC = _EXEC_DOWNLOAD
    // prepare the thread handler
    var th = pthread_t.init(bitPattern: 0)
    // call the thread
    let _ = pthread_create(&th, nil, downloader, pRaw)
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

  /// get the current file position
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
  public func partiallyRead(amount: UInt32, timeout:UInt32 = 0) throws -> [UInt8] {
    // prepare a buffer to read
    var iov = mongoc_iovec_t()
    iov.iov_len = Int(amount)
    iov.iov_base = malloc(iov.iov_len)

    // perform a reading
    let res = mongoc_gridfs_file_readv(_fp, &iov, 1, iov.iov_len, timeout)

    // check the reading outcome
    if res < 0 {
      free(iov.iov_base)
      throw MongoClientError.initError("gridfs.file.read(\(amount)) = \(res) in \(timeout) ms")
    }//end if

    // turn the c type buffer to an array
    let p = unsafeBitCast(iov.iov_base, to: UnsafePointer<UInt8>.self)
    let buf = UnsafeBufferPointer<UInt8>(start: p, count: res)
    let a = Array(buf)
    free(iov.iov_base)
    return a
  }//end read

  /// partially write some bytes to the remote file
  /// - parameters:
  ///   - bytes: an array of bytes to write
  ///   - timeout: milliseconds to wait. default 0 to return immediately
  /// - returns:
  /// bytes totally written
  /// - throws:
  /// MongoClientError if failed to read
  public func partiallyWrite(bytes:[UInt8], timeout:UInt32 = 0) throws -> Int {
    // prepare a buffer to write
    let pointer = UnsafeBufferPointer<UInt8>(start: bytes, count: bytes.count)
    var iov = mongoc_iovec_t()
    iov.iov_len = Int(bytes.count)
    iov.iov_base = malloc(iov.iov_len)
    #if os(Linux)
      memcpy(iov.iov_base, pointer.baseAddress!, 8)
    #else
      memcpy(iov.iov_base, pointer.baseAddress, 8)
    #endif

    // perform writing
    let res = mongoc_gridfs_file_writev(_fp, &iov, 1, timeout)

    free(iov.iov_base)
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

  /// internal grid file uploader. *NOT FOR API END USERS*
  /// - parameters:
  ///   - fsHandler: MongoClient gridfs handler
  ///   - from: local file name to upload
  ///   - to: remote file name as expected
  ///   - completion: callback once uploaded with a bool hint for success
  static func _upload(fsHandle: OpaquePointer?, from: String, to: String, completion:@escaping (GridFile?)->()) {
    // validate the gridfs_t
    guard fsHandle != nil else {
      completion(nil)
      return
    }//end guard
    // open a stream for reading
    guard let stream = mongoc_stream_file_new_for_path(from, O_RDONLY, 0) else {
      completion(nil)
      return
    }//end guard
    // set the reading option with local file name to upload
    var opt = mongoc_gridfs_file_opt_t()
    opt.filename = _PTR(to)
    // create remote file handler
    let file = mongoc_gridfs_create_file_from_stream(fsHandle, stream, &opt)
    guard file != nil else {
      completion(nil)
      return
    }//end guard
    #if os(Linux)
      mongoc_gridfs_file_set_filename(file, to)
    #endif
    // upload the file
    let save = mongoc_gridfs_file_save(file)
    if save {
      do {
        // call the callback once uploaded
        let f = try GridFile.init(file)
        completion(f)
      } catch {
        completion(nil)
      }
    }else{
      completion(nil)
    }//end
    // release resources
    // mongoc_gridfs_file_destroy(file)
  }//end _upload

  /// upload a file from local drive to server directly. *NOT SUGGESTED* because it will block the thread.
  /// - parameters:
  ///   - from: local file name to upload
  ///   - to: destinated file name on server
  /// - returns:
  /// true for a successful upload
  public func upload(from: String, to: String) -> GridFile? {
    var file: GridFile?
    // call the static internal upload method
    GridFS._upload(fsHandle: handle, from: from, to: to) { file = $0 }
    // return the file handle
    return file
  }//end upload

  /// upload a file in a non-blocking fashion.
  /// - parameters:
  ///   - from: local file name to upload
  ///   - to: destinated file name on server
  ///   - completion: callback for upload with indicating whether success or not
  public func upload(from: String, to: String, completion:@escaping (GridFile?)->()) {
    // setup a structure to pass the parameters for internal static calling
    let param = _UPPARAM(fs: handle!, from: from, to: to, completion: completion)
    // prepare a pointer to hold these parameters
    let pParam = UnsafeMutablePointer<_UPPARAM>.allocate(capacity: 1)
    // memory copy from the structure to the pointer
    pParam.initialize(to: param)
    // cast the structure pointer to a thread parameter pointer
    let pRaw = unsafeBitCast(pParam, to: UnsafeMutableRawPointer.self)
    // prepare the thread routine
    let uploader: THREADPROC = _EXEC_UPLOAD
    // prepare the thread handler
    #if os(Linux)
      var th = pthread_t()
    #else
      var th = pthread_t.init(bitPattern: 0)
    #endif
    // run the thread
    let _ = pthread_create(&th, nil, uploader, pRaw)
  }//end upload

  /// download a file in a non-blocking fashion
  /// - parameters:
  ///   - from: file name on server
  ///   - to: local path to save the downloaded file
  ///   - completion: callback once done, with a parameter of total bytes
  /// - throws:
  /// MongoClientError if not file found or failed to download
  public func download(from: String, to: String, completion:@escaping (Int)->()) throws {
    // find the file first
    let file = try search(name: from)
    // download it then
    file.download(to: to, completion: completion)
  }//end download

  /// download a file in blocking mode
  /// - parameters:
  ///   - from: file name on server
  ///   - to: local path to save the downloaded file
  /// - throws:
  /// MongoClientError if not file found or failed to download
  public func download(from: String, to: String) throws -> Int {
    // find the file first
    let file = try search(name: from)
    // download it then
    return file.download(to: to)
  }//end download

  /// search for a file on the gridfs
  /// - parameters:
  ///   - name: name of file to find
  /// - returns:
  /// a grid file object if found
  /// - throws:
  /// MongoClientError if failed or not found
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
  public func gridFS(database: String, prefix: String? = nil) throws -> GridFS {
    return try GridFS(client: self, database: database, prefix: prefix)
  }//end gridFS
}//end MongoClient

