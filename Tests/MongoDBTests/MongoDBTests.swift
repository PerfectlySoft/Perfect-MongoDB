//
//  MongoDBTests.swift
//  MongoDBTests
//
//  Created by Kyle Jessup on 2015-11-18.
//  Copyright © 2015 PerfectlySoft. All rights reserved.
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

import Foundation
import XCTest
@testable import MongoDB

class MongoDBTests: XCTestCase {
    
    override func setUp() {
        super.setUp()
        // Put setup code here. This method is called before the invocation of each test method in the class.
    }
    
    override func tearDown() {
        // Put teardown code here. This method is called after the invocation of each test method in the class.
        super.tearDown()
    }
    
    func testBSONFromJSON() {
		let json = "{\"id\":1,\"first_name\":\"Kimberly\",\"last_name\":\"Gonzales\",\"email\":\"kgonzales0@usnews.com\",\"country\":\"France\",\"ip_address\":\"164.55.182.176\",\"ip_address0\":\"Turquoise\",\"ip_address1\":\"Euro\",\"ip_address2\":\"1qttm1nWiNDfpwuaYuoj7S7TXxUWxauBt\",\"ip_address3\":\"Demivee\",\"ip_address4\":false,\"ip_address5\":\"6/27/2015\"}"
		// it adds spaces
		let jsonResult = "{ \"id\" : 1, \"first_name\" : \"Kimberly\", \"last_name\" : \"Gonzales\", \"email\" : \"kgonzales0@usnews.com\", \"country\" : \"France\", \"ip_address\" : \"164.55.182.176\", \"ip_address0\" : \"Turquoise\", \"ip_address1\" : \"Euro\", \"ip_address2\" : \"1qttm1nWiNDfpwuaYuoj7S7TXxUWxauBt\", \"ip_address3\" : \"Demivee\", \"ip_address4\" : false, \"ip_address5\" : \"6\\/27\\/2015\" }"
		do {
			let bson = try BSON(json: json)
			defer {
				bson.close()
			}
			let backToJson = bson.description
			
			XCTAssert(jsonResult == backToJson, backToJson)
		} catch {
			XCTAssert(false, "Exception was thrown \(error)")
		}
    }
	
	func testBSONAppend() {
		let bson = BSON()
		defer {
			bson.close()
		}
		
		XCTAssert(bson.append(key: "stringKey", string: "String Value"))
		XCTAssert(bson.append(key: "intKey", int: 42))
		XCTAssert(bson.append(key: "nullKey"))
		XCTAssert(bson.append(key: "int32Key", int32: 42))
		XCTAssert(bson.append(key: "doubleKey", double: 4.2))
		
		XCTAssert(bson.append(key: "boolKey", bool: true))
		
		let t = time(nil)
		XCTAssert(bson.append(key: "timeKey", time: t))
		XCTAssert(bson.append(key: "dateTimeKey", dateTime: 4200102))
		
		let str = bson.asString
		let expectedJson = "{ \"stringKey\" : \"String Value\", \"intKey\" : 42, \"nullKey\" : null, \"int32Key\" : 42, \"doubleKey\" : 4.2, " +
			"\"boolKey\" : true, \"timeKey\" : { \"$date\" : \(t * 1000) }, \"dateTimeKey\" : { \"$date\" : 4200102 } }"
		
		XCTAssert(str == expectedJson, "\n\(str)\n\(expectedJson)\n")
	}
	
	func testBSONHasFields() {
		let bson = BSON()
		defer {
			bson.close()
		}
		
		XCTAssert(bson.append(key: "stringKey", string: "String Value"))
		XCTAssert(bson.append(key: "intKey", int: 42))
		XCTAssert(bson.append(key: "nullKey"))
		XCTAssert(bson.append(key: "int32Key", int32: 42))
		XCTAssert(bson.append(key: "doubleKey", double: 4.2))
		
		XCTAssert(bson.append(key: "boolKey", bool: true))
		
		let t = time(nil)
		XCTAssert(bson.append(key: "timeKey", time: t))
		XCTAssert(bson.append(key: "dateTimeKey", dateTime: 4200102))
		
		let str = bson.asString
		let expectedJson = "{ \"stringKey\" : \"String Value\", \"intKey\" : 42, \"nullKey\" : null, \"int32Key\" : 42, \"doubleKey\" : 4.2, " +
		"\"boolKey\" : true, \"timeKey\" : { \"$date\" : \(t * 1000) }, \"dateTimeKey\" : { \"$date\" : 4200102 } }"
		
		XCTAssert(str == expectedJson, "\n\(str)\n\(expectedJson)\n")
		
		XCTAssert(bson.countKeys() == 8)
		
		XCTAssert(bson.hasField(key: "nullKey"))
		XCTAssert(bson.hasField(key: "doubleKey"))
		XCTAssert(false == bson.hasField(key: "noKey"))
	}
	
	func testBSONIterate() {
		let bson = BSON()
		defer {
			bson.close()
		}
		
		XCTAssert(bson.append(key: "stringKey", string: "String Value"))
		XCTAssert(bson.append(key: "intKey", int: 42))
		XCTAssert(bson.append(key: "nullKey"))
		XCTAssert(bson.append(key: "int32Key", int32: 42))
		XCTAssert(bson.append(key: "doubleKey", double: 4.2))
		
		XCTAssert(bson.append(key: "boolKey", bool: true))
		
		let t = time(nil)
		XCTAssert(bson.append(key: "timeKey", time: t))
		XCTAssert(bson.append(key: "dateTimeKey", dateTime: 4200102))
		
		let bsonAry = BSON()
		bsonAry.append(key: "0", string: "String Value 1")
		bsonAry.append(key: "1", string: "String Value 2")
		
		XCTAssert(bson.appendArray(key: "arrayKey", array: bsonAry))
		
		bson.append(key: "regexKey", regex: "/[^ ]/c", options: "")
		
		let expectedKeys = ["stringKey", "intKey", "nullKey", "int32Key", "doubleKey",
		                    "boolKey", "timeKey", "dateTimeKey", "arrayKey", "regexKey"]
		do {
			guard var iterator = bson.iterator() else {
				return XCTAssert(false, "nil iterator")
			}
			var keysGen = expectedKeys.makeIterator()
			var valuesDict: [String:BSON.BSONValue] = [:]
			while iterator.next() {
				guard let currentKey = iterator.currentKey else {
					return XCTAssert(false)
				}
				XCTAssert(currentKey == keysGen.next())
				if currentKey == "nullKey" {
					XCTAssert(nil == iterator.currentValue)
				} else if currentKey == "arrayKey" {
					guard var subIt = iterator.currentChildIterator else {
						return XCTAssert(false)
					}
					
					XCTAssert(subIt.next())
					XCTAssert(subIt.currentKey == "0")
					XCTAssert(subIt.next())
					XCTAssert(subIt.currentKey == "1")
					
				} else {
					guard let value = iterator.currentValue else {
						return XCTAssert(false, "No value")
					}
					valuesDict[currentKey] = value
				}
			}
			
			XCTAssert(valuesDict["stringKey"]!.string! == "String Value")
			XCTAssert(valuesDict["intKey"]!.int! == 42)
			XCTAssert(valuesDict["int32Key"]!.int! == 42)
			XCTAssert(valuesDict["doubleKey"]!.double == 4.2)
			XCTAssert(valuesDict["boolKey"]!.bool)
			XCTAssert(time_t(valuesDict["timeKey"]!.int!) == t * 1000)
			XCTAssert(valuesDict["dateTimeKey"]!.int! == 4200102)
			
			XCTAssert(nil == keysGen.next())
		}
		
		do {
			guard var iterator = bson.iterator() else {
				return XCTAssert(false, "nil iterator")
			}
			guard let newIt = iterator.findDescendant(key: "arrayKey.1") else {
				return XCTAssert(false, "nil iterator")
			}
			XCTAssert(newIt.currentValue?.string == "String Value 2")
		}
	}
	
	func testBSONCompare() {
		let bson = BSON()
		defer {
			bson.close()
		}
		
		XCTAssert(bson.append(key: "stringKey", string: "String Value"))
		
		let expectedJson = "{ \"stringKey\" : \"String Value\" }"
		
		let bson2 = try! BSON(json: expectedJson)
		
		let cmp = bson == bson2
		
		XCTAssert(cmp, "\n\(bson.asString)\n\(bson2.asString)\n")
	}
	
	func testClientConnect() {
		let client = try! MongoClient(uri: "mongodb://localhost")
		let status = client.serverStatus()
		switch status {
		case .error(let domain, let code, let message):
			XCTAssert(false, "Error: \(domain) \(code) \(message)")
		case .replyDoc(let doc):
			print("Status doc: \(doc)")
			XCTAssert(true)
		default:
			XCTAssert(false, "Strange reply type \(status)")
		}
	}
	
	func testClientConnectFail() {
		if let _ = try? MongoClient(uri: "mongoib//typo") {
			XCTAssert(false, "client should be nil")
		}
	}
	
	func testClientGetDatabase() {
		let client = try! MongoClient(uri: "mongodb://localhost")
		let db = client.getDatabase(name: "test")
		XCTAssert(db.name() == "test")
		db.close()
		client.close()
	}
	
	func testDBCreateCollection() {
		let client = try! MongoClient(uri: "mongodb://localhost")
		let db = client.getDatabase(name: "test")
		XCTAssert(db.name() == "test")
		
        if let oldC = db.getCollection(name: "testcollection") {
            let _ = oldC.drop()
        }
		
		let result = db.createCollection(name: "testcollection", options: BSON())
		switch result {
		case .replyCollection(let collection):
			XCTAssert(collection.name() == "testcollection")
			
			guard case .replyDoc = collection.validate() else {
				return XCTAssert(false, "Bad validate")
			}
			
			collection.close()
		default:
			XCTAssert(false, "Bad result \(result)")
		}
		db.close()
		client.close()
	}
	
	func testClientGetDatabaseNames() {
		let client = try! MongoClient(uri: "mongodb://localhost")
		let db = client.getDatabase(name: "test")
		XCTAssert(db.name() == "test")
		
		if let oldC = db.getCollection(name: "testcollection") {
			let _ = oldC.drop()
		}
		
        guard let collection = db.getCollection(name: "testcollection") else {
            XCTAssert(false, "Collection was nil")
            return
        }
		XCTAssert(collection.name() == "testcollection")
		
        defer {
            collection.close()
            db.close()
            client.close()
        }
        
		let bson = BSON()
		defer {
			bson.close()
		}
		
		XCTAssert(bson.append(key: "stringKey", string: "String Value"))
		XCTAssert(bson.append(key: "intKey", int: 42))
		XCTAssert(bson.append(key: "nullKey"))
		XCTAssert(bson.append(key: "int32Key", int32: 42))
		XCTAssert(bson.append(key: "doubleKey", double: 4.2))
		XCTAssert(bson.append(key: "boolKey", bool: true))
		
		let result2 = collection.save(document: bson)
		switch result2 {
		case .success:
			XCTAssert(true)
		default:
			XCTAssert(false, "Bad result \(result2)")
		}
		
		let names = client.databaseNames()
		
		XCTAssert(names.contains("test"), "\(names)")
	}
	
	func testGetCollection() {
		let client = try! MongoClient(uri: "mongodb://localhost")
		let db = client.getDatabase(name: "test")
        guard let col = db.getCollection(name: "testcollection") else {
            XCTAssert(false, "Collection was nil")
            return
        }
		XCTAssert(db.name() == "test")
		XCTAssert(col.name() == "testcollection")
		db.close()
		client.close()
	}
	
	func testDeleteDoc() {
		let client = try! MongoClient(uri: "mongodb://localhost")
		let db = client.getDatabase(name: "test")
		XCTAssert(db.name() == "test")
		
        guard let collection = db.getCollection(name: "testcollection") else {
            XCTAssert(false, "Collection was nil")
            return
        }
		XCTAssert(collection.name() == "testcollection")
		
        defer {
            collection.close()
            db.close()
            client.close()
        }
        
		let bson = BSON()
		defer {
			bson.close()
		}
		
		XCTAssert(bson.append(key: "stringKey", string: "String Value"))
		XCTAssert(bson.append(key: "intKey", int: 42))
		XCTAssert(bson.append(key: "nullKey"))
		XCTAssert(bson.append(key: "int32Key", int32: 42))
		XCTAssert(bson.append(key: "doubleKey", double: 4.2))
		XCTAssert(bson.append(key: "boolKey", bool: true))
		
		let result2 = collection.insert(document: bson)
		switch result2 {
		case .success:
			XCTAssert(true)
		default:
			XCTAssert(false, "Bad result \(result2)")
		}
		
		let result3 = collection.remove(selector: bson)
		switch result3 {
		case .success:
			XCTAssert(true)
		default:
			XCTAssert(false, "Bad result \(result2)")
		}
	}
    
    
    
    func testCollectionFind() {
        let client = try! MongoClient(uri: "mongodb://localhost")
        let db = client.getDatabase(name: "test")
        XCTAssert(db.name() == "test")
        
        guard let collection = db.getCollection(name: "testcollection") else {
            XCTAssert(false, "Collection was nil")
            return
        }
        XCTAssert(collection.name() == "testcollection")
        
        defer {
            collection.close()
            db.close()
            client.close()
        }
        
        do {
            let bson = BSON()
            defer {
                bson.close()
            }
            
            XCTAssert(bson.append(key: "stringKey", string: "String Value"))
            XCTAssert(bson.append(key: "intKey", int: 42))
            XCTAssert(bson.append(key: "nullKey"))
            XCTAssert(bson.append(key: "int32Key", int32: 42))
            XCTAssert(bson.append(key: "doubleKey", double: 4.2))
            XCTAssert(bson.append(key: "boolKey", bool: true))
            
            let result2 = collection.save(document: bson)
            switch result2 {
            case .success:
                XCTAssert(true)
            default:
                XCTAssert(false, "Bad result \(result2)")
                return
            }
        }
        
        do {
            let bson = BSON()
            defer {
                bson.close()
            }
            
            XCTAssert(bson.append(key: "stringKey", string: "String Value 2"))
            XCTAssert(bson.append(key: "intKey", int: 43))
            XCTAssert(bson.append(key: "nullKey"))
            XCTAssert(bson.append(key: "int32Key", int32: 43))
            XCTAssert(bson.append(key: "doubleKey", double: 4.3))
            XCTAssert(bson.append(key: "boolKey", bool: false))
            
            let result2 = collection.save(document: bson)
            switch result2 {
            case .success:
                XCTAssert(true)
            default:
                XCTAssert(false, "Bad result \(result2)")
                return
            }
        }
        
        let countResult = collection.count(query: BSON())
        guard case MongoResult.replyInt(let expectedCount) = countResult else {
            XCTAssert(false, "Invalid count response")
            return
        }
        
        guard let fnd = collection.find() else {
            XCTAssert(false, "Cursor was nil")
            return
        }
        
        var looped = 0
        for _ in fnd {
            looped += 1
        }
        
        XCTAssert(looped == expectedCount)
        
        let names = client.databaseNames()
        
        XCTAssert(names.contains("test"), "\(names)")
    }
    
    func testCollectionDistinct() {
        let collectionName = "testdistinctcollection"
        let attributeName = "attribute"
        
        let client = try! MongoClient(uri: "mongodb://localhost")
        let db = client.getDatabase(name: "test")
        XCTAssert(db.name() == "test")
        
        guard let collection = db.getCollection(name: collectionName) else {
            XCTAssert(false, "Collection was nil")
            return
        }
        XCTAssert(collection.name() == collectionName)
        
        defer {
            collection.close()
            db.close()
            client.close()
        }
        
        do {
            let testValues = ["a", "a", "a", "b", "b", "c"]
            for value in testValues {
                let bson = BSON()
                defer {
                    bson.close()
                }
                
                XCTAssert(bson.append(key: attributeName, string: value))
                
                let result2 = collection.save(document: bson)
                switch result2 {
                case .success:
                    XCTAssert(true)
                default:
                    XCTAssert(false, "Bad result \(result2)")
                    return
                }
            }
            
            guard let _ = collection.distinct(key: attributeName) else {
                XCTAssert(false, "Invalid distinct response")
                return
            }
            
/*
 * Unfortunately PerfectLib unavailable
 * imposible to validate distinct result
             
            let expectingValues = Set(testValues)
            let distinctStr = distinct.asString
            
            guard let distinctDict = try! distinctStr.jsonDecode() as? [String:Any] else {
                XCTAssert(false, "Invalid distinct response")
                return
            }
            let distinctValues = Set(distinctDict["values"])
            XCTAssertEqual(expectingValues, distinctValues)
 */
        }
    }

	func testUpdate() {
		let client = try! MongoClient(uri: "mongodb://localhost")
		let db = client.getDatabase(name: "test")
		XCTAssert(db.name() == "test")
		
		if let oldC = db.getCollection(name: "testcollection") {
			let _ = oldC.drop()
		}
		
		let result = db.createCollection(name: "testcollection", options: BSON())
		guard case .replyCollection(let collection) = result else {
			return XCTAssert(false, "Bad result \(result)")
		}
		XCTAssert(collection.name() == "testcollection")
		
		defer {
			collection.close()
			db.close()
			client.close()
		}
		
		do {
			let bson = BSON()
			defer {
				bson.close()
			}
			
			XCTAssert(bson.append(key: "stringKey", string: "String Value"))
			XCTAssert(bson.append(key: "intKey", int: 42))
			XCTAssert(bson.append(key: "nullKey"))
			XCTAssert(bson.append(key: "int32Key", int32: 42))
			XCTAssert(bson.append(key: "doubleKey", double: 4.2))
			XCTAssert(bson.append(key: "boolKey", bool: true))
			
			let result2 = collection.save(document: bson)
			switch result2 {
			case .success:
				XCTAssert(true)
			default:
				XCTAssert(false, "Bad result \(result2)")
				return
			}
		}
		
		let queryBson = BSON()
		queryBson.append(key: "intKey", int32: 42)
		
		let countResult = collection.count(query: queryBson)
		guard case MongoResult.replyInt(let expectedCount) = countResult else {
			return XCTAssert(false, "Invalid count response")
		}
		guard expectedCount == 1 else {
			return XCTAssert(false, "Invalid count response")
		}
		
		guard let fnd = collection.find(query: queryBson),
			let foundBson = fnd.next(),
			var bsonIt = foundBson.iterator() else {
			return XCTAssert(false, "Cursor was nil")
		}
		
		guard bsonIt.find(key: "_id") else {
			return XCTAssert(false, "No document _id")
		}
		guard let oid = bsonIt.currentValue?.oid else {
			return XCTAssert(false, "No document _id")
		}
		
		do {
			let query = BSON()
			query.append(oid: oid)
			let newBson = BSON()
			let inner = BSON()
			inner.append(key: "intKey", int: 44)
			newBson.append(key: "$set", document: inner)
			
			guard case .success = collection.update(selector: query, update: newBson) else {
				return XCTAssert(false)
			}
		}
		
		do {
			guard let cursor = collection.find(),
				let bson = cursor.next(),
				var it = bson.iterator(),
				it.find(key: "intKey") else {
				return XCTAssert(false)
			}
			XCTAssert(it.currentValue?.int == 44)
		}
		
	}

  func testGridfs() {
    let client = try! MongoClient(uri: "mongodb://localhost")
    var gridfs: GridFS
    do {
      gridfs = try client.gridFS(database: "test")
    }catch(let err) {
      XCTFail("gridfs open: \(err)")
      return
    }

    defer {
      gridfs.close()
    }//end defer

    do {
      let a = try gridfs.list()
      a.forEach { file in
        try! file.delete()
      }//next
      XCTAssertGreaterThanOrEqual(a.count, 0)
    }catch (let err) {
      XCTFail("gridfs list: \(err)")
    }

    let now = String(format:"%2X", time(nil))
    let local = "/tmp/gridfsTest\(now).dat"
    let sz = 134217728 // 128MB
    let buffer = malloc(sz)
    let fd = fopen(local, "wb")
    fwrite(buffer, sz, 1, fd)
    fclose(fd)
    free(buffer)
    let remote = "uploadTest\(now).dat"

    let bol = gridfs.upload(from: local, to: remote)
    if bol {
      print("sync uploading succeed")
    }else{
      print("sync uploading failed")
    }//end if
    XCTAssertTrue(bol)

    do {
      let f = try gridfs.search(name: remote)
      try f.delete()
    }catch(let err) {
      XCTFail("gridfs.sync.upload.delete failed = \(err)")
    }

    let exp1 = self.expectation(description: "async uploading")
    gridfs.upload(from: local, to: remote) { success in
      unlink(local)
      XCTAssertTrue(success)
      exp1.fulfill()
    }//end upload

    self.waitForExpectations(timeout: 10) {
      error in
      if let error = error {
        XCTFail("gridfs async upload: \(error.localizedDescription)")
      }//end if
    }//end wait
    
    do {
      let a = try gridfs.list()
      print(a)
      XCTAssertGreaterThan(a.count, 0)
    }catch (let err) {
      XCTFail("gridfs list: \(err)")
    }


    var f: GridFile? = nil
    do {
      f = try gridfs.search(name: remote)
      print(f?.id ?? "")
      print(f?.fileName ?? "")
      print(f?.contentType ?? "")
      print(f?.md5 ?? "")
      print(f?.metaData ?? "")
      print(f?.uploadDate ?? 0)
      XCTAssertEqual(f?.fileName, remote)
      XCTAssertEqual(f?.length, Int64(sz))

      let pos = f?.tell()
      print(pos ?? 0)
      let mb = 1048576
      try f?.seek(cursor: Int64(mb))
      let bytes = try f?.partiallyRead(amount: UInt32(mb))
      XCTAssertEqual(bytes?.count, mb)
      let sz = try f?.partiallyWrite(bytes: bytes!)
      XCTAssertEqual(sz, mb)
      try f?.seek(cursor: 0)
    }catch(let err){
      XCTFail("gridfs search: \(err)")
    }//end f

    let downloaded = "/tmp/gridfsdownload.bin"

    let exp2 = self.expectation(description: "async downloading")
    f?.download(to: downloaded) { total in
      unlink(downloaded)
      XCTAssertEqual(total, sz)
      exp2.fulfill()
    }//end download

    self.waitForExpectations(timeout: 10) {
      error in
      if let error = error {
        XCTFail("gridfs async download: \(error.localizedDescription)")
      }//end if
    }//end wait
    
    do {
      try f?.delete()
    }catch(let err){
      XCTFail("gridfs delete: \(err)")
    }
    f?.close()
  }

}

extension MongoDBTests {
    static var allTests : [(String, (MongoDBTests) -> () throws -> ())] {
        return [
            ("testBSONFromJSON", testBSONFromJSON),
            ("testBSONAppend", testBSONAppend),
            ("testBSONHasFields", testBSONHasFields),
            ("testBSONIterate", testBSONIterate),
            ("testBSONCompare", testBSONCompare),
            ("testClientConnect", testClientConnect),
            ("testClientConnectFail", testClientConnectFail),
            ("testClientGetDatabase", testClientGetDatabase),
            ("testDBCreateCollection", testDBCreateCollection),
            ("testClientGetDatabaseNames", testClientGetDatabaseNames),
            ("testGetCollection", testGetCollection),
            ("testDeleteDoc", testDeleteDoc),
            ("testCollectionFind", testCollectionFind),
            ("testCollectionDistinct", testCollectionDistinct),
            ("testGridfs", testGridfs)
        ]
    }
}

extension BSON {
	var oid: OID? {
		guard var it = self.iterator(),
			it.find(key: "_id") else {
			return nil
		}
		return it.currentValue?.oid
	}
}









