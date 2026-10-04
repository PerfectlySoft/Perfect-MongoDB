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
@testable import PerfectMongoDB

/// Server for the tests; set MONGODB_URI to use one other than localhost.
let testURI = ProcessInfo.processInfo.environment["MONGODB_URI"] ?? "mongodb://localhost"

class PerfectMongoDBTests: XCTestCase {
    func testBSONFromJSON() {
		let json = "{\"id\":1,\"first_name\":\"Kimberly\",\"last_name\":\"Gonzales\",\"email\":\"kgonzales0@usnews.com\",\"country\":\"France\",\"ip_address\":\"164.55.182.176\",\"ip_address0\":\"Turquoise\",\"ip_address1\":\"Euro\",\"ip_address2\":\"1qttm1nWiNDfpwuaYuoj7S7TXxUWxauBt\",\"ip_address3\":\"Demivee\",\"ip_address4\":false,\"ip_address5\":\"6/27/2015\"}"
		// it adds spaces
		let jsonResult = "{ \"id\" : 1, \"first_name\" : \"Kimberly\", \"last_name\" : \"Gonzales\", \"email\" : \"kgonzales0@usnews.com\", \"country\" : \"France\", \"ip_address\" : \"164.55.182.176\", \"ip_address0\" : \"Turquoise\", \"ip_address1\" : \"Euro\", \"ip_address2\" : \"1qttm1nWiNDfpwuaYuoj7S7TXxUWxauBt\", \"ip_address3\" : \"Demivee\", \"ip_address4\" : false, \"ip_address5\" : \"6/27/2015\" }"
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
		XCTAssert(bson.append(key: "doubleKey", double: 4.0))
		
		XCTAssert(bson.append(key: "boolKey", bool: true))
		
		let t = time(nil)
		XCTAssert(bson.append(key: "timeKey", time: t))
		XCTAssert(bson.append(key: "dateTimeKey", dateTime: 4200102))
		
		let str = bson.asString
		let expectedJson = "{ \"stringKey\" : \"String Value\", \"intKey\" : 42, \"nullKey\" : null, \"int32Key\" : 42, \"doubleKey\" : 4.0, " +
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
		XCTAssert(bson.append(key: "doubleKey", double: 4.0))
		
		XCTAssert(bson.append(key: "boolKey", bool: true))
		
		let t = time(nil)
		XCTAssert(bson.append(key: "timeKey", time: t))
		XCTAssert(bson.append(key: "dateTimeKey", dateTime: 4200102))
		
		let str = bson.asString
		let expectedJson = "{ \"stringKey\" : \"String Value\", \"intKey\" : 42, \"nullKey\" : null, \"int32Key\" : 42, \"doubleKey\" : 4.0, " +
		"\"boolKey\" : true, \"timeKey\" : { \"$date\" : \(t * 1000) }, \"dateTimeKey\" : { \"$date\" : 4200102 } }"
		
		XCTAssert(str == expectedJson, "\n\(str)\n\(expectedJson)\n")
		
		XCTAssert(bson.countKeys() == 8)
		
		XCTAssert(bson.hasField(key: "nullKey"))
		XCTAssert(bson.hasField(key: "doubleKey"))
		XCTAssert(false == bson.hasField(key: "noKey"))
	}
	
	func testBSONIterate() {
		let t = time(nil)
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
		do {
			XCTAssert(bson.append(key: "timeKey", time: t))
			XCTAssert(bson.append(key: "dateTimeKey", dateTime: 4200102))
			let bsonAry = BSON()
			bsonAry.append(key: "0", string: "String Value 1")
			bsonAry.append(key: "1", string: "String Value 2")
			XCTAssert(bson.appendArray(key: "arrayKey", array: bsonAry))
		}
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
					XCTAssert(.array == iterator.currentType)
					XCTAssert(nil != iterator.currentValue)
					guard var subIt = iterator.currentChildIterator else {
						return XCTAssert(false)
					}
					XCTAssert(subIt.next())
					XCTAssert(subIt.currentKey == "0")
					XCTAssert(subIt.currentValue?.string == "String Value 1")
					XCTAssert(subIt.next())
					XCTAssert(subIt.currentKey == "1")
					XCTAssert(subIt.currentValue?.string == "String Value 2")
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
	
	func testBSONIterate2() {
		let src = "{\"name\": \"TestName\", \"cars\":[{\"brand\": \"BMW\", \"model\": \"320d\"},{\"brand\": \"Volvo\", \"model\": \"XC90\"}]}"
		do {
			let bson = try BSON(json: src)
			
			guard var iter = bson.iterator() else {
				return XCTAssert(false)
			}
			
			while iter.next() {
				guard let key = iter.currentKey else {
					return XCTAssert(false)
				}
				guard let type = iter.currentType else {
					return XCTAssert(false)
				}
				if case .array = type {
					guard key == "cars" else {
						return XCTAssert(false)
					}
					guard var subIt = iter.currentChildIterator else {
						return XCTAssert(false)
					}
					while subIt.next() {
						guard let type = subIt.currentType else {
							return XCTAssert(false)
						}
						guard case .document = type else {
							return XCTAssert(false)
						}
						guard let value = subIt.currentValue else {
							return XCTAssert(false)
						}
						guard let _ = value.doc else {
							return XCTAssert(false)
						}
						guard var subSubIt = subIt.currentChildIterator else {
							return XCTAssert(false)
						}
						while subSubIt.next() {
							guard let _ = subSubIt.currentKey,
								let _ = subSubIt.currentValue else {
									return XCTAssert(false)
							}
						}
					}
				}
			}
		} catch {
			return XCTAssert(false)
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
		let client = try! MongoClient(uri: testURI)
		let status = client.serverStatus()
		switch status {
		case .error(let domain, let code, let message):
			XCTAssert(false, "Error: \(domain) \(code) \(message)")
		case .replyDoc(_):
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
		let client = try! MongoClient(uri: testURI)
		let db = client.getDatabase(name: "test")
		XCTAssert(db.name() == "test")
		db.close()
		client.close()
	}
	
	func testDBCreateCollection() {
		let client = try! MongoClient(uri: testURI)
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
		let client = try! MongoClient(uri: testURI)
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
		let client = try! MongoClient(uri: testURI)
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
		let client = try! MongoClient(uri: testURI)
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
        let client = try! MongoClient(uri: testURI)
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
        
        let client = try! MongoClient(uri: testURI)
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
		let client = try! MongoClient(uri: testURI)
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

	func testGridFS() {
		let client = try! MongoClient(uri: testURI)
		var gridfs: GridFS
		do {
			gridfs = try client.gridFS(database: "test")
		} catch {
			XCTFail("gridfs open: \(error)")
			return
		}
		
		defer {
			gridfs.close()
		}
		
		// test list / delete
		do {
			let a = try gridfs.list()
			XCTAssertGreaterThanOrEqual(a.count, 0)
			try a.forEach { try $0.delete() }
		} catch {
			XCTFail("gridfs list / delete: \(error)")
		}
		
		let secret:[UInt8] = [65, 66, 67, 68, 0] // "ABCD\0"
		var fp = fopen("/tmp/secret.txt", "wb")!
		fwrite(secret, 1, secret.count, fp)
		fclose(fp)
		
		// test upload / download / properties
		do {
			let f = try gridfs.upload(from: "/tmp/secret.txt", to: "secret.txt", md5: "abcd1234")
			XCTAssertNotNil(f)
			XCTAssertEqual(f.contentType, "text/plain")
			XCTAssertEqual(f.md5, "abcd1234")
			XCTAssertEqual(f.length, Int64(secret.count))
			let dl = try f.download(to: "/tmp/secret2.txt")
			XCTAssertEqual(dl, secret.count)
		} catch {
			XCTFail("gridfs.upload / download mismatched = \(error)")
		}
		
		var secret2:[UInt8] = [0, 0, 0, 0, 0]
		fp = fopen("/tmp/secret2.txt", "rb")!
		let _ = secret2.withUnsafeMutableBufferPointer{ p in
			fread(p.baseAddress, 1, 5, fp)
		}
		fclose(fp)
		for i in 0...4 {
			XCTAssertEqual(secret[i], secret2[i])
		}//next i
		
		unlink("/tmp/secret.txt")
		unlink("/tmp/secret2.txt")
		
		
		// test big file upload
		let local = "/tmp/base128.dat"
		let sz = 134217728 / 3 // 128MB / 3
		let buffer = [UInt8](repeating: 66, count:sz)
		fp = fopen(local, "wb")!
		fwrite(buffer, 1, sz, fp)
		fclose(fp)
		let remote = "base128.dat"
		
		do {
			let f = try gridfs.upload(from: local, to: remote)
			XCTAssertNotNil(f)
			f.close()
		} catch {
			XCTFail("gridfs.upload failed = \(error)")
		}
		unlink(local)
		
		// test search partially read / write
		do {
			let f = try gridfs.search(name: remote)
			let mb = 1048576 / 3
			try f.seek(cursor: Int64(mb))
			let bytes = try f.partiallyRead(amount: UInt32(mb))
			XCTAssertEqual(bytes.count, mb)
			bytes.forEach { XCTAssertEqual($0, 66) }
			try f.seek(cursor: Int64(mb * 10))
			let buf = [UInt8](repeating: 67, count: mb)
			let sz = try f.partiallyWrite(bytes: buf)
			XCTAssertEqual(sz, mb)
			try f.seek(cursor: Int64(mb * 10))
			let buf2 = try f.partiallyRead(amount: UInt32(mb))
			buf2.forEach{ XCTAssertEqual($0, 67) }
			try f.delete()
		} catch {
			XCTFail("gridfs partially read / write: \(error)")
		}
		
		// test leaky
		do {
			for i in 0...20 {
				let toUpload = "upload\(i).bin"
				fp = fopen("/tmp/\(toUpload)", "wb")!
				fwrite(buffer, 1, sz, fp)
				fclose(fp)
				let fx = try gridfs.upload(from: "/tmp/\(toUpload)", to: toUpload)
				XCTAssertNotNil(fx)
				let dl = try fx.download(to: "/tmp/download\(i).bin")
				XCTAssertEqual(dl, sz)
			}
		} catch {
			XCTFail("gridfs leaky: \(error)")
		}
		
		// clean up
		do {
			let a = try gridfs.list()
			try a.forEach { file in
				try file.delete()
			}
		} catch {
			XCTFail("gridfs list: \(error)")
		}
	}

    func testAggregate() {
        let groupsCollectionName = "testaggregate.groups"
        let usersCollectionName = "testaggregate.users"
        
        let client = try! MongoClient(uri: testURI)
        let db = client.getDatabase(name: "test")
        XCTAssert(db.name() == "test")
        
        guard let groupsCollection = db.getCollection(name: groupsCollectionName) else {
            XCTAssert(false, "Collection was nil")
            return
        }
        
        guard let usersCollection = db.getCollection(name: usersCollectionName) else {
            XCTAssert(false, "Collection was nil")
            return
        }
        
        defer {
            groupsCollection.close()
            usersCollection.close()
            db.close()
            client.close()
        }
        
        func saveObjects(collection: MongoCollection, jsons: [String]) throws {
            for json in jsons {
                let doc = try BSON(json: json)
                defer {
                    doc.close()
                }
                
                let result = collection.save(document: doc)
                switch result {
                case .success:
                    XCTAssert(true)
                default:
                    XCTAssert(false, "Bad result \(result)")
                    return
                }
            }
        }
        
        func removeTestData() {
            let query = BSON()
            defer {
                query.close()
            }
            
            _ = groupsCollection.remove(selector: query)
            _ = usersCollection.remove(selector: query)
        }
        
        removeTestData()
        
        do {
            let groupsJSONs = ["{\"_id\": {\"$oid\": \"587caeae7fe1c5580570ed71\"}, \"name\": \"First\"}",
                               "{\"_id\": {\"$oid\": \"587caeae7fe1c5580570ed72\"}, \"name\": \"Second\"}",
                               "{\"_id\": {\"$oid\": \"587caeae7fe1c5580570ed73\"}, \"name\": \"Third\"}",]
            
            let usersJSONs = ["{\"groupId\": {\"$oid\": \"587caeae7fe1c5580570ed71\"}, \"name\": \"John\"}",
                                "{\"groupId\": {\"$oid\": \"587caeae7fe1c5580570ed71\"}, \"name\": \"Peter\"}",
                                "{\"groupId\": {\"$oid\": \"587caeae7fe1c5580570ed72\"}, \"name\": \"Dmitry\"}",]
            

            try saveObjects(collection: groupsCollection, jsons: groupsJSONs)
            try saveObjects(collection: usersCollection, jsons: usersJSONs)
            
            let piplineJSON = "[{ \"$lookup\": { \"from\": \"\(usersCollectionName)\", \"localField\": \"_id\", \"foreignField\": \"groupId\", \"as\": \"users\" }}, {\"$project\": { \"id\" : true, \"count\": { \"$size\": \"$users\" }}}]"
            
            let pipline = try BSON(json: piplineJSON)
            defer { pipline.close() }
            
            let res = groupsCollection.aggregate(pipeline: pipline, flags: MongoQueryFlag.none, options: nil)
            
            guard let safeRes = res else {
                XCTAssertNotNil(res)
                return
            }
            
            let expectations = ["587caeae7fe1c5580570ed71": 2,
                                "587caeae7fe1c5580570ed72": 1,
                                "587caeae7fe1c5580570ed73": 0]
        
            while let doc = safeRes.next(){
                guard let oid = doc.oid else {
                    XCTFail("OID not found")
                    return
                }
                
                guard var it = doc.iterator(), it.find(key: "count") else {
                    XCTFail("Count not found")
                    return
                }
                
                let count = it.currentValue?.int
                
                XCTAssertEqual(expectations[oid.description], count)
            }
            
            removeTestData()
        } catch {
            XCTAssertNil(error)
        }
    }

    func testNewObjectIdGeneration() {
        let objectId = BSON.OID.newObjectId()
        XCTAssertTrue(objectId.count == 24, "Should generate valid ObjectId")
    }

	// Returns the client too: a MongoCollection doesn't keep its client alive.
	private func freshCollection(_ name: String) -> (MongoClient, MongoCollection) {
		let client = try! MongoClient(uri: testURI)
		let db = client.getDatabase(name: "test")
		if let old = db.getCollection(name: name) {
			_ = old.drop()
		}
		guard case .replyCollection(let collection) = db.createCollection(name: name, options: nil) else {
			fatalError("could not create \(name)")
		}
		for a in 1...3 {
			let doc = BSON()
			doc.append(key: "a", int: a)
			doc.append(key: "b", string: "value \(a)")
			guard case .success = collection.insert(document: doc) else {
				fatalError("insert failed")
			}
		}
		return (client, collection)
	}

	func testFindLegacyQueryModifiers() {
		let (client, collection) = freshCollection("testlegacyfind")
		defer { withExtendedLifetime(client) {} }
		defer { _ = collection.drop() }

		let orderby = BSON()
		orderby.append(key: "a", int: -1)
		let query = BSON()
		query.append(key: "$query", document: BSON())
		query.append(key: "$orderby", document: orderby)
		let fields = BSON()
		fields.append(key: "b", int: 0)

		guard let cursor = collection.find(query: query, fields: fields, skip: 1, limit: 1) else {
			return XCTFail("find returned nil")
		}
		let docs = cursor.map { $0.asString }
		XCTAssertEqual(docs.count, 1)
		XCTAssert(docs.first?.contains("\"a\" : 2") == true, "\(docs)")
		XCTAssert(docs.first?.contains("\"b\"") == false, "projection not applied: \(docs)")
	}

	func testCreateIndexAndStats() {
		let (client, collection) = freshCollection("testindexstats")
		defer { withExtendedLifetime(client) {} }
		defer { _ = collection.drop() }

		let keys = BSON()
		keys.append(key: "a", int: 1)
		guard case .success = collection.createIndex(keys: keys, options: MongoIndexOptions(unique: true)) else {
			return XCTFail("createIndex failed")
		}
		let duplicate = BSON()
		duplicate.append(key: "a", int: 1)
		guard case .error = collection.insert(document: duplicate) else {
			return XCTFail("unique index not enforced")
		}
		guard case .success = collection.dropIndex(name: "a_1") else {
			return XCTFail("generated index name should be a_1")
		}

		guard case .replyDoc(let stats) = collection.stats(options: BSON()) else {
			return XCTFail("stats failed")
		}
		XCTAssert(stats.asString.contains("\"count\" : 3"), stats.asString)
	}

	private func count(_ collection: MongoCollection, _ json: String = "{}") -> Int {
		guard case .replyInt(let n) = collection.count(query: try! BSON(json: json)) else {
			XCTFail("count failed")
			return -1
		}
		return n
	}

	func testUpdateForms() {
		let (client, collection) = freshCollection("testupdateforms")
		defer { withExtendedLifetime(client) {} }
		defer { _ = collection.drop() }

		// update-operator document, single
		guard case .success = collection.update(selector: try! BSON(json: "{}"), update: try! BSON(json: "{\"$set\": {\"c\": 1}}")) else {
			return XCTFail("update one failed")
		}
		XCTAssertEqual(count(collection, "{\"c\": 1}"), 1)
		// update-operator document, multi
		guard case .success = collection.update(selector: try! BSON(json: "{}"), update: try! BSON(json: "{\"$set\": {\"c\": 2}}"), flag: .multiUpdate) else {
			return XCTFail("update many failed")
		}
		XCTAssertEqual(count(collection, "{\"c\": 2}"), 3)
		// replacement document
		guard case .success = collection.update(selector: try! BSON(json: "{\"a\": 1}"), update: try! BSON(json: "{\"a\": 1, \"replaced\": true}")) else {
			return XCTFail("replace failed")
		}
		XCTAssertEqual(count(collection, "{\"replaced\": true, \"c\": {\"$exists\": false}}"), 1)
		// upsert
		guard case .success = collection.update(selector: try! BSON(json: "{\"a\": 9}"), update: try! BSON(json: "{\"$set\": {\"b\": \"new\"}}"), flag: .upsert) else {
			return XCTFail("upsert failed")
		}
		XCTAssertEqual(count(collection), 4)
		// remove single, then all matching
		guard case .success = collection.remove(selector: try! BSON(json: "{\"c\": 2}"), flag: .singleRemove) else {
			return XCTFail("remove one failed")
		}
		XCTAssertEqual(count(collection, "{\"c\": 2}"), 1)
		guard case .success = collection.remove(selector: try! BSON(json: "{}")) else {
			return XCTFail("remove all failed")
		}
		XCTAssertEqual(count(collection), 0)
	}

	private func assertCountFails(_ collection: MongoCollection, _ json: String, containing text: String? = nil, file: StaticString = #filePath, line: UInt = #line) {
		switch collection.count(query: try! BSON(json: json)) {
		case .error(_, _, let message):
			if let text {
				XCTAssert(message.contains(text), message, file: file, line: line)
			}
		case let other:
			XCTFail("count(\(json)) should fail, got \(other)", file: file, line: line)
		}
	}

	func testInvalidLegacyQueriesAreRejected() {
		let (client, collection) = freshCollection("testinvalidlegacy")
		defer { withExtendedLifetime(client) {} }
		defer { _ = collection.drop() }

		// A $query that isn't a document used to match every document.
		let notDocument = "{\"$query\": 5}"
		XCTAssertNil(collection.find(query: try! BSON(json: notDocument)))
		assertCountFails(collection, notDocument, containing: "$query must be a document")

		// A non-$ field next to $query used to be merged into the filter.
		let mixed = "{\"$query\": {\"a\": 1}, \"b\": \"value 1\"}"
		XCTAssertNil(collection.find(query: try! BSON(json: mixed)))
		assertCountFails(collection, mixed, containing: "Cannot mix $query with non-dollar field 'b'")

		let gridfs = try! client.gridFS(database: "test")
		defer { gridfs.close() }
		XCTAssertThrowsError(try gridfs.list(filter: try! BSON(json: notDocument)))

		// Well-formed legacy queries still work.
		let valid = "{\"$query\": {\"a\": {\"$gte\": 2}}, \"$orderby\": {\"a\": 1}}"
		XCTAssertEqual(collection.find(query: try! BSON(json: valid))?.map { $0 }.count, 2)
		XCTAssertEqual(count(collection, valid), 2)
	}

	func testLegacyCountOptions() {
		let (client, collection) = freshCollection("testlegacycount")
		defer { withExtendedLifetime(client) {} }
		defer { _ = collection.drop() }

		// A negative legacy limit counts like a positive one.
		if case .replyInt(let limited) = collection.count(query: BSON(), limit: -2) {
			XCTAssertEqual(limited, 2)
		} else {
			XCTFail("count with a negative limit failed")
		}

		// hint reaches the server: an unknown index is an error.
		assertCountFails(collection, "{\"$query\": {}, \"$hint\": \"no_such_index\"}")
		XCTAssertEqual(count(collection, "{\"$query\": {}, \"$hint\": {\"_id\": 1}}"), 3)

		// maxTimeMS reaches the server: a negative value is rejected.
		assertCountFails(collection, "{\"$query\": {}, \"$maxTimeMS\": -1}")

		// collation reaches the server: case-insensitive match.
		XCTAssertEqual(count(collection, "{\"$query\": {\"b\": \"VALUE 1\"}}"), 0)
		XCTAssertEqual(count(collection, "{\"$query\": {\"b\": \"VALUE 1\"}, \"$collation\": {\"locale\": \"en\", \"strength\": 2}}"), 1)

		// Duplicate $query: the last one wins, as in libmongoc 1.x.
		XCTAssertEqual(count(collection, "{\"$query\": {\"a\": 1}, \"$query\": {\"a\": 2}}"), 1)
		XCTAssertEqual(count(collection, "{\"$query\": {\"a\": 1}, \"$query\": {\"b\": \"value 2\"}}"), 1)

		// Int.min has no absolute value; it's rejected before llabs.
		if case .error(_, _, let message) = collection.count(query: BSON(), limit: Int.min) {
			XCTAssert(message.contains("INT64_MIN"), message)
		} else {
			XCTFail("count with limit Int.min should fail")
		}
	}

	func testLegacyCountComment() throws {
		// Checked through the profiler, in a database of its own so the test's profiling
		// level and profile entries don't touch the shared test database.
		let client = try MongoClient(uri: testURI)
		let db = client.getDatabase(name: "perfect_test_count_comment")
		defer { _ = db.drop() }
		guard case .replyCollection(let collection) = db.createCollection(name: "c", options: nil) else {
			return XCTFail("could not create collection")
		}
		guard case .success = collection.insert(document: try BSON(json: "{\"a\": 1}")) else {
			return XCTFail("insert failed")
		}
		guard case .replyDoc = collection.runCommand(try BSON(json: "{\"profile\": 2}")) else {
			throw XCTSkip("profiling isn't available on this server")
		}
		defer { _ = collection.runCommand(try! BSON(json: "{\"profile\": 0}")) }
		let comment = "perfect-count-\(UUID().uuidString)"
		XCTAssertEqual(count(collection, "{\"$query\": {}, \"$comment\": \"\(comment)\"}"), 1)
		let profile = client.getCollection(databaseName: "perfect_test_count_comment", collectionName: "system.profile")
		XCTAssertEqual(profile.find(query: try BSON(json: "{\"command.comment\": \"\(comment)\"}"))?.map { $0 }.count, 1)
	}

	func testMultiUpdateWithReplacementIsRejected() {
		let (client, collection) = freshCollection("testmultireplace")
		defer { withExtendedLifetime(client) {} }
		defer { _ = collection.drop() }

		guard case .error(_, _, let message) = collection.update(selector: try! BSON(json: "{}"), update: try! BSON(json: "{\"replaced\": true}"), flag: .multiUpdate) else {
			return XCTFail("multi update with a replacement document succeeded")
		}
		XCTAssert(message.contains("replacement"), message)
		XCTAssertEqual(count(collection, "{\"replaced\": true}"), 0)
		XCTAssertEqual(count(collection, "{\"a\": {\"$exists\": true}}"), 3)
	}

	func testBulkWrites() {
		let (client, collection) = freshCollection("testbulkwrites")
		defer { withExtendedLifetime(client) {} }
		defer { _ = collection.drop() }

		let docs = (4...6).map { try! BSON(json: "{\"a\": \($0)}") }
		guard case .success = collection.insert(documents: docs) else {
			return XCTFail("bulk insert failed")
		}
		XCTAssertEqual(count(collection), 6)
		let updates: [(selector: BSON, update: BSON)] = [
			(try! BSON(json: "{\"a\": {\"$gt\": 3}}"), try! BSON(json: "{\"$set\": {\"bulk\": true}}")),
			(try! BSON(json: "{\"a\": 1}"), try! BSON(json: "{\"a\": 1, \"swapped\": true}"))
		]
		guard case .success = collection.update(updates: updates) else {
			return XCTFail("bulk update failed")
		}
		XCTAssertEqual(count(collection, "{\"bulk\": true}"), 3)
		XCTAssertEqual(count(collection, "{\"swapped\": true}"), 1)
	}

	func testFindAndModify() {
		let (client, collection) = freshCollection("testfindandmodify")
		defer { withExtendedLifetime(client) {} }
		defer { _ = collection.drop() }

		guard case .replyDoc(let updated) = collection.findAndModify(query: try! BSON(json: "{\"a\": 2}"), sort: nil, update: try! BSON(json: "{\"$inc\": {\"a\": 10}}"), fields: nil, remove: false, upsert: false, new: true) else {
			return XCTFail("findAndModify update failed")
		}
		XCTAssert(updated.asString.contains("\"a\" : 12"), updated.asString)

		guard case .replyDoc(let removed) = collection.findAndModify(query: nil, sort: try! BSON(json: "{\"a\": -1}"), update: nil, fields: nil, remove: true, upsert: false, new: false) else {
			return XCTFail("findAndModify remove failed")
		}
		XCTAssert(removed.asString.contains("\"a\" : 12"), removed.asString)
		XCTAssertEqual(count(collection), 2)
	}

	func testClientPool() {
		let pool = MongoClientPool(uri: testURI)
		guard let first = pool.tryPopClient() else {
			return XCTFail("tryPopClient returned nil")
		}
		guard case .replyDoc = first.serverStatus() else {
			return XCTFail("pooled client unusable")
		}
		pool.pushClient(first)
		do {
			// released without pushClient: goes back to the pool instead of being destroyed
			let dropped = pool.popClient()
			XCTAssertEqual(dropped.databaseNames().isEmpty, false)
		}
		var ran = false
		pool.executeBlock { client in
			if case .replyDoc = client.serverStatus() {
				ran = true
			}
		}
		XCTAssert(ran)
	}

	func testCollectionOutlivesClientVariable() {
		func makeCollection() -> MongoCollection {
			let client = try! MongoClient(uri: testURI)
			return client.getDatabase(name: "test").getCollection(name: "testoutlives")!
		}
		let collection = makeCollection()
		defer { _ = collection.drop() }
		guard case .success = collection.insert(document: try! BSON(json: "{\"a\": 1}")) else {
			return XCTFail("insert through orphaned collection failed")
		}
		XCTAssertEqual(collection.find()?.map { $0 }.count, 1)
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









