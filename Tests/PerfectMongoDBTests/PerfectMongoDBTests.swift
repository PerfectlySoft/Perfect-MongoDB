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
		let client = try! MongoClient(uri: "mongodb://localhost")
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

	func testGridFS() {
		let client = try! MongoClient(uri: "mongodb://localhost")
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
		var fp = fopen("/tmp/secret.txt", "wb")
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
		fp = fopen("/tmp/secret2.txt", "rb")
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
		fp = fopen(local, "wb")
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
				fp = fopen("/tmp/\(toUpload)", "wb")
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
        
        let client = try! MongoClient(uri: "mongodb://localhost")
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
}

extension PerfectMongoDBTests {
    static var allTests : [(String, (PerfectMongoDBTests) -> () throws -> ())] {
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
            ("testGridFS", testGridFS),
            ("testNewObjectIdGeneration", testNewObjectIdGeneration)
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









