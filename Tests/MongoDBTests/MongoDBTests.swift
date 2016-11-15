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
		
		XCTAssert(names == ["test"])
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
        
        guard let fnd = collection.find(query: BSON()) else {
            XCTAssert(false, "Cursor was nil")
            return
        }
        
        var looped = 0
        for _ in fnd {
            looped += 1
        }
        
        XCTAssert(looped == expectedCount)
        
        let names = client.databaseNames()
        
        XCTAssert(names == ["test"])
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
}

extension MongoDBTests {
    static var allTests : [(String, (MongoDBTests) -> () throws -> ())] {
        return [
            ("testBSONFromJSON", testBSONFromJSON),
            ("testBSONAppend", testBSONAppend),
            ("testBSONHasFields", testBSONHasFields),
            ("testBSONCompare", testBSONCompare),
            ("testClientConnect", testClientConnect),
            ("testClientConnectFail", testClientConnectFail),
            ("testClientGetDatabase", testClientGetDatabase),
            ("testDBCreateCollection", testDBCreateCollection),
            ("testClientGetDatabaseNames", testClientGetDatabaseNames),
            ("testGetCollection", testGetCollection),
            ("testDeleteDoc", testDeleteDoc),
            ("testCollectionFind", testCollectionFind),
            ("testCollectionDistinct", testCollectionDistinct)
        ]
    }
}











