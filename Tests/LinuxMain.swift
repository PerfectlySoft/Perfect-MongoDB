import XCTest

import MongoDBTestSuite

var tests = [XCTestCaseEntry]()
tests += MongoDBTestSuite.allTests()
XCTMain(tests)
