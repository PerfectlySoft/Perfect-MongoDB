# Perfect - MongoDB Connector

[![GitHub version](https://badge.fury.io/gh/PerfectlySoft%2FPerfect-MongoDB.svg)](https://badge.fury.io/gh/PerfectlySoft%2FPerfect-MongoDB)

This project provides a Swift wrapper around the mongo-c client library, enabling access to MongoDB servers.

This package builds with Swift Package Manager and is part of the [Perfect](https://github.com/PerfectlySoft/Perfect) project. It was written to be stand-alone and so does not require PerfectLib or any other components.

Ensure you have installed and activated the latest Swift 3.0 tool chain.

## OS X Build Notes

This package requires the [Home Brew](http://brew.sh) build of mongo-c.

To install Home Brew:

```
/usr/bin/ruby -e "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/master/install)"
```

To install mongo-c:

```
brew install mongo-c
```

## Linux Build Notes

Ensure that you have installed libmongoc.

## Building

Add this project as a dependency in your Package.swift file.

```
.Package(url:"https://github.com/PerfectlySoft/Perfect-MongoDB.git", versions: Version(0,0,0)..<Version(10,0,0))
```
