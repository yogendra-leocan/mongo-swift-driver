@testable import MongoSwift
import Nimble
import NIO
import TestsCommon

final class EventLoopBoundMongoClientTests: MongoSwiftTestCase {
    func testEventLoopBoundDb() throws {
        let elg = MultiThreadedEventLoopGroup(numberOfThreads: 4)
        let expectedEventLoop = elg.next()

        try self.withTestClient(eventLoopGroup: elg) { client in
            // TODO: SWIFT-1030 - add tests for database operations that return ChangeStream and MongoCursor
            let elBoundClient = EventLoopBoundMongoClient(client: client, eventLoop: expectedEventLoop)
            let db = elBoundClient.db(Self.testDatabase)
            defer { try? db.drop().wait() }
            expect(db.eventLoop) === expectedEventLoop

            // test the MongoDatabase operations return futures on the expected event loop
            expect(db.collection("test").eventLoop) === expectedEventLoop
            let res1 = db.listCollectionNames()
            expect(res1.eventLoop) === expectedEventLoop
            _ = try res1.wait()

            let res2 = db.runCommand(["insert": "coll", "documents": [["foo": "bar"]]])
            expect(res2.eventLoop) === expectedEventLoop
            _ = try res2.wait()

            let res3 = db.createCollection("test")
            expect(res3.eventLoop) === expectedEventLoop
            // test the returned MongoCollection has the expected event loop
            expect(try res3.wait().eventLoop) === expectedEventLoop

            let res4 = db.listMongoCollections()
            expect(res4.eventLoop) === expectedEventLoop
            let collections = try res4.wait()
            // test the returned MongoCollections have the expected event loop
            for coll in collections {
                expect(coll.eventLoop) === expectedEventLoop
            }
        }
    }

    func testEventLoopBoundCollection() throws {
        let elg = MultiThreadedEventLoopGroup(numberOfThreads: 4)
        let expectedEventLoop = elg.next()

        try self.withTestClient(eventLoopGroup: elg) { client in
            let elBoundClient = EventLoopBoundMongoClient(client: client, eventLoop: expectedEventLoop)

            let db = elBoundClient.db(Self.testDatabase)
            defer { try? db.drop().wait() }
            let coll1 = try db.createCollection(self.getCollectionName(suffix: "1")).wait()

            // test renamed
            let res1 = coll1.renamed(to: self.getCollectionName(suffix: "2"))
            let coll2 = try res1.wait()
            expect(res1.eventLoop) === expectedEventLoop
            expect(coll2.eventLoop) === expectedEventLoop

            // test drop
            let res2 = coll2.drop()
            expect(res2.eventLoop) === expectedEventLoop
            _ = try res2.wait()
        }
    }

    func testEventLoopBoundCollectionReads() throws {
        let elg = MultiThreadedEventLoopGroup(numberOfThreads: 4)
        let expectedEventLoop = elg.next()

        try self.withTestClient(eventLoopGroup: elg) { client in
            // TODO: SWIFT-1030 - add tests for collection operations that return MongoCursor
            let elBoundClient = EventLoopBoundMongoClient(client: client, eventLoop: expectedEventLoop)

            let db = elBoundClient.db(Self.testDatabase)
            defer { try? db.drop().wait() }
            let coll1 = try db.createCollection(self.getCollectionName(suffix: "1")).wait()

            // test countDocuments
            let res1 = coll1.countDocuments()
            expect(res1.eventLoop) === expectedEventLoop
            _ = try res1.wait()

            // test estimatedDocumentCount
            let res2 = coll1.estimatedDocumentCount()
            expect(res2.eventLoop) === expectedEventLoop
            _ = try res2.wait()

            // test distinct
            let res3 = coll1.distinct(fieldName: "foo", filter: [:])
            expect(res3.eventLoop) === expectedEventLoop
            _ = try res3.wait()
        }
    }

    func testEventLoopBoundCollectionIndexes() throws {
        // TODO: SWIFT-1030 - add tests for collection operations that return MongoCursor
        let elg = MultiThreadedEventLoopGroup(numberOfThreads: 4)
        let expectedEventLoop = elg.next()

        try self.withTestClient(eventLoopGroup: elg) { client in
            let elBoundClient = EventLoopBoundMongoClient(client: client, eventLoop: expectedEventLoop)

            let db = elBoundClient.db(Self.testDatabase)
            defer { try? db.drop().wait() }
            let coll = try db.createCollection(self.getCollectionName(suffix: "1")).wait()

            // test createIndex
            let res1 = coll.createIndex(IndexModel(keys: ["y": 1]))
            expect(res1.eventLoop) === expectedEventLoop
            _ = try res1.wait()

            // test createIndexes
            let res2 = coll.createIndexes([IndexModel(keys: ["x": 1])])
            expect(res2.eventLoop) === expectedEventLoop
            _ = try res2.wait()

            // test a createIndexes that returns a failed future
            let res3 = coll.createIndexes([])
            expect(res3.eventLoop) === expectedEventLoop
            expect(try res3.wait()).to(throwError())

            // test listIndexNames
            let res4 = coll.listIndexNames()
            expect(res4.eventLoop) === expectedEventLoop
            _ = try res4.wait()

            // test a dropIndex that returns a failed future
            let res5 = coll.dropIndex("*")
            expect(res5.eventLoop) === expectedEventLoop
            expect(try res5.wait()).to(throwError())

            // test dropIndexes
            let res6 = coll.dropIndexes()
            expect(res6.eventLoop) === expectedEventLoop
            _ = try res6.wait()
        }
    }

    func testEventLoopBoundCollectionFindAndModify() throws {
        let elg = MultiThreadedEventLoopGroup(numberOfThreads: 4)
        let expectedEventLoop = elg.next()

        try self.withTestClient(eventLoopGroup: elg) { client in
            let elBoundClient = EventLoopBoundMongoClient(client: client, eventLoop: expectedEventLoop)

            let db = elBoundClient.db(Self.testDatabase)
            defer { try? db.drop().wait() }
            let coll = try db.createCollection(self.getCollectionName(suffix: "1")).wait()

            _ = try coll.insertOne(["x": 1]).wait()
            let res = coll.findOneAndReplace(filter: ["x": 1], replacement: ["x": 2])
            expect(res.eventLoop) === expectedEventLoop
            _ = try res.wait()
        }
    }

    func testEventLoopBoundCollectionBulkWrite() throws {
        let elg = MultiThreadedEventLoopGroup(numberOfThreads: 4)
        let expectedEventLoop = elg.next()

        try self.withTestClient(eventLoopGroup: elg) { client in
            let elBoundClient = EventLoopBoundMongoClient(client: client, eventLoop: expectedEventLoop)

            let db = elBoundClient.db(Self.testDatabase)
            defer { try? db.drop().wait() }
            let coll = try db.createCollection(self.getCollectionName(suffix: "1")).wait()

            let res1 = coll.bulkWrite([WriteModel.insertOne(["y": 1])])
            expect(res1.eventLoop) === expectedEventLoop
            _ = try res1.wait()
            // test a bulkWrite that returns a failed future
            let res2 = coll.bulkWrite([])
            expect(res2.eventLoop) === expectedEventLoop
            expect(try res2.wait()).to(throwError())
        }
    }
}
