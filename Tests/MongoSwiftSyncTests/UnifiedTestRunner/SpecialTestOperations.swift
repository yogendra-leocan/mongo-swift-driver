// swiftlint:disable duplicate_imports
@testable import class MongoSwift.ClientSession
import MongoSwiftSync
@testable import class MongoSwiftSync.ClientSession
import Nimble
import TestsCommon

struct UnifiedFailPoint: UnifiedOperationProtocol {
    /// The configureFailpoint command to be executed.
    let failPoint: BSONDocument

    /// The client entity to use for setting the failpoint.
    let client: String

    static var knownArguments: Set<String> {
        ["failPoint", "client"]
    }

    func execute(
        on _: UnifiedOperation.Object,
        entities: EntityMap,
        testRunner: UnifiedTestRunner
    ) throws -> UnifiedOperationResult {
        let testClient = try entities.getEntity(id: self.client).asTestClient()
        let opts = RunCommandOptions(readPreference: .primary)
        try testClient.client.db("admin").runCommand(self.failPoint, options: opts)
        testRunner.enabledFailPoints.append((failPoint["configureFailPoint"]!.stringValue!, nil))
        return .none
    }
}

struct UnifiedTargetedFailPoint: UnifiedOperationProtocol {
    /// The configureFailPoint command to be executed.
    let failPoint: BSONDocument

    /// Identifier for the session entity with which to set the fail point.
    let session: String

    static var knownArguments: Set<String> {
        ["failPoint", "session"]
    }

    func execute(
        on _: UnifiedOperation.Object,
        entities: EntityMap,
        testRunner: UnifiedTestRunner
    ) throws -> UnifiedOperationResult {
        let session = try entities.getEntity(id: self.session).asSession()
        // The mongos on which to set the fail point is determined by the session argument (after resolution to a
        // session entity). est runners MUST error if the session is not pinned to a mongos server at the time this
        // operation is executed.
        expect(session.pinnedServerAddress)
            .toNot(beNil(), description: "Session \(self.session) unexpectedly not pinned to a mongos")
        let pinnedMongos = session.pinnedServerAddress!
        // The test runner SHOULD use the client entity associated with the session to execute the configureFailPoint
        // command.
        let client = session.client
        try client.db("admin").runCommand(self.failPoint, on: pinnedMongos)
        testRunner.enabledFailPoints.append((failPoint["configureFailPoint"]!.stringValue!, pinnedMongos))
        return .none
    }
}

struct UnifiedAssertCollectionExists: UnifiedOperationProtocol {
    /// The collection name.
    let collectionName: String

    /// The name of the database
    let databaseName: String

    static var knownArguments: Set<String> {
        ["collectionName", "databaseName"]
    }

    func execute(
        on _: UnifiedOperation.Object,
        entities _: EntityMap,
        testRunner: UnifiedTestRunner
    ) throws -> UnifiedOperationResult {
        let db = testRunner.internalClient.db(self.databaseName)
        expect(try db.listCollectionNames()).to(
            contain(self.collectionName),
            description: "Expected db \(self.databaseName) to contain collection \(self.collectionName)"
        )
        return .none
    }
}

struct UnifiedAssertCollectionNotExists: UnifiedOperationProtocol {
    /// The collection name.
    let collectionName: String

    /// The name of the database.
    let databaseName: String

    static var knownArguments: Set<String> {
        ["collectionName", "databaseName"]
    }

    func execute(
        on _: UnifiedOperation.Object,
        entities _: EntityMap,
        testRunner: UnifiedTestRunner
    ) throws -> UnifiedOperationResult {
        let db = testRunner.internalClient.db(self.databaseName)
        expect(try db.listCollectionNames()).toNot(
            contain(self.collectionName),
            description: "Expected db \(self.databaseName) to not contain collection \(self.collectionName)"
        )
        return .none
    }
}

struct UnifiedAssertIndexExists: UnifiedOperationProtocol {
    /// The collection name.
    let collectionName: String

    /// The name of the database.
    let databaseName: String

    /// The name of the index.
    let indexName: String

    static var knownArguments: Set<String> {
        ["collectionName", "databaseName", "indexName"]
    }

    func execute(
        on _: UnifiedOperation.Object,
        entities _: EntityMap,
        testRunner: UnifiedTestRunner
    ) throws -> UnifiedOperationResult {
        let collection = testRunner.internalClient.db(self.databaseName).collection(self.collectionName)
        expect(try collection.listIndexNames()).to(
            contain(self.indexName),
            description: "Expected collection \(collection.namespace) to contain index \(self.indexName)"
        )
        return .none
    }
}

struct UnifiedAssertIndexNotExists: UnifiedOperationProtocol {
    /// The collection name.
    let collectionName: String

    /// The name of the database to look for the collection in.
    let databaseName: String

    /// The name of the index.
    let indexName: String

    static var knownArguments: Set<String> {
        ["collectionName", "databaseName", "indexName"]
    }

    func execute(
        on _: UnifiedOperation.Object,
        entities _: EntityMap,
        testRunner: UnifiedTestRunner
    ) throws -> UnifiedOperationResult {
        let collection = testRunner.internalClient.db(self.databaseName).collection(self.collectionName)
        expect(try collection.listIndexNames()).toNot(
            contain(self.indexName),
            description: "Expected collection \(collection.namespace) to not contain index \(self.indexName)"
        )
        return .none
    }
}

struct AssertSessionNotDirty: UnifiedOperationProtocol {
    /// The session entity to perform the assertion on.
    let session: String

    static var knownArguments: Set<String> {
        ["session"]
    }

    func execute(
        on _: UnifiedOperation.Object,
        entities _: EntityMap,
        testRunner _: UnifiedTestRunner
    ) throws -> UnifiedOperationResult {
        // TODO: SWIFT-1021: Actually implement this operation when we implement explicit sessions in Swift and can tell
        // whether a session is dirty. For now it is a no-op.
        .none
    }
}

struct AssertSessionDirty: UnifiedOperationProtocol {
    /// The session entity to perform the assertion on.
    let session: String

    static var knownArguments: Set<String> {
        ["session"]
    }

    func execute(
        on _: UnifiedOperation.Object,
        entities _: EntityMap,
        testRunner _: UnifiedTestRunner
    ) throws -> UnifiedOperationResult {
        // TODO: SWIFT-1021: Actually implement this operation when we implement explicit sessions in Swift and can tell
        // whether a session is dirty. For now it is a no-op.
        .none
    }
}

struct UnifiedAssertSessionPinned: UnifiedOperationProtocol {
    /// The session entity to perform the assertion on.
    let session: String

    static var knownArguments: Set<String> {
        ["session"]
    }

    func execute(
        on _: UnifiedOperation.Object,
        entities: EntityMap,
        testRunner _: UnifiedTestRunner
    ) throws -> UnifiedOperationResult {
        let session = try entities.getEntity(id: self.session).asSession()
        expect(session.isPinned).to(beTrue(), description: "Session \(self.session) unexpectedly unpinned")
        return .none
    }
}

struct UnifiedAssertSessionUnpinned: UnifiedOperationProtocol {
    /// The session entity to perform the assertion on.
    let session: String

    static var knownArguments: Set<String> {
        ["session"]
    }

    func execute(
        on _: UnifiedOperation.Object,
        entities: EntityMap,
        testRunner _: UnifiedTestRunner
    ) throws -> UnifiedOperationResult {
        let session = try entities.getEntity(id: self.session).asSession()
        expect(session.isPinned).to(beFalse(), description: "Session \(self.session) unexpectedly pinned")
        return .none
    }
}

struct UnifiedAssertSessionTransactionState: UnifiedOperationProtocol {
    /// The session entity to perform the assertion on.
    let session: String

    /// The expected transaction state.
    let state: MongoSwift.ClientSession.TransactionState

    static var knownArguments: Set<String> {
        ["session", "state"]
    }

    func execute(
        on _: UnifiedOperation.Object,
        entities: EntityMap,
        testRunner _: UnifiedTestRunner
    ) throws -> UnifiedOperationResult {
        let session = try entities.getEntity(id: self.session).asSession()
        let actualState = session.asyncSession.transactionState
        expect(actualState).to(equal(self.state), description: "Session had unexpected transaction state")
        return .none
    }
}

struct AssertDifferentLsidOnLastTwoCommands: UnifiedOperationProtocol {
    /// Identifier for the client to perform the assertion on.
    let client: String

    static var knownArguments: Set<String> {
        ["client"]
    }

    func execute(
        on _: UnifiedOperation.Object,
        entities: EntityMap,
        testRunner _: UnifiedTestRunner
    ) throws -> UnifiedOperationResult {
        let client = try entities.getEntity(id: self.client).asTestClient()
        makeLsidAssertion(client: client, same: false)
        return .none
    }
}

struct AssertSameLsidOnLastTwoCommands: UnifiedOperationProtocol {
    /// Identifier for the client to perform the assertion on.
    let client: String

    static var knownArguments: Set<String> {
        ["client"]
    }

    func execute(
        on _: UnifiedOperation.Object,
        entities: EntityMap,
        testRunner _: UnifiedTestRunner
    ) throws -> UnifiedOperationResult {
        let client = try entities.getEntity(id: self.client).asTestClient()
        makeLsidAssertion(client: client, same: true)
        return .none
    }
}

func makeLsidAssertion(client: UnifiedTestClient, same: Bool) {
    let lastTwoEvents = Array(client.commandMonitor.events.compactMap { $0.commandStartedValue }.suffix(2))
    expect(lastTwoEvents.count).to(equal(2), description: "Expected at least two command started events")

    let command1 = lastTwoEvents[0].command
    let command2 = lastTwoEvents[1].command

    expect(command1["lsid"]).toNot(beNil())
    let lsid1 = command1["lsid"]!

    expect(command2["lsid"]).toNot(beNil())
    let lsid2 = command2["lsid"]!

    if same {
        expect(lsid1).to(equal(lsid2), description: "lsids for last two commands did not match")
    } else {
        expect(lsid1).toNot(equal(lsid2), description: "lsids for last two commands unexpectedly matched")
    }
}
