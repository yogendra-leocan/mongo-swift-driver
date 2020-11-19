@testable import class MongoSwift.ClientSession
import MongoSwiftSync
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
}

struct UnifiedAssertCollectionExists: UnifiedOperationProtocol {
    /// The collection name.
    let collectionName: String

    /// The name of the database
    let databaseName: String

    static var knownArguments: Set<String> {
        ["collectionName", "databaseName"]
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
}

struct AssertSessionNotDirty: UnifiedOperationProtocol {
    /// The session entity to perform the assertion on.
    let session: String

    static var knownArguments: Set<String> {
        ["session"]
    }

    func execute(on object: UnifiedOperation.Object, using entities: [String: Entity]) throws {
        // TODO SWIFT-1021: this is currently a no-op as we don't have access to the underlying server session, it
        // should eventually be filled in when we implement pure Swift sessions.
    }
}

struct AssertSessionDirty: UnifiedOperationProtocol {
    /// The session entity to perform the assertion on.
    let session: String

    static var knownArguments: Set<String> {
        ["session"]
    }

    func execute(on object: UnifiedOperation.Object, using entities: [String: Entity]) throws {
        // TODO SWIFT-1021: this is currently a no-op as we don't have access to the underlying server session, it
        // should eventually be filled in when we implement pure Swift sessions.
    }
}

struct UnifiedAssertSessionPinned: UnifiedOperationProtocol {
    /// The session entity to perform the assertion on.
    let session: String

    static var knownArguments: Set<String> {
        ["session"]
    }
}

struct UnifiedAssertSessionUnpinned: UnifiedOperationProtocol {
    /// The session entity to perform the assertion on.
    let session: String

    static var knownArguments: Set<String> {
        ["session"]
    }
}

struct UnifiedAssertSessionTransactionState: UnifiedOperationProtocol {
    /// The session entity to perform the assertion on.
    let session: String

    /// The expected transaction state.
    let state: ClientSession.TransactionState

    static var knownArguments: Set<String> {
        ["session", "state"]
    }
}

struct AssertDifferentLsidOnLastTwoCommands: UnifiedOperationProtocol {
    /// Identifier for the client to perform the assertion on.
    let client: String

    static var knownArguments: Set<String> {
        ["client"]
    }

    func execute(on object: UnifiedOperation.Object, using entities: [String: Entity]) throws {
        let client = try entities.getEntity(id: self.client).asTestClient()
        try doLsidAssertion(commandEvents: client.commandEvents, same: false)
    }
}

struct AssertSameLsidOnLastTwoCommands: UnifiedOperationProtocol {
    /// Identifier for the client to perform the assertion on.
    let client: String

    static var knownArguments: Set<String> {
        ["client"]
    }

    func execute(on object: UnifiedOperation.Object, using entities: [String: Entity]) throws {
        let client = try entities.getEntity(id: self.client).asTestClient()
        try doLsidAssertion(commandEvents: client.commandEvents, same: true)
    }
}

func doLsidAssertion(commandEvents: [CommandEvent], same: Bool) throws {
    print("command events: \(commandEvents)")
    let commandStarted = commandEvents.compactMap { $0.commandStartedValue }
    guard commandStarted.count >= 2 else {
        throw TestError(
            message: "Unexpectedly found < 2 commandStarted events when performing lsid assertion on last two commands"
        )
    }

    let lastTwo = commandStarted.suffix(2)
    let lsid1 = lastTwo[0].command["lsid"]
    let lsid2 = lastTwo[1].command["lsid"]
    expect(lsid1).toNot(beNil())
    expect(lsid2).toNot(beNil())
    if same {
        expect(lsid1).to(equal(lsid2))
    } else {
         expect(lsid1).toNot(equal(lsid2))
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
}
