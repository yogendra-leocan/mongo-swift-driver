import Foundation
import MongoSwiftSync
import TestsCommon

/// Protocol which all operations supported by the unified test runner conform to.
protocol UnifiedOperationProtocol: Decodable {
    /// Set of supported arguments for the operation.
    static var knownArguments: Set<String> { get }

    func execute(on object: UnifiedOperation.Object, using entities: [String: Entity]) throws
}

extension UnifiedOperationProtocol {
    func execute(on object: UnifiedOperation.Object, using entities: [String: Entity]) throws {
        throw TestError(message: "execute unimplemented for self \(self)")
    }
}

struct UnifiedOperation: Decodable {
    /// Represents an object on which to perform an operation.
    enum Object: RawRepresentable, Decodable {
        /// Used for special test operations.
        case testRunner
        /// An entity name e.g. "client0".
        case entity(String)

        public var rawValue: String {
            switch self {
            case .testRunner:
                return "testRunner"
            case let .entity(s):
                return s
            }
        }

        public init(rawValue: String) {
            switch rawValue {
            case "testRunner":
                self = .testRunner
            default:
                self = .entity(rawValue)
            }
        }

        func asEntityId() throws -> String {
            guard case let .entity(str) = self else {
                throw TestError(message: "Expected Object to be an entity, but got \(self)")
            }
            return str
        }
    }

    /// Object on which to perform the operation.
    let object: Object

    /// Specific operation to execute.
    let operation: UnifiedOperationProtocol

    /// Expected result of the operation.
    let result: UnifiedOperationResult?
    
    func execute(using entities: [String: Entity]) throws {
        try self.operation.execute(on: object, using: entities)
    }

    private enum CodingKeys: String, CodingKey {
        case name, object, arguments
    }

    public init(from decoder: Decoder) throws {
        let container = try decoder.container(keyedBy: CodingKeys.self)

        let name = try container.decode(String.self, forKey: .name)
        switch name {
        case "abortTransaction":
            self.operation = UnifiedAbortTransaction()
        case "aggregate":
            self.operation = try container.decode(UnifiedAggregate.self, forKey: .arguments)
        case "assertCollectionExists":
            self.operation = try container.decode(UnifiedAssertCollectionExists.self, forKey: .arguments)
        case "assertCollectionNotExists":
            self.operation = try container.decode(UnifiedAssertCollectionNotExists.self, forKey: .arguments)
        case "assertIndexExists":
            self.operation = try container.decode(UnifiedAssertIndexExists.self, forKey: .arguments)
        case "assertIndexNotExists":
            self.operation = try container.decode(UnifiedAssertIndexNotExists.self, forKey: .arguments)
        case "assertDifferentLsidOnLastTwoCommands":
            self.operation = try container.decode(AssertDifferentLsidOnLastTwoCommands.self, forKey: .arguments)
        case "assertSameLsidOnLastTwoCommands":
            self.operation = try container.decode(AssertSameLsidOnLastTwoCommands.self, forKey: .arguments)
        case "assertSessionDirty":
            self.operation = try container.decode(AssertSessionDirty.self, forKey: .arguments)
        case "assertSessionNotDirty":
            self.operation = try container.decode(AssertSessionNotDirty.self, forKey: .arguments)
        case "assertSessionPinned":
            self.operation = try container.decode(UnifiedAssertSessionPinned.self, forKey: .arguments)
        case "assertSessionUnpinned":
            self.operation = try container.decode(UnifiedAssertSessionUnpinned.self, forKey: .arguments)
        case "assertSessionTransactionState":
            self.operation = try container.decode(UnifiedAssertSessionTransactionState.self, forKey: .arguments)
        case "bulkWrite":
            self.operation = try container.decode(UnifiedBulkWrite.self, forKey: .arguments)
        case "commitTransaction":
            self.operation = UnifiedCommitTransaction()
        case "createChangeStream":
            self.operation = try container.decode(CreateChangeStream.self, forKey: .arguments)
        case "createCollection":
            self.operation = try container.decode(UnifiedCreateCollection.self, forKey: .arguments)
        case "createIndex":
            self.operation = try container.decode(UnifiedCreateIndex.self, forKey: .arguments)
        case "deleteOne":
            self.operation = try container.decode(UnifiedDeleteOne.self, forKey: .arguments)
        case "dropCollection":
            self.operation = try container.decode(UnifiedDropCollection.self, forKey: .arguments)
        case "endSession":
            self.operation = EndSession()
        case "find":
            self.operation = try container.decode(UnifiedFind.self, forKey: .arguments)
        case "findOneAndReplace":
            self.operation = try container.decode(UnifiedFindOneAndReplace.self, forKey: .arguments)
        case "findOneAndUpdate":
            self.operation = try container.decode(UnifiedFindOneAndUpdate.self, forKey: .arguments)
        case "failPoint":
            self.operation = try container.decode(UnifiedFailPoint.self, forKey: .arguments)
        case "insertOne":
            self.operation = try container.decode(UnifiedInsertOne.self, forKey: .arguments)
        case "insertMany":
            self.operation = try container.decode(UnifiedInsertMany.self, forKey: .arguments)
        case "iterateUntilDocumentOrError":
            self.operation = IterateUntilDocumentOrError()
        case "listDatabases":
            self.operation = UnifiedListDatabases()
        case "replaceOne":
            self.operation = try container.decode(UnifiedReplaceOne.self, forKey: .arguments)
        case "startTransaction":
            self.operation = UnifiedStartTransaction()
        case "targetedFailPoint":
            self.operation = try container.decode(UnifiedTargetedFailPoint.self, forKey: .arguments)
        // GridFS ops
        case "delete", "download", "upload":
            self.operation = Placeholder()
        // convenient txn API
        case "withTransaction":
            self.operation = Placeholder()
        default:
            throw TestError(message: "unrecognized operation name \(name)")
        }

        if type(of: self.operation) != Placeholder.self,
           let rawArgs = try container.decodeIfPresent(BSONDocument.self, forKey: .arguments)?.keys
        {
            let knownArgsForType = type(of: self.operation).knownArguments
            for arg in rawArgs {
                guard knownArgsForType.contains(arg) else {
                    throw TestError(
                        message: "Unrecognized argument \"\(arg)\" for operation type \"\(type(of: self.operation))\""
                    )
                }
            }
        }

        self.object = try container.decode(Object.self, forKey: .object)

        let singleContainer = try decoder.singleValueContainer()
        let result = try singleContainer.decode(UnifiedOperationResult.self)
        guard !result.isEmpty else {
            self.result = nil
            return
        }

        self.result = result
    }
}

/// Placeholder for an unsupported operation.
struct Placeholder: UnifiedOperationProtocol {
    static var knownArguments: Set<String> { [] }
}

/// Represents the expected result of an operation.
enum UnifiedOperationResult: Decodable {
    /// One or more assertions for an error expected to be raised by the operation.
    case error(ExpectedError)
    /// - result: A value corresponding to the expected result of the operation.
    /// - saveAsEntity: If specified, the actual result returned by the operation (if any) will be saved with this
    ///       name in the Entity Map.
    // TODO: SWIFT-913: consider using custom type to represent results
    case result(result: BSON?, saveAsEntity: String?)

    private enum CodingKeys: String, CodingKey {
        case expectError, expectResult, saveResultAsEntity
    }

    init(from decoder: Decoder) throws {
        let container = try decoder.container(keyedBy: CodingKeys.self)

        if let expectError = try container.decodeIfPresent(ExpectedError.self, forKey: .expectError) {
            self = .error(expectError)
            return
        }

        let expectResult = try container.decodeIfPresent(BSON.self, forKey: .expectResult)
        let saveAsEntity = try container.decodeIfPresent(String.self, forKey: .saveResultAsEntity)
        self = .result(result: expectResult, saveAsEntity: saveAsEntity)
    }

    /// If none of the fields are present we currently end up with an empty object. This allows us to check easily that
    /// there are not actually any result assertions to be made.
    var isEmpty: Bool {
        guard case let .result(result, save) = self else {
            return false
        }
        return result == nil && save == nil
    }
}

/// One or more assertions for an error/exception, which is expected to be raised by an executed operation.
struct ExpectedError: Decodable {
    /// If true, the test runner MUST assert that an error was raised. This is primarily used when no other error
    /// assertions apply but the test still needs to assert an expected error.
    let isError: Bool?

    /// When true, indicates that the error originated from the client. When false, indicates that the error
    /// originated from a server response.
    let isClientError: Bool?

    /// A substring of the expected error message (e.g. "errmsg" field in a server error document).
    let errorContains: String?

    /// The expected "code" field in the server-generated error response.
    let errorCode: Int?

    /// The expected "codeName" field in the server-generated error response.
    let errorCodeName: String?

    /// A list of error label strings that the error is expected to have.
    let errorLabelsContain: [String]?

    /// A list of error label strings that the error is expected not to have.
    let errorLabelsOmit: [String]?

    /// This field is only used in cases where the error includes a result (e.g. bulkWrite).
    // TODO: SWIFT-913: consider using custom type to represent results
    let expectResult: BSON?
}
