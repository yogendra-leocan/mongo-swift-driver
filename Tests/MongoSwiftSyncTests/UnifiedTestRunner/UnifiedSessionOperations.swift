@testable import MongoSwiftSync

struct EndSession: UnifiedOperationProtocol {
    static var knownArguments: Set<String> { [] }

    func execute(on object: UnifiedOperation.Object, using entities: [String: Entity]) throws {
        let session = try entities.getEntity(id: object.asEntityId()).asSession()
        session.end()
    }
}

struct UnifiedStartTransaction: UnifiedOperationProtocol {
    static var knownArguments: Set<String> { [] }
}

struct UnifiedCommitTransaction: UnifiedOperationProtocol {
    static var knownArguments: Set<String> { [] }
}

struct UnifiedAbortTransaction: UnifiedOperationProtocol {
    static var knownArguments: Set<String> { [] }
}
