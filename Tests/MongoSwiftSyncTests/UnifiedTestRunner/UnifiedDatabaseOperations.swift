import MongoSwiftSync

struct UnifiedCreateCollection: UnifiedOperationProtocol {
    /// The collection to create.
    let collection: String

    /// Optional identifier for a session entity to use.
    let session: String?

    static var knownArguments: Set<String> {
        ["collection", "session"]
    }

    func execute(
        on object: UnifiedOperation.Object,
        entities: EntityMap,
        testRunner _: UnifiedTestRunner
    ) throws -> UnifiedOperationResult {
        let db = try entities.getEntity(from: object).asDatabase()
        let session = try entities.resolveSession(id: self.session)
        _ = try db.createCollection(self.collection, session: session)
        return .none
    }
}

struct UnifiedDropCollection: UnifiedOperationProtocol {
    /// The collection to drop.
    let collection: String

    /// Optional identifier for a session entity to use.
    let session: String?

    static var knownArguments: Set<String> {
        ["collection", "session"]
    }

    func execute(
        on object: UnifiedOperation.Object,
        entities: EntityMap,
        testRunner _: UnifiedTestRunner
    ) throws -> UnifiedOperationResult {
        let db = try entities.getEntity(from: object).asDatabase()
        let session = try entities.resolveSession(id: self.session)
        try db.collection(self.collection).drop(session: session)
        return .none
    }
}
