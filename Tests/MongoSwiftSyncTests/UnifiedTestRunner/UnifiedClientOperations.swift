import MongoSwiftSync

struct UnifiedListDatabases: UnifiedOperationProtocol {
    static var knownArguments: Set<String> { [] }

    func execute(
        on object: UnifiedOperation.Object,
        entities: EntityMap,
        testRunner _: UnifiedTestRunner
    ) throws -> UnifiedOperationResult {
        let testClient = try entities.getEntity(from: object).asTestClient()
        let dbSpecs = try testClient.client.listDatabases()
        let encoded = try BSONEncoder().encode(dbSpecs)
        return .bson(.array(encoded.map { .document($0) }))
    }
}
