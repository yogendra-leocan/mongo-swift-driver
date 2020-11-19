import MongoSwiftSync
import TestsCommon

struct UnifiedTestRunner {
    let internalClient: MongoClient
    let serverVersion: ServerVersion
    let topologyType: TestTopologyConfiguration

    static let MIN_SCHEMA_VERSION = SchemaVersion(rawValue: "1.0.0")!
    static let MAX_SCHEMA_VERSION = SchemaVersion(rawValue: "1.0.0")!

    init() throws {
        let connStr = MongoSwiftTestCase.getConnectionString(singleMongos: false).toString()
        self.internalClient = try MongoClient.makeTestClient(connStr)
        self.serverVersion = try self.internalClient.serverVersion()
        self.topologyType = try self.internalClient.topologyType()

        // The test runner SHOULD terminate any open transactions using the internal MongoClient before executing any
        // tests. Using the internal MongoClient, execute the killAllSessions command on either the primary or, if
        // connected to a sharded cluster, all mongos servers.
        switch self.topologyType {
        case .single:
            return
        case .replicaSet:
            let admin = self.internalClient.db("admin")
            for address in MongoSwiftTestCase.getHosts() {
                let isMaster = try admin.runCommand(["isMaster": 1], on: address)["ismaster"]!.boolValue!
                if isMaster {
                    _ = try admin.runCommand(["killAllSessions": []], on: address)
                    return
                }
            }
        case .sharded, .shardedReplicaSet:
            for address in MongoSwiftTestCase.getHosts() {
                _ = try self.internalClient.db("admin").runCommand(["killAllSessions": []], on: address)
            }
        }
    }

    func getUnmetRequirement(_ requirement: TestRequirement) -> UnmetRequirement? {
        requirement.getUnmetRequirement(givenCurrent: self.serverVersion, self.topologyType)
    }

    func runFiles(_ files: [UnifiedTestFile]) throws {
        for file in files {
            // Upon loading a file, the test runner MUST read the schemaVersion field and determine if the test file
            // can be processed further.
            guard file.schemaVersion >= Self.MIN_SCHEMA_VERSION && file.schemaVersion <= Self.MAX_SCHEMA_VERSION else {
                throw TestError(
                    message: "Test file \"\(file.description)\" has unsupported schema version \(file.schemaVersion)"
                )
            }

            // If runOnRequirements is specified, the test runner MUST skip the test file unless one or more
            //  runOnRequirement objects are satisfied.
            if let requirements = file.runOnRequirements {
                guard requirements.contains(where: { self.getUnmetRequirement($0) == nil }) else {
                    fileLevelLog("Skipping tests from file \"\(file.description)\", deployment requirements not met.")
                    continue
                }
            }

            for test in file.tests {
                // If test.skipReason is specified, the test runner MUST skip this test and MAY use the string value to
                // log a message.
                if let skipReason = test.skipReason {
                    fileLevelLog(
                        "Skipping test \"\(test.description)\" from file \"\(file.description)\": \(skipReason)."
                    )
                    continue
                }

                // If test.runOnRequirements is specified, the test runner MUST skip the test unless one or more
                // runOnRequirement objects are satisfied.
                if let requirements = test.runOnRequirements {
                    guard requirements.contains(where: { self.getUnmetRequirement($0)  == nil }) else {
                        fileLevelLog(
                            "Skipping test \"\(test.description)\" from file \"\(file.description)\", " + 
                            "deployment requirements not met."
                        )
                        continue
                    }
                }

                fileLevelLog("Running test \"\(test.description)\" from file \"\(file.description)\"")



                // If initialData is specified, for each collectionData therein the test runner MUST drop the
                // collection and insert the specified documents (if any) using a "majority" write concern. If no
                // documents are specified, the test runner MUST create the collection with a "majority" write concern.
                // The test runner MUST use the internal MongoClient for these operations.
                if let initialData = file.initialData {
                    for collData in initialData {
                        let db = internalClient.db(collData.databaseName)
                        let coll = db.collection(collData.collectionName)
                        try coll.drop(options: DropCollectionOptions(writeConcern: .majority))

                        guard !collData.documents.isEmpty else {
                            _ = try db.createCollection(
                                collData.collectionName,
                                options: CreateCollectionOptions(writeConcern: .majority)
                            )
                            continue
                        }

                        try coll.insertMany(collData.documents, options: InsertManyOptions(writeConcern: .majority))
                    }
                }

                let entityMap = try file.createEntities?.toEntityMap() ?? [:]

                // Workaround for SERVER-39704:  a test runners MUST execute a non-transactional distinct command on
                // each mongos server before running any test that might execute distinct within a transaction. To ease
                // the implementation, test runners MAY execute distinct before every test.
                if self.topologyType == .sharded || self.topologyType == .shardedReplicaSet {
                    let collEntities = entityMap.values.compactMap { try? $0.asCollection() }
                    for address in MongoSwiftTestCase.getHosts() {
                        for entity in collEntities {
                            _ = try self.internalClient.db(entity.namespace.db).runCommand(
                                ["distinct": .string(entity.name), "key": "_id"],
                                on: address
                            )
                        }
                    }
                }

                for operation in test.operations {
                    print(operation)
                    try operation.execute(using: entityMap)
                    print("\n\n")
                }

                // disable fail points

                // check expected events

                // check outcome

                // clear entity map / end sessions
            }
        }
    }
}
