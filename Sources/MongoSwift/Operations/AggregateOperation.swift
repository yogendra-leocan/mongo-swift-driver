import CLibMongoC
import Foundation

/// The entity on which to execute the `aggregate` command against.
internal enum AggregateTarget<CollectionType: Codable> {
    /// Indicates `aggregate` command will be executed against a database.
    case database(MongoDatabase)

    ///  Indicates `aggregate` command will be executed against a collection.
    case collection(MongoCollection<CollectionType>)
}

/// Options to use when executing an `aggregate` command on a `MongoCollection`.
public struct AggregateOptions: Codable {
    /// Enables the server to write to temporary files. When set to true, the aggregate operation
    /// can write data to the _tmp subdirectory in the dbPath directory.
    public var allowDiskUse: Bool?

    /// The number of `BSONDocument`s to return per batch.
    public var batchSize: Int?

    /// If true, allows the write to opt-out of document level validation. This only applies
    /// when the $out stage is specified.
    public var bypassDocumentValidation: Bool?

    /// Specifies a collation.
    public var collation: BSONDocument?

    /// Enables users to specify an arbitrary string to help trace the operation through
    /// the database profiler, currentOp and logs. The default is to not send a value.
    public var comment: String?

    /// The index hint to use for the aggregation. The hint does not apply to $lookup and $graphLookup stages.
    public var hint: IndexHint?

    /// The maximum amount of time to allow the query to run.
    public var maxTimeMS: Int?

    /// A `ReadConcern` to use in read stages of this operation.
    public var readConcern: ReadConcern?

    // swiftlint:disable redundant_optional_initialization
    /// A ReadPreference to use for this operation.
    public var readPreference: ReadPreference? = nil
    // swiftlint:enable redundant_optional_initialization

    /// A `WriteConcern` to use in `$out` stages of this operation.
    public var writeConcern: WriteConcern?

    /// Convenience initializer allowing any/all parameters to be omitted or optional.
    public init(
        allowDiskUse: Bool? = nil,
        batchSize: Int? = nil,
        bypassDocumentValidation: Bool? = nil,
        collation: BSONDocument? = nil,
        comment: String? = nil,
        hint: IndexHint? = nil,
        maxTimeMS: Int? = nil,
        readConcern: ReadConcern? = nil,
        readPreference: ReadPreference? = nil,
        writeConcern: WriteConcern? = nil
    ) {
        self.allowDiskUse = allowDiskUse
        self.batchSize = batchSize
        self.bypassDocumentValidation = bypassDocumentValidation
        self.collation = collation
        self.comment = comment
        self.hint = hint
        self.maxTimeMS = maxTimeMS
        self.readConcern = readConcern
        self.readPreference = readPreference
        self.writeConcern = writeConcern
    }

    internal enum CodingKeys: String, CodingKey, CaseIterable {
        case allowDiskUse, batchSize, bypassDocumentValidation, collation, maxTimeMS, comment, hint, readConcern,
             writeConcern
    }
}

/// An operation corresponding to an "aggregate" command on either a database or a collection.
internal struct AggregateOperation<CollectionType: Codable, OutputType: Codable>: Operation {
    private let target: AggregateTarget<CollectionType>
    private let pipeline: [BSONDocument]
    private let options: AggregateOptions?

    internal init(target: AggregateTarget<CollectionType>, pipeline: [BSONDocument], options: AggregateOptions?) {
        self.target = target
        self.pipeline = pipeline
        self.options = options
    }

    internal func execute(using connection: Connection, session: ClientSession?) throws -> MongoCursor<OutputType> {
        let opts = try encodeOptions(options: self.options, session: session)
        let pipeline: BSONDocument = ["pipeline": .array(self.pipeline.map { .document($0) })]

        return try pipeline.withBSONPointer { pipelinePtr in
            try withOptionalBSONPointer(to: opts) { optsPtr in
                try ReadPreference.withOptionalMongocReadPreference(from: self.options?.readPreference) { rpPtr in
                    let result: OpaquePointer
                    let client: MongoClient
                    let decoder: BSONDecoder
                    switch self.target {
                    case let .database(db):
                        client = db._client
                        decoder = db.decoder
                        result = db.withMongocDatabase(from: connection) { dbPtr in
                            guard let result = mongoc_database_aggregate(
                                dbPtr,
                                pipelinePtr,
                                optsPtr,
                                rpPtr
                            ) else {
                                fatalError(failedToRetrieveCursorMessage)
                            }
                            return result
                        }
                    case let .collection(coll):
                        client = coll._client
                        decoder = coll.decoder
                        result = coll.withMongocCollection(from: connection) { collPtr in
                            guard let result = mongoc_collection_aggregate(
                                collPtr,
                                MONGOC_QUERY_NONE,
                                pipelinePtr,
                                optsPtr,
                                rpPtr
                            ) else {
                                fatalError(failedToRetrieveCursorMessage)
                            }
                            return result
                        }
                    }

                    return try MongoCursor<OutputType>(
                        stealing: result,
                        connection: connection,
                        client: client,
                        decoder: decoder,
                        session: session
                    )
                }
            }
        }
    }
}
