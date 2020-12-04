import MongoSwiftSync
import Nimble
import TestsCommon

final class UnifiedRunnerTests: MongoSwiftTestCase {
    override func setUp() {
        self.continueAfterFailure = false
    }

    func testSchemaVersion() {
        let oneTwoThree = SchemaVersion(rawValue: "1.2.3")
        expect(oneTwoThree).toNot(beNil())
        expect(oneTwoThree?.major).to(equal(1))
        expect(oneTwoThree?.minor).to(equal(2))
        expect(oneTwoThree?.patch).to(equal(3))

        // no patch provided
        let oneTwo = SchemaVersion(rawValue: "1.2")
        expect(oneTwo).toNot(beNil())
        expect(oneTwo?.major).to(equal(1))
        expect(oneTwo?.minor).to(equal(2))
        expect(oneTwo?.patch).to(equal(0))

        // no minor provided
        let one = SchemaVersion(rawValue: "1")
        expect(one).toNot(beNil())
        expect(one?.major).to(equal(1))
        expect(one?.minor).to(equal(0))
        expect(one?.patch).to(equal(0))

        // invalid inputs
        let inputs = [
            "a",
            "1.2.3.4",
            ""
        ]

        for input in inputs {
            expect(SchemaVersion(rawValue: input)).to(beNil())
        }
    }

    func testSampleUnifiedTests() throws {
        let validPassTests = try retrieveSpecTestFiles(
            specName: "unified-test-format",
            subdirectory: "valid-pass",
            asType: UnifiedTestFile.self
        ).map { $0.1 }

        let skipRunningValid: [String: [String]] = [
            // unsupported APIs
            "poc-transactions-convenient-api": ["*"],
            "poc-gridfs": ["*"],
            // requires DB-level aggregate. TODO SWIFT-577: unskip
            "poc-crud": ["Aggregate with $listLocalSessions"]
        ]

        let runner = try UnifiedTestRunner()
        try runner.runFiles(validPassTests, skipTests: skipRunningValid)

        let skipDecodingValidFailFiles = [
            // Because we use an enum to represent ReturnDocument, the invalid string present in this file "Invalid"
            // gives us a decoding error, and therefore we cannot decode it. Other drivers may not report an error
            // until runtime.
            "returnDocument-enum-invalid.json"
        ]

        let validFailTests = try retrieveSpecTestFiles(
            specName: "unified-test-format",
            subdirectory: "valid-fail",
            excludeFiles: skipDecodingValidFailFiles,
            asType: UnifiedTestFile.self
        )

        for (_, test) in validFailTests {
            expect(try runner.runFiles([test])).to(throwError())
        }
    }

    func testStrictDecodableTypes() throws {
        // Test decoding a valid key. Conveniently, this options is supported by 4 of the 5 StrictDecodable types.
        let validOptsRaw: BSONDocument = [
            "readPreference": "primary"
        ]

        expect(try BSONDecoder().decode(MongoDatabaseOptions.self, from: validOptsRaw)).toNot(throwError())
        expect(try BSONDecoder().decode(MongoCollectionOptions.self, from: validOptsRaw)).toNot(throwError())
        expect(try BSONDecoder().decode(MongoClientOptions.self, from: validOptsRaw)).toNot(throwError())
        expect(try BSONDecoder().decode(TransactionOptions.self, from: validOptsRaw)).toNot(throwError())

        let validSessionOptsRaw: BSONDocument = [
            "causalConsistency": true,
            "defaultTransactionOptions": .document(validOptsRaw)
        ]
        expect(try BSONDecoder().decode(ClientSessionOptions.self, from: validSessionOptsRaw)).toNot(throwError())

        // Test decoding from a document with an unsupported key errors.
        let invalidOptsRaw: BSONDocument = [
            "blah": "hi"
        ]
        expect(try BSONDecoder().decode(MongoDatabaseOptions.self, from: invalidOptsRaw))
            .to(throwError(errorType: TestError.self))
        expect(try BSONDecoder().decode(MongoCollectionOptions.self, from: invalidOptsRaw))
            .to(throwError(errorType: TestError.self))
        expect(try BSONDecoder().decode(MongoClientOptions.self, from: invalidOptsRaw))
            .to(throwError(errorType: TestError.self))
        expect(try BSONDecoder().decode(ClientSessionOptions.self, from: invalidOptsRaw))
            .to(throwError(errorType: TestError.self))
        expect(try BSONDecoder().decode(TransactionOptions.self, from: invalidOptsRaw))
            .to(throwError(errorType: TestError.self))

        // Test that we error when the invalid key is in a nested field (whose type also conforms to StrictDecodable).
        let invalidNestedOptsRaw: BSONDocument = [
            "defaultTransactionOptions": .document(invalidOptsRaw)
        ]
        expect(try BSONDecoder().decode(ClientSessionOptions.self, from: invalidNestedOptsRaw))
            .to(throwError(errorType: TestError.self))
    }
}
