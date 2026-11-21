import Foundation
import HappyStore
import HappyWire
import Testing
@testable import HappyCore

@Suite("HandySttRemoteClient")
struct HandySttRemoteClientTests {
    @Test("prefers active decryptable machine sorted by activeAt")
    @MainActor
    func prefersActiveDecryptableMachine() {
        let olderActive = MachineRecord(
            id: "m-old", seq: 1, createdAt: 1, updatedAt: 1, active: true, activeAt: 100,
            metadataVersion: 1, metadataCipher: "", metadata: nil,
            daemonStateVersion: 1, daemonStateCipher: nil, daemonState: nil,
            dataKeyCipher: "k", decryptable: true
        )
        let newerActive = MachineRecord(
            id: "m-new", seq: 1, createdAt: 1, updatedAt: 1, active: true, activeAt: 200,
            metadataVersion: 1, metadataCipher: "", metadata: nil,
            daemonStateVersion: 1, daemonStateCipher: nil, daemonState: nil,
            dataKeyCipher: "k", decryptable: true
        )
        let undecryptable = MachineRecord(
            id: "m-locked", seq: 1, createdAt: 1, updatedAt: 1, active: true, activeAt: 999,
            metadataVersion: 1, metadataCipher: "", metadata: nil,
            daemonStateVersion: 1, daemonStateCipher: nil, daemonState: nil,
            dataKeyCipher: nil, decryptable: false
        )
        #expect(HandySttRegistry.preferredMachineId(in: [olderActive, newerActive, undecryptable]) == "m-new")
    }

    @Test("streams start -> chunk -> finish and builds valid 16kHz RIFF WAV fallback")
    func streamsAndBuildsWav() async throws {
        actor Recorder {
            var actions: [String] = []
            func record(_ a: String) { actions.append(a) }
        }
        let rec = Recorder()
        let client = HandySttRemoteClient { params in
            let action = params["action"]?.stringValue ?? ""
            await rec.record(action)
            switch action {
            case "start":
                return .object(["ok": .bool(true), "streamId": .string("st-42")])
            case "chunk":
                return .object(["ok": .bool(true), "text": .string("проверь голосовой")])
            case "finish":
                return .object(["ok": .bool(true), "text": .string("Проверь голосовой ввод в Happy.")])
            default:
                return .object(["ok": .bool(false)])
            }
        }

        let sid = try await client.startStream()
        #expect(sid == "st-42")
        let partial = try await client.sendChunk(streamId: sid, pcm16: Data([1, 2, 3, 4]))
        #expect(partial == "проверь голосовой")
        let final = try await client.finishStream(streamId: sid, tailPcm16: Data([5, 6]))
        #expect(final == "Проверь голосовой ввод в Happy.")
        #expect(await rec.actions == ["start", "chunk", "finish"])

        let wav = HandySttRemoteClient.buildPcm16Wav(pcm16: Data([0, 1, 2, 3]))
        #expect(wav.count == 48)
        #expect(String(decoding: wav.prefix(4), as: UTF8.self) == "RIFF")
        #expect(String(decoding: wav[8..<12], as: UTF8.self) == "WAVE")
    }
}
