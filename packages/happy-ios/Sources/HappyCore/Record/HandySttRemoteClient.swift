import Foundation
import HappyStore
import HappyWire

/// Tracks the active signed-in `AccountSession` so `RecordButton` / `Dictation`
/// can stream audio over Happy's E2EE machine RPC (`handy-stt`) without
/// threading extra dependencies through every screen.
@MainActor
public enum HandySttRegistry {
    public private(set) static weak var activeSession: AccountSession?

    public static func observe(_ state: AccountController.State) {
        switch state {
        case .signedIn(let session):
            activeSession = session
        case .signedOut, .agentConnection:
            activeSession = nil
        }
    }

    public static func preferredMachineId(in machines: [MachineRecord]) -> String? {
        let candidates = machines.filter(\.decryptable)
        guard !candidates.isEmpty else { return nil }
        return candidates
            .sorted { lhs, rhs in
                if lhs.active != rhs.active { return lhs.active && !rhs.active }
                return lhs.activeAt > rhs.activeAt
            }
            .first?
            .id
    }
}

public enum HandySttRemoteError: Error, Equatable, Sendable {
    case unavailable
    case serverError(String)
}

/// Encrypted RPC client for the Mac's `Handy Antigravity` STT bridge (`handy-stt`).
public struct HandySttRemoteClient: Sendable {
    public static let method = "handy-stt"

    private let callFn: @Sendable (JSONValue) async throws -> JSONValue

    public init(machineActions: MachineActions, machineId: String) {
        self.callFn = { params in
            try await machineActions.call(machineId: machineId, method: Self.method, params: params)
        }
    }

    public init(call: @escaping @Sendable (JSONValue) async throws -> JSONValue) {
        self.callFn = call
    }

    @MainActor
    public static func current() -> HandySttRemoteClient? {
        guard ProcessInfo.processInfo.environment["HAPPY_DISABLE_HANDY_STT"] != "1",
              let session = HandySttRegistry.activeSession,
              let machines = try? session.store.machines(),
              let machineId = HandySttRegistry.preferredMachineId(in: machines)
        else {
            return nil
        }
        return HandySttRemoteClient(machineActions: session.machineActions, machineId: machineId)
    }

    public func startStream() async throws -> String {
        let res = try await callFn(.object(["action": .string("start")]))
        guard res["ok"]?.boolValue == true, let streamId = res["streamId"]?.stringValue, !streamId.isEmpty else {
            throw HandySttRemoteError.serverError(res["error"]?.stringValue ?? "Failed to start Handy STT stream")
        }
        return streamId
    }

    public func sendChunk(streamId: String, pcm16: Data) async throws -> String {
        let res = try await callFn(.object([
            "action": .string("chunk"),
            "streamId": .string(streamId),
            "pcm16Base64": .string(pcm16.base64EncodedString()),
        ]))
        guard res["ok"]?.boolValue == true else {
            throw HandySttRemoteError.serverError(res["error"]?.stringValue ?? "Failed to send Handy STT chunk")
        }
        return res["text"]?.stringValue ?? ""
    }

    public func finishStream(streamId: String, tailPcm16: Data = Data()) async throws -> String {
        var payload: [String: JSONValue] = [
            "action": .string("finish"),
            "streamId": .string(streamId),
        ]
        if !tailPcm16.isEmpty {
            payload["pcm16Base64"] = .string(tailPcm16.base64EncodedString())
        }
        let res = try await callFn(.object(payload))
        guard res["ok"]?.boolValue == true else {
            throw HandySttRemoteError.serverError(res["error"]?.stringValue ?? "Failed to finish Handy STT stream")
        }
        return res["text"]?.stringValue ?? ""
    }

    public func cancelStream(streamId: String) async {
        _ = try? await callFn(.object([
            "action": .string("cancel"),
            "streamId": .string(streamId),
        ]))
    }

    public func transcribeAudio(data: Data, filename: String = "iphone-dictation.wav") async throws -> String {
        let res = try await callFn(.object([
            "action": .string("transcribe"),
            "audioBase64": .string(data.base64EncodedString()),
            "filename": .string(filename),
        ]))
        guard res["ok"]?.boolValue == true else {
            throw HandySttRemoteError.serverError(res["error"]?.stringValue ?? "Handy STT transcribe failed")
        }
        return res["text"]?.stringValue ?? ""
    }

    /// Wrap raw 16kHz 16-bit mono little-endian PCM bytes in a 44-byte RIFF/WAVE header.
    public static func buildPcm16Wav(pcm16: Data, sampleRate: UInt32 = 16_000) -> Data {
        let channels: UInt16 = 1
        let bitsPerSample: UInt16 = 16
        let byteRate = sampleRate * UInt32(channels) * UInt32(bitsPerSample / 8)
        let blockAlign = channels * (bitsPerSample / 8)
        let dataSize = UInt32(pcm16.count)
        let riffSize = 36 + dataSize

        var out = Data(capacity: 44 + pcm16.count)
        out.append(contentsOf: "RIFF".utf8)
        out.appendLittleEndian(riffSize)
        out.append(contentsOf: "WAVE".utf8)
        out.append(contentsOf: "fmt ".utf8)
        out.appendLittleEndian(UInt32(16))
        out.appendLittleEndian(UInt16(1))
        out.appendLittleEndian(channels)
        out.appendLittleEndian(sampleRate)
        out.appendLittleEndian(byteRate)
        out.appendLittleEndian(blockAlign)
        out.appendLittleEndian(bitsPerSample)
        out.append(contentsOf: "data".utf8)
        out.appendLittleEndian(dataSize)
        out.append(pcm16)
        return out
    }
}

private extension Data {
    mutating func appendLittleEndian<T: FixedWidthInteger>(_ value: T) {
        var le = value.littleEndian
        Swift.withUnsafeBytes(of: &le) { append(contentsOf: $0) }
    }
}
