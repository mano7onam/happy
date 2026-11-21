#if canImport(UIKit)
import AVFAudio
import HappyCore
import HappyLayout
import UIKit

/// Streams 16 kHz mono PCM16 microphone audio over Happy's encrypted machine
/// RPC (`handy-stt`) to the user's Mac (`Handy Antigravity`), feeding live
/// partial transcripts into `RecordButton` / `AgentInputCard` while speaking
/// and returning the merged final text on release or Stop-and-edit.
@MainActor
final class HandyRemoteDictation {
    var onLevel: ((CGFloat) -> Void)?
    var onTranscript: ((_ finalized: String, _ volatile: String) -> Void)?

    private let client: HandySttRemoteClient
    private let engine = AVAudioEngine()
    private let bufferBox = Pcm16StreamBuffer()
    private var worker: Task<String, Never>?
    private var latestText = ""
    private(set) var isRunning = false

    init(client: HandySttRemoteClient) {
        self.client = client
    }

    func start() throws {
        guard !isRunning else { throw Dictation.Failure.audio }
        latestText = ""
        onTranscript?("", "")

        let session = AVAudioSession.sharedInstance()
        do {
            try session.setCategory(.playAndRecord, mode: .default, options: [.duckOthers, .defaultToSpeaker, .allowBluetoothHFP])
            try session.setActive(true)
            let input = engine.inputNode
            let hwFormat = input.outputFormat(forBus: 0)
            guard let targetFormat = AVAudioFormat(commonFormat: .pcmFormatInt16, sampleRate: 16_000, channels: 1, interleaved: true),
                  let converter = AVAudioConverter(from: hwFormat, to: targetFormat)
            else {
                throw Dictation.Failure.audio
            }
            converter.primeMethod = .none

            let box = bufferBox
            let levelCallback: @Sendable (CGFloat) -> Void = { [weak self] value in
                Task { @MainActor in self?.onLevel?(value) }
            }
            input.removeTap(onBus: 0)
            input.installTap(onBus: 0, bufferSize: 1024, format: hwFormat, block: Self.makeTap(converter: converter, targetFormat: targetFormat, box: box, level: levelCallback))
            engine.prepare()
            try engine.start()
        } catch {
            stopAudio()
            throw Dictation.Failure.audio
        }

        isRunning = true
        let client = self.client
        let box = self.bufferBox
        worker = Task { [weak self] in
            var streamId: String?
            do {
                streamId = try await client.startStream()
            } catch {
                streamId = nil
            }

            // Flush ~360ms (11,520 bytes @ 16kHz Int16) chunks while recording is active
            while !Task.isCancelled {
                let (chunk, ended) = box.takePendingChunk(minBytes: 11_520)
                if let chunk, !chunk.isEmpty, let activeStreamId = streamId {
                    if let partial = try? await client.sendChunk(streamId: activeStreamId, pcm16: chunk), !partial.isEmpty {
                        await MainActor.run {
                            guard let self, self.isRunning else { return }
                            self.latestText = partial
                            self.onTranscript?("", partial)
                        }
                    }
                }
                if ended { break }
                try? await Task.sleep(for: .milliseconds(120))
            }

            if Task.isCancelled {
                if let activeStreamId = streamId {
                    await client.cancelStream(streamId: activeStreamId)
                }
                return ""
            }

            let tail = box.takeRemainingPending()
            if let activeStreamId = streamId {
                if let final = try? await client.finishStream(streamId: activeStreamId, tailPcm16: tail), !final.isEmpty {
                    return final
                }
            }

            // Crash-safe fallback: send the complete recorded 16kHz WAV in one shot if live stream dropped
            let allPcm = box.fullRecording()
            guard !allPcm.isEmpty else { return "" }
            let wav = HandySttRemoteClient.buildPcm16Wav(pcm16: allPcm)
            return (try? await client.transcribeAudio(data: wav, filename: "iphone-dictation.wav")) ?? ""
        }
    }

    func finish() async -> String {
        guard isRunning else { return latestText }
        isRunning = false
        stopAudio()
        bufferBox.markEnded()
        let final = (await worker?.value)?.trimmingCharacters(in: .whitespacesAndNewlines) ?? ""
        worker = nil
        let resolved = final.isEmpty ? latestText : final
        latestText = resolved
        onTranscript?(resolved, "")
        return resolved
    }

    func cancel() {
        guard isRunning else { return }
        isRunning = false
        stopAudio()
        bufferBox.markEnded()
        worker?.cancel()
        worker = nil
        latestText = ""
        onTranscript?("", "")
    }

    private func stopAudio() {
        if engine.isRunning { engine.stop() }
        engine.inputNode.removeTap(onBus: 0)
        try? AVAudioSession.sharedInstance().setActive(false, options: .notifyOthersOnDeactivation)
    }

    nonisolated private static func makeTap(
        converter: AVAudioConverter,
        targetFormat: AVAudioFormat,
        box: Pcm16StreamBuffer,
        level: @escaping @Sendable (CGFloat) -> Void
    ) -> AVAudioNodeTapBlock {
        var lastLevelTime = 0.0
        return { buffer, when in
            let now = AVAudioTime.seconds(forHostTime: when.hostTime)
            if now - lastLevelTime > 0.05 || lastLevelTime == 0 {
                lastLevelTime = now
                level(RecordMotion.level(rms: rms(buffer)))
            }
            let ratio = targetFormat.sampleRate / max(1.0, buffer.format.sampleRate)
            let capacity = AVAudioFrameCount((Double(buffer.frameLength) * ratio).rounded(.up)) + 4
            guard let outBuffer = AVAudioPCMBuffer(pcmFormat: targetFormat, frameCapacity: capacity) else { return }
            nonisolated(unsafe) var provided = false
            var error: NSError?
            let status = converter.convert(to: outBuffer, error: &error) { _, outStatus in
                if provided {
                    outStatus.pointee = .noDataNow
                    return nil
                }
                provided = true
                outStatus.pointee = .haveData
                return buffer
            }
            guard status != .error, outBuffer.frameLength > 0, let int16Ptr = outBuffer.int16ChannelData?[0] else { return }
            let byteCount = Int(outBuffer.frameLength) * MemoryLayout<Int16>.size
            box.append(Data(bytes: int16Ptr, count: byteCount))
        }
    }

    nonisolated private static func rms(_ buffer: AVAudioPCMBuffer) -> Float {
        guard let samples = buffer.floatChannelData?[0], buffer.frameLength > 0 else { return 0 }
        var sum: Float = 0
        for i in 0..<Int(buffer.frameLength) {
            sum += samples[i] * samples[i]
        }
        return (sum / Float(buffer.frameLength)).squareRoot()
    }
}

private final class Pcm16StreamBuffer: @unchecked Sendable {
    private let lock = NSLock()
    private var pending = Data()
    private var full = Data()
    private var ended = false

    func append(_ data: Data) {
        lock.withLock {
            guard !ended else { return }
            pending.append(data)
            full.append(data)
        }
    }

    func markEnded() {
        lock.withLock { ended = true }
    }

    func takePendingChunk(minBytes: Int) -> (Data?, Bool) {
        lock.withLock {
            if pending.count >= minBytes {
                let out = pending
                pending = Data()
                return (out, ended)
            }
            return (nil, ended)
        }
    }

    func takeRemainingPending() -> Data {
        lock.withLock {
            let out = pending
            pending = Data()
            return out
        }
    }

    func fullRecording() -> Data {
        lock.withLock { full }
    }
}
#endif
