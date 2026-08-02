import AppKit
import Foundation
import Vision

struct OCRResult: Codable {
    let path: String
    let status: String
    let text: String
    let confidence: Float
    let error: String?
}

func cgImage(from path: String) -> CGImage? {
    guard let image = NSImage(contentsOfFile: path) else { return nil }
    var rect = NSRect(origin: .zero, size: image.size)
    return image.cgImage(forProposedRect: &rect, context: nil, hints: nil)
}

let encoder = JSONEncoder()
encoder.outputFormatting = [.withoutEscapingSlashes]

for path in CommandLine.arguments.dropFirst() {
    guard let image = cgImage(from: path) else {
        let result = OCRResult(path: path, status: "failed", text: "", confidence: 0, error: "image_load_failed")
        print(String(data: try encoder.encode(result), encoding: .utf8)!)
        continue
    }

    let request = VNRecognizeTextRequest()
    request.recognitionLevel = .accurate
    request.usesLanguageCorrection = true
    request.recognitionLanguages = ["zh-Hans", "en-US"]
    let handler = VNImageRequestHandler(cgImage: image, options: [:])

    do {
        try handler.perform([request])
        let observations = request.results ?? []
        let candidates = observations.compactMap { $0.topCandidates(1).first }
        let text = candidates.map { $0.string }.joined(separator: "\n")
        let confidence = candidates.isEmpty ? 0 : candidates.map { $0.confidence }.reduce(0, +) / Float(candidates.count)
        let result = OCRResult(path: path, status: "success", text: text, confidence: confidence, error: nil)
        print(String(data: try encoder.encode(result), encoding: .utf8)!)
    } catch {
        let result = OCRResult(path: path, status: "failed", text: "", confidence: 0, error: String(describing: error))
        print(String(data: try encoder.encode(result), encoding: .utf8)!)
    }
}
