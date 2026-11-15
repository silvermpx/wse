// swift-tools-version:5.10
// The WSE client for Swift (iOS 15+, macOS 12+). The native part comes from
// build.sh: WseFFIC.xcframework, and the bindings in Sources/WseFFI.
import PackageDescription

let package = Package(
    name: "WseClient",
    platforms: [.iOS(.v15), .macOS(.v12)],
    products: [.library(name: "WseFFI", targets: ["WseFFI"])],
    targets: [
        .binaryTarget(name: "WseFFIC", path: "WseFFIC.xcframework"),
        .target(name: "WseFFI", dependencies: ["WseFFIC"]),
        .testTarget(name: "WseFFITests", dependencies: ["WseFFI"]),
    ],
    swiftLanguageVersions: [.v5]
)
