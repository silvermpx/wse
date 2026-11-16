plugins {
    kotlin("jvm") version "2.4.21"
}

group = "dev.wse"
version = "3.1.0"

kotlin { jvmToolchain(21) }

// The shared wrapper, and the bindings generated for the JVM.
sourceSets["main"].kotlin.srcDir("generated/jvm")

dependencies {
    implementation("net.java.dev.jna:jna:5.19.1")
    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-core:1.11.0")
    testImplementation(kotlin("test"))
}

tasks.test {
    useJUnitPlatform()
    // The native library build.sh made, and the server run-tests.sh started.
    systemProperty(
        "jna.library.path",
        System.getenv("WSE_NATIVE_LIB_DIR") ?: rootDir.resolve("../../target/debug").absolutePath,
    )
    System.getenv("WSE_TEST_URL")?.let { environment("WSE_TEST_URL", it) }
    jvmArgs("--enable-native-access=ALL-UNNAMED")
    testLogging { events("passed", "failed", "skipped"); showStandardStreams = false }
}
