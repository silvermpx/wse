import org.jetbrains.kotlin.gradle.dsl.JvmTarget

plugins {
    kotlin("jvm") version "2.4.21"
}

group = "io.github.silvermpx"
version = "3.2.0"

// The JAR runs on Java 17 and newer, whichever JDK (17 or newer) builds it.
kotlin {
    compilerOptions {
        jvmTarget.set(JvmTarget.JVM_17)
        freeCompilerArgs.add("-Xjdk-release=17")
    }
}
java {
    sourceCompatibility = JavaVersion.VERSION_17
    targetCompatibility = JavaVersion.VERSION_17
}
// The Java tests compile against Java 17's API too.
tasks.withType<JavaCompile>().configureEach { options.release.set(17) }

// The shared wrapper, the JVM's own part, and the bindings generated for the JVM.
sourceSets["main"].kotlin.srcDirs("src/jvm/kotlin", "generated/jvm")
// The conformance driver, which the Android library's instrumented tests run too.
sourceSets["test"].kotlin.srcDirs("src/conformance/kotlin")

dependencies {
    implementation("net.java.dev.jna:jna:5.19.1")
    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-core:1.11.0")
    testImplementation(kotlin("test"))
    // The conformance driver's JSON.
    testImplementation("com.google.code.gson:gson:2.11.0")
}

base { archivesName.set("wse-client-jvm") }

tasks.jar {
    // The Rust library of each platform, where JNA looks for it:
    // WSE_JVM_NATIVES names a directory jvm-natives.sh laid out.
    System.getenv("WSE_JVM_NATIVES")?.let { from(it) }
    manifest { attributes("Implementation-Title" to "wse-client-jvm", "Implementation-Version" to project.version) }
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
    testLogging {
        events("passed", "failed", "skipped")
        showStandardStreams = false
        // A failure names its test and why in a quiet run too (run-tests.sh's).
        quiet {
            events("failed")
            exceptionFormat = org.gradle.api.tasks.testing.logging.TestExceptionFormat.FULL
        }
    }
    // What the tests run against, the native library and the servers, is
    // outside Gradle's inputs: they run each time, never taken as done.
    outputs.upToDateWhen { false }
    outputs.cacheIf { false }
}
