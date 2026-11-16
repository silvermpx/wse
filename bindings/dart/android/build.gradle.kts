// The package's Android part: it sets up Android's certificate verifier as the
// app starts. The app's Android Gradle plugin builds it.
group = "dev.wse.flutter"
version = "3.1.0"

plugins {
    id("com.android.library")
}

android {
    namespace = "dev.wse.flutter"
    compileSdk = 36
    compileOptions {
        sourceCompatibility = JavaVersion.VERSION_17
        targetCompatibility = JavaVersion.VERSION_17
    }
    defaultConfig {
        minSdk = 24
        consumerProguardFiles("consumer-rules.pro")
    }
}

dependencies {
    // The Java part of Android's certificate verifier (the rustls-platform-verifier
    // crate's), which bindings/rustls-platform-verifier.sh fetched; it is written in Kotlin.
    implementation(files("libs/rustls-platform-verifier.jar"))
    implementation("org.jetbrains.kotlin:kotlin-stdlib:2.0.21")
}
