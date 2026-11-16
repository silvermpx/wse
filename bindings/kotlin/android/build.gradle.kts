// AGP's built-in Kotlin compiler is older than the Kotlin the libraries
// below are built with: this one takes its place.
buildscript {
    repositories { google(); mavenCentral() }
    dependencies { classpath("org.jetbrains.kotlin:kotlin-gradle-plugin:2.4.21") }
}

plugins {
    id("com.android.library") version "9.5.1"
}

android {
    namespace = "dev.wse.ffi"
    compileSdk = 36
    defaultConfig {
        minSdk = 26
        testInstrumentationRunner = "androidx.test.runner.AndroidJUnitRunner"
        consumerProguardFiles("consumer-rules.pro")
    }
    sourceSets["main"].kotlin.srcDirs("src/main/kotlin", "../src/main/kotlin", "../generated/android")
    sourceSets["androidTest"].kotlin.srcDirs("src/androidTest/kotlin")
    // cargo-ndk's output: one folder per ABI.
    sourceSets["main"].jniLibs.srcDirs(System.getenv("WSE_JNI_LIBS") ?: "../../../target/android-jniLibs")
    compileOptions {
        sourceCompatibility = JavaVersion.VERSION_17
        targetCompatibility = JavaVersion.VERSION_17
    }
}

dependencies {
    implementation("net.java.dev.jna:jna:5.19.1@aar")
    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-core:1.11.0")
    implementation("androidx.annotation:annotation:1.9.1")
    implementation("androidx.lifecycle:lifecycle-process:2.11.0")
    implementation("androidx.startup:startup-runtime:1.2.0")
    // The Java part of Android's certificate verifier, which build-libs.sh fetched: inside the AAR.
    implementation(files(System.getenv("WSE_ANDROID_VERIFIER") ?: "../../../target/android-verifier/rustls-platform-verifier.jar"))
    androidTestImplementation("androidx.test.ext:junit:1.3.0")
    androidTestImplementation("androidx.test:runner:1.7.0")
    androidTestImplementation("org.jetbrains.kotlin:kotlin-test:2.4.21")
}
