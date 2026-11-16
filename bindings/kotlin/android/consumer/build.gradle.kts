// AGP's built-in Kotlin compiler is older than the Kotlin the libraries
// below are built with: this one takes its place.
buildscript {
    repositories { google(); mavenCentral() }
    dependencies { classpath("org.jetbrains.kotlin:kotlin-gradle-plugin:2.4.21") }
}

plugins {
    id("com.android.application") version "9.5.1"
}

android {
    namespace = "dev.wse.consumer"
    compileSdk = 36
    defaultConfig {
        applicationId = "dev.wse.consumer"
        minSdk = 26
        targetSdk = 36
    }
    sourceSets["main"].kotlin.srcDirs("src/main/kotlin")
    buildTypes {
        release {
            // As an application ships: shrunk and obfuscated, the AAR's own
            // rules all that keeps what JNA and the bindings need.
            isMinifyEnabled = true
            proguardFiles(getDefaultProguardFile("proguard-android-optimize.txt"))
            signingConfig = signingConfigs.getByName("debug")
        }
    }
    compileOptions {
        sourceCompatibility = JavaVersion.VERSION_17
        targetCompatibility = JavaVersion.VERSION_17
    }
}

dependencies {
    implementation(files(System.getenv("WSE_AAR") ?: error("WSE_AAR names the AAR to use")))
    implementation("net.java.dev.jna:jna:5.19.1@aar")
    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-core:1.11.0")
}
