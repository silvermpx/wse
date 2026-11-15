// The WSE client as an Android library: the shared Kotlin wrapper, the
// bindings generated for Android and the native libraries cargo-ndk builds.
pluginManagement {
    repositories { google(); mavenCentral(); gradlePluginPortal() }
}
dependencyResolutionManagement {
    repositories { google(); mavenCentral() }
}
rootProject.name = "wse-android"
