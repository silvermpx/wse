// An app that uses the WSE package the way an application does, from a Maven
// repository with its POM, built with R8: run-consumer-check.sh lays the
// repository out and checks on an emulator that the app still connects.
pluginManagement {
    repositories { google(); mavenCentral(); gradlePluginPortal() }
}
dependencyResolutionManagement {
    repositories {
        maven { url = uri(System.getenv("WSE_MAVEN_REPO") ?: error("WSE_MAVEN_REPO names the package's repository")) }
        google()
        mavenCentral()
    }
}
rootProject.name = "wse-consumer"
