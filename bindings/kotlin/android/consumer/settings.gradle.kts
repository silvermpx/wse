// An app that uses the WSE AAR the way an application does, built with R8:
// run-consumer-check.sh checks on an emulator that it still connects.
pluginManagement {
    repositories { google(); mavenCentral(); gradlePluginPortal() }
}
dependencyResolutionManagement {
    repositories { google(); mavenCentral() }
}
rootProject.name = "wse-consumer"
