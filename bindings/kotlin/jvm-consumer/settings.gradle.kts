// A Java application that uses the WSE JVM package the way one does: from a
// Maven repository, with the dependencies its POM names, and nothing else.
// run-jvm-consumer-check.sh builds and runs it.
rootProject.name = "wse-jvm-consumer"

dependencyResolutionManagement {
    repositories {
        // The package's repository, laid out the way Maven Central serves it.
        maven { url = uri(System.getenv("WSE_MAVEN_REPO") ?: error("WSE_MAVEN_REPO names the repository with the package")) }
        mavenCentral()
    }
}
