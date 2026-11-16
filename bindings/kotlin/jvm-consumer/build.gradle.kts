plugins {
    java
    application
}

dependencies {
    implementation("io.github.silvermpx:wse-client-jvm:${System.getenv("WSE_VERSION") ?: error("WSE_VERSION names the package's version")}")
}

// Java 17's API, the oldest Java the package runs on.
tasks.withType<JavaCompile>().configureEach { options.release.set(17) }

application { mainClass.set("dev.wse.consumer.Main") }
