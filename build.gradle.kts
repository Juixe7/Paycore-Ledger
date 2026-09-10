import org.gradle.api.tasks.testing.logging.TestExceptionFormat
import org.gradle.api.tasks.testing.logging.TestLogEvent

plugins {
    java
    id("org.springframework.boot") version "3.3.5" apply false
    id("io.spring.dependency-management") version "1.1.6" apply false
    id("com.google.protobuf") version "0.9.4" apply false
}

val serviceModules = setOf(
    "payments-core",
    "webhook-service",
    "read-model-service",
    "reconciliation",
    "mock-psp"
)

allprojects {
    group = "com.paycore"
    version = property("paycore.version").toString()

    repositories {
        mavenCentral()
    }
}

subprojects {
    apply(plugin = "java")

    java {
        toolchain {
            languageVersion.set(JavaLanguageVersion.of(21))
        }
    }

    tasks.withType<JavaCompile>().configureEach {
        options.encoding = "UTF-8"
        options.compilerArgs.add("-parameters")
    }

    tasks.withType<Test>().configureEach {
        useJUnitPlatform()
        testLogging {
            events = setOf(TestLogEvent.PASSED, TestLogEvent.SKIPPED, TestLogEvent.FAILED)
            exceptionFormat = TestExceptionFormat.FULL
            showStandardStreams = false
        }
    }

    // Architectural Rule (Section 5.7 & 7.16):
    // Deployable services never import each other; they interact only through Protobuf events (platform-common) and published APIs.
    if (project.name in serviceModules) {
        configurations.configureEach {
            dependencies.withType<ProjectDependency>().configureEach {
                val targetName = dependencyProject.name
                if (targetName in serviceModules && targetName != project.name) {
                    throw GradleException(
                        "Architecture violation: Service module '${project.name}' must not depend on service module '$targetName'. " +
                            "Services may only share contracts via ':platform-common'."
                    )
                }
            }
        }
    }
}
