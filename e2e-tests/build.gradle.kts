plugins {
    java
    id("io.spring.dependency-management")
}

val jqwikVersion: String by project
val testcontainersVersion: String by project

dependencyManagement {
    imports {
        mavenBom("org.springframework.boot:spring-boot-dependencies:3.3.5")
    }
}

dependencies {
    testImplementation(project(":platform-common"))
    testImplementation("org.springframework.boot:spring-boot-starter-test")
    testImplementation("org.springframework.boot:spring-boot-starter-web")
    testImplementation("org.testcontainers:junit-jupiter:$testcontainersVersion")
    testImplementation("org.testcontainers:postgresql:$testcontainersVersion")
    testImplementation("org.testcontainers:redpanda:$testcontainersVersion")
    testImplementation("org.testcontainers:toxiproxy:$testcontainersVersion")
    testRuntimeOnly("org.postgresql:postgresql")
    testImplementation("net.jqwik:jqwik:$jqwikVersion")
}
