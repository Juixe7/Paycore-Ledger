import com.google.protobuf.gradle.id

plugins {
    `java-library`
    id("com.google.protobuf")
}

val protobufVersion: String by project

dependencies {
    api("com.google.protobuf:protobuf-java:$protobufVersion")
    api("com.google.protobuf:protobuf-java-util:$protobufVersion")
    api("com.fasterxml.jackson.core:jackson-databind:2.17.2")
    api("com.fasterxml.jackson.datatype:jackson-datatype-jsr310:2.17.2")

    testImplementation("org.junit.jupiter:junit-jupiter:5.10.5")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
    testImplementation("org.assertj:assertj-core:3.26.3")
}

protobuf {
    protoc {
        artifact = "com.google.protobuf:protoc:$protobufVersion"
    }
}

// CI Schema Compatibility Check (Section 5.1 & 7.11):
// Verifies that payment_events.proto never reuses or renumbers core wire tags.
val checkProtoCompatibility by tasks.registering {
    group = "verification"
    description = "Verifies backward compatibility of Protobuf event contracts"
    val protoFile = file("src/main/proto/paycore/events/v1/payment_events.proto")
    inputs.file(protoFile)
    doLast {
        val content = protoFile.readText()
        val requiredFieldTags = listOf(
            "string event_id = 1;",
            "string event_type = 2;",
            "uint32 schema_version = 3;",
            "string payment_id = 4;",
            "string merchant_id = 5;",
            "uint64 event_seq = 6;",
            "google.protobuf.Timestamp occurred_at = 7;",
            "map<string, string> trace_context = 8;",
            "PaymentAuthorized authorized = 20;",
            "PaymentCaptured captured = 21;",
            "RefundProcessed refunded = 22;",
            "PayoutCreated payout = 23;"
        )
        requiredFieldTags.forEach { expected ->
            if (!content.contains(expected)) {
                throw GradleException("Incompatible Protobuf schema change detected! Missing or altered wire contract: '$expected'")
            }
        }
    }
}

tasks.named("check") {
    dependsOn(checkProtoCompatibility)
}
