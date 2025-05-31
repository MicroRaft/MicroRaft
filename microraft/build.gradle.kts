plugins {
    `java-library`
    `java-test-fixtures`
    alias(libs.plugins.defaults)
    alias(libs.plugins.metadata)
    alias(libs.plugins.javadocLinks)
}

group = "io.microraft"
version = "0.9-SNAPSHOT"
description = "Feature-complete implementation of the Raft consensus algorithm"

metadata {
    moduleName.set("io.microraft.microraft")
    readableName.set("Microraft")
    license {
        apache2()
    }
    organization {
        name.set("MicroRaft")
        url.set("https://microraft.io")
    }
    developers {
        register("metanet") {
            fullName.set("Ensar Basri Kahveci")
            email.set("ebkahveci@gmail.com")
        }
        register("mdogan") {
            fullName.set("Mehmet Dogan")
            email.set("mehmet@dogan.io")
        }
    }
    github {
        org.set("MicroRaft")
        pages()
        issues()
    }
}

java {
    toolchain {
        languageVersion = JavaLanguageVersion.of(11)
    }
    withJavadocJar()
    withSourcesJar()
}

tasks.withType<Jar>().configureEach {
    manifest.attributes(
        "Implementation-Title" to project.name,
        "Implementation-Vendor" to metadata.organization.provider.flatMap { it.name },
        "Implementation-Version" to provider { project.version.toString() },
    )
}

tasks.javadoc {
    isFailOnError = false
    exclude("io/microraft/model/**")
    exclude("**/impl/**")
}


repositories {
    mavenCentral()
}

dependencies {
    compileOnly(libs.findbugs)
    implementation(libs.slf4j)
}


@Suppress("UnstableApiUsage") //
testing {
    suites {
        withType(JvmTestSuite::class) {
            useJUnit(libs.versions.junit)
        }

        val test by getting(JvmTestSuite::class) {
            dependencies {
                implementation(libs.mockito)
                implementation(libs.assertj)
                implementation(libs.log4j.core)
                implementation(libs.log4j.api)
                implementation(libs.log4j.slf4j.impl)
                compileOnly(libs.findbugs)
            }
        }
    }
}

dependencies {
    testFixturesImplementation(libs.junit)
    testFixturesCompileOnly(libs.findbugs)
    testFixturesImplementation(libs.slf4j)
}
