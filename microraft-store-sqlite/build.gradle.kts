plugins {
    `java-library`
    alias(libs.plugins.defaults)
    alias(libs.plugins.metadata)
    alias(libs.plugins.javadocLinks)
    `maven-publish`
    signing
    alias(libs.plugins.mavenCentralPublishing)
    alias(libs.plugins.spotbugs)
    checkstyle
}

group = "io.microraft"
version = "0.9-SNAPSHOT"
description = ""

metadata {
    moduleName = "io.microraft.store.sqlite"
    readableName = "MicroRaft SQLite store"
    license {
        apache2()
    }
    organization {
        name = "MicroRaft"
        url = "https://microraft.io"
    }
    developers {
        register("metanet") {
            fullName = "Ensar Basri Kahveci"
            email = "ebkahveci@gmail.com"
        }
        register("mdogan") {
            fullName = "Mehmet Dogan"
            email = "mehmet@dogan.io"
        }
    }
    github {
        org = "MicroRaft"
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


dependencies {
    api(project(":microraft"))
    implementation(libs.jooq)
    implementation(libs.sqlite)
    compileOnly(libs.findbugs.annotations)
}


@Suppress("UnstableApiUsage") //
testing {
    suites {
        withType<JvmTestSuite> {
            useJUnit(libs.versions.junit)
        }

        named<JvmTestSuite>("test") {
            dependencies {
                implementation(testFixtures(project(":microraft")))
                implementation(libs.assertj)
                implementation(libs.jackson.databind)
                runtimeOnly(libs.log4j.slf4j.impl)
                compileOnly(libs.findbugs.annotations)
            }
        }
    }
}


publishing {
    publications {
        create<MavenPublication>("main") {
            from(components["java"])
        }
    }
}

signing {
    val signingKey: String? by project
    val signingPassword: String? by project
    useInMemoryPgpKeys(signingKey, signingPassword)
    sign(publishing.publications["main"])
}