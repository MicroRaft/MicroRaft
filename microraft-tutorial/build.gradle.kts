plugins {
    `java-library`
    alias(libs.plugins.defaults)
    alias(libs.plugins.metadata)
    alias(libs.plugins.javadocLinks)
}

group = "io.microraft"
version = "0.9-SNAPSHOT"
description = ""

metadata {
    moduleName.set("io.microraft.microraft-tutorial")
    readableName.set("MicroRaft Tutorial")
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
}


repositories {
    mavenCentral()
}

dependencies {
    implementation(project(":microraft"))
    implementation(libs.slf4j)
    compileOnly(libs.findbugs)
}


@Suppress("UnstableApiUsage") //
testing {
    suites {
        withType(JvmTestSuite::class) {
            useJUnit(libs.versions.junit)
        }

        val test by getting(JvmTestSuite::class) {
            dependencies {
                implementation(libs.assertj)
            }
        }
    }
}