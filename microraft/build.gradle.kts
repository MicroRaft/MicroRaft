plugins {
    `java-library`
    `java-test-fixtures`
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

metadata {
    moduleName = "io.microraft"
    readableName = "Microraft"
    description = "Feature-complete implementation of the Raft consensus algorithm"
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

tasks.javadoc {
    exclude("io/microraft/model/**")
    exclude("**/impl/**")
}

dependencies {
    implementation(libs.slf4j.api)
    compileOnly(libs.findbugs.annotations)
}

@Suppress("UnstableApiUsage") //
testing {
    suites {
        withType<JvmTestSuite> {
            useJUnit(libs.versions.junit)
        }
        named<JvmTestSuite>("test") {
            targets.all {
                testTask.configure {
                    maxParallelForks = 4
                }
            }
            dependencies {
                implementation(libs.assertj)
                implementation(libs.mockito)
                runtimeOnly(libs.log4j.slf4j.impl)
                compileOnly(libs.findbugs.annotations)
            }
        }
    }
}

dependencies {
    testFixturesImplementation(libs.junit)
    testFixturesImplementation(libs.slf4j.api)
    testFixturesCompileOnly(libs.findbugs.annotations)
}

// Do not publish test fixtures for now
val javaComponent = components["java"] as AdhocComponentWithVariants
javaComponent.withVariantsFromConfiguration(configurations.testFixturesApiElements.get()) { skip() }
javaComponent.withVariantsFromConfiguration(configurations.testFixturesRuntimeElements.get()) { skip() }

publishing {
    // TODO Remove after debugging
    repositories {
        maven {
            this.name = "TestPublish"
            // change URLs to point to your repos, e.g. http://my.org/repo
            val releasesRepoUrl = uri(layout.buildDirectory.dir("repos/releases"))
            val snapshotsRepoUrl = uri(layout.buildDirectory.dir("repos/snapshots"))
            url = if (version.toString().endsWith("SNAPSHOT")) snapshotsRepoUrl else releasesRepoUrl
        }
    }

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
