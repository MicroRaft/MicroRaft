rootProject.name = "MicroRaft"

dependencyResolutionManagement {
    @Suppress("UnstableApiUsage") //
    repositories {
        mavenCentral()
    }
}

include("microraft")
include("microraft-hocon")
include("microraft-metrics")
include("microraft-store-sqlite")
include("microraft-tutorial")
include("microraft-yaml")
