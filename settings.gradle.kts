rootProject.name = "MicroRaft"

include("microraft")
include("microraft-hocon")
include("microraft-metrics")
include("microraft-store-sqlite")
include("microraft-tutorial")
include("microraft-yaml")

dependencyResolutionManagement {
    @Suppress("UnstableApiUsage") //
    repositories {
        mavenCentral()
    }
}
