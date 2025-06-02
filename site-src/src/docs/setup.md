
# Setup

MicroRaft JARs are available via Maven Central. If you are
using Maven, just add the following lines to your your build tool dependency config:

Gradle (version catalog)
```toml
[versions]
microraft = "0.9"

[libraries]
microraft = { module = "io.microraft:microraft", version.ref = "microraft" }
```

Gradle (kotlinscript)
```kotlin
implementation("io.microraft:microraft:0.9")
```

Maven
```xml
<dependency>
	<groupId>io.microraft</groupId>
	<artifactId>microraft</artifactId>
	<version>0.9</version>
</dependency>
```

If you don't have Gradle but want to build the project on your machine, ./gradlew is
available in the MicroRaft repository. Just hit the following command on your
terminal.

```
gh repo clone MicroRaft/MicroRaft && cd MicroRaft && ./gradlew build
``` 

Then you can get the JARs from `microraft/build/libs`, `microraft-hocon/build/libs`, and
`microraft-yaml/build/libs` directories.

-----

## Logging

MicroRaft depends on the <a href="http://www.slf4j.org/" target="_blank">SLF4J
library</a> for logging. Actually it is the only dependency of MicroRaft. Make
sure you enable the `INFO` logging level for the `io.microraft` package. If you
are going hard, you can also give the `DEBUG` level a shot, but I assure you it
will be a bumpy ride.

-----

## What is next?

OK. You have the MicroRaft JAR in your classpath, and the logging also looks
good. Now you are ready to build your CP distributed system! Then why don't you
start with [checking out the main abstractions](main-abstractions.md) defined in
MicroRaft?
