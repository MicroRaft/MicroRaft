
# Setup

MicroRaft JARs are available via [Jitpack](https://jitpack.io/#MicroRaft/MicroRaft). If you are
using Maven, just add the following lines to your your `pom.xml`:

~~~~{.xml}
<repositories>
	<repository>
		<id>jitpack.io</id>
		<url>https://jitpack.io</url>
	</repository>
</repositories>
~~~~

~~~~{.xml}
<dependency>
    <groupId>com.github.MicroRaft</groupId>
	<artifactId>MicroRaft</artifactId>
	<version>v0.1</version>
</dependency>
~~~~

If you don't have Maven but want to build the project on your machine, `mvnw` is
available in the MicroRaft repository. Just hit the following command on your
terminal.

```
gh repo clone MicroRaft/MicroRaft && cd MicroRaft && ./mvnw clean package
``` 

Then you can get the JARs from `microraft/target`, `microraft-hocon/target`, and
`microraft-yaml/target` directories.

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
