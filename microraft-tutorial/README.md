# MicroRaft Tutorial

Gradle (version catalog)

```toml
[versions]
microraft-tutorial = "0.9"

[libraries]
microraft-tutorial = { module = "io.microraft:microraft-tutorial", version.ref = "microraft-tutorial" }
```

Gradle (kotlinscript)

```kotlin
implementation("io.microraft:microraft-tutorial:0.9")
```

Maven

```xml
<dependency>
    <groupId>io.microraft</groupId>
    <artifactId>microraft-tutorial</artifactId>
    <version>0.9</version>
</dependency>
```