# MicroRaft

This module contains the source code of MicroRaft along with its unit and 
integration test suite. 

Gradle (version catalog)
```toml
[versions]
microraft = "0.9"

[libraries]
microraft = { group = "io.microraft", name = "microraft", version.ref = "microraft" }
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