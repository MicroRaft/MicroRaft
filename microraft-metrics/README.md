# MicroRaft metrics with Micrometer integration

Add the following dependency to the classpath for publishing MicroRaft metrics
to external monitoring systems via
<a href="https://micrometer.io/" target="_blank">Micrometer</a>.

Gradle (version catalog)

```toml
[versions]
microraft-metrics = "0.9"

[libraries]
microraft-metrics = { module = "io.microraft:microraft-metrics", version.ref = "microraft-metrics" }
```

Gradle (kotlinscript)

```kotlin
implementation("io.microraft:microraft-metrics:0.9")
```

Maven

```xml
<dependency>
    <groupId>io.microraft</groupId>
    <artifactId>microraft-metrics</artifactId>
    <version>0.9</version>
</dependency>
```

<a href="https://github.com/MicroRaft/MicroRaft/blob/master/microraft-metrics/src/main/java/io/microraft/metrics/RaftNodeMetrics.java" target="_blank">
`RaftNodeMetrics`</a>
implements the
<a href="https://github.com/MicroRaft/MicroRaft/blob/master/microraft/src/main/java/io/microraft/report/RaftNodeReportListener.java" target="_blank">
`RaftNodeReportListener`</a>
interface and can be injected into created `RaftNode` instances via
`RaftNodeBuilder.setRaftNodeReportListener()`. Then, several metrics extracted
from published `RaftNodeReport` objects are passed to meter registries. 
