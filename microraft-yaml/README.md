# MicroRaft YAML Config Parser

Gradle (version catalog)
```toml
[versions]
microraft-yaml = "0.9"

[libraries]
microraft-yaml = { group = "io.microraft", name = "microraft-yaml", version.ref = "microraft-yaml" }
```

Gradle (kotlinscript)
```kotlin
implementation("io.microraft:microraft-yaml:0.9")
```

Maven
```xml
<dependency>
	<groupId>io.microraft</groupId>
	<artifactId>microraft-yaml</artifactId>
	<version>0.9</version>
</dependency>
```

This project enables you to create `RaftConfig` objects from YAML files 
easily, as shown below:

```
String configFilePath = "...";
RaftConfig raftConfig = YamlRaftConfigParser.parseFile(new Yaml(), configFilePath);
``` 

Other than reading your config from a file, `YamlRaftConfigParser` also offers 
a few other parsing methods.  

[microraft-default.yaml](https://github.com/MicroRaft/MicroRaft/blob/master/microraft-yaml/microraft-default.yaml) 
is the default MicroRaft YAML configuration file. 

Please refer to 
[MicroRaft documentation page](https://microraft.io/docs/configuration/) 
to learn more about configuration. 
