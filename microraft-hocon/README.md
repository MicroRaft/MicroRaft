# MicroRaft HOCON Config Parser

```
<dependency>
	<groupId>io.microraft</groupId>
	<artifactId>microraft-hocon</artifactId>
	<version>1.0</version>
</dependency>
```

This project enables you to create `RaftConfig` objects from HOCON files 
easily, as shown below:

```
String configFilePath = "...";
Config hoconConfig = ConfigFactory.parseFile(new File(configFilePath));
RaftConfig raftConfig = HoconRaftConfigParser.parseConfig(hoconConfig);
``` 

Other than reading your config from a file, you can create your HOCON `Config`
object in any other way and then parse it via 
`HoconRaftConfigParser.parseConfig()`.

[microraft-default.conf](https://github.com/MicroRaft/MicroRaft/blob/master/microraft-hocon/microraft-default.conf) 
is the default MicroRaft HOCON configuration file. 

Please refer to 
[MicroRaft documentation page](https://microraft.io/user-guide/configuration/) 
to learn more about configuration. 
