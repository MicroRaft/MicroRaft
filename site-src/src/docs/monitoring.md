
# Monitoring

<a
href="https://github.com/MicroRaft/MicroRaft/blob/master/microraft/src/main/java/io/microraft/report/RaftNodeReport.java"
target="_blank">`RaftNodeReport`</a> contains detailed information about
internal state of a `RaftNode`, such as its Raft role, term, leader, last log
index, and commit index. We can feed our external monitoring systems with these
information pieces as follows:  

1. We can build a simple _pull-based_ system to query `RaftNodeReport` objects
   via `RaftNode.getReport()` and publish those objects to any external
   monitoring system.

2. MicroRaft contains another abstraction, <a
   href="https://github.com/MicroRaft/MicroRaft/blob/master/microraft/src/main/java/io/microraft/report/RaftNodeReportListener.java"
   target="_blank">`RaftNodeReportListener`</a> which is called by Raft nodes
   anytime there is an important change in the internal Raft state, such as
   leader change, term change, or snapshot installation. We can also use this
   abstraction to capture `RaftNodeReport` objects and notify external
   monitoring systems promptly with a _push-based_ approach.

-----

## Micrometer integration

MicroRaft offers a module to publish Raft node metrics to external systems
easily via the <a href="https://micrometer.io/" target="_blank">Micrometer</a>
project. Just add the following dependency to the classpath for using the
Micrometer integration.

~~~~{.xml}
<dependency>
	<groupId>io.microraft</groupId>
	<artifactId>microraft-metrics</artifactId>
	<version>0.1</version>
</dependency>
~~~~

<a
href="https://github.com/MicroRaft/MicroRaft/blob/master/microraft-metrics/src/main/java/io/microraft/metrics/RaftNodeMetrics.java"
target="_blank">`RaftNodeMetrics`</a> implements the <a
href="https://github.com/MicroRaft/MicroRaft/blob/master/microraft/src/main/java/io/microraft/report/RaftNodeReportListener.java"
target="_blank">`RaftNodeReportListener`</a> interface and can be injected into
created `RaftNode` instances via `RaftNodeBuilder.setRaftNodeReportListener()`.
Then, several metrics extracted from published `RaftNodeReport` objects are
passed to meter registries.
