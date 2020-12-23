
# Configuration

MicroRaft is a lightweight library with a minimal feature set, yet it allows
users to fine-tune its behaviour. In this section, we elaborate the
configuration parameters available in MicroRaft.

`RaftConfig` is an immutable configuration class containing a number of
parameters to tune behaviour of your Raft nodes. You can populate it via
`RaftConfigBuilder`. Your software in which MicroRaft is embedded could be
already working with HOCON or YAML files. MicroRaft also offers HOCON and YAML
parsers to populate `RaftConfig` objects for such cases. Once you create a
`RaftConfig` object either programmatically, or by parsing a HOCON or YAML file,
you can provide it to `RaftNodeBuilder` while building `RaftNode` instances.

You can see MicroRaft's configuration parameters below:

* __Leader election timeout milliseconds:__

Duration of leader election rounds in milliseconds. If a candidate cannot win
majority votes before this timeout elapses, a new leader election round is
started. See "§ 5.2: Leader Election" in the [Raft
paper](https://raft.github.io/raft.pdf)) for more details about how leader
elections work in Raft.

* __Leader heartbeat timeout seconds:__

Duration in seconds for a follower to decide on failure of the current leader
and start a new leader election round. If this duration is too short, a leader
could be considered as failed unnecessarily in case of a small hiccup. If it is
too long, it takes longer to detect an actual failure.

The Raft paper uses a single parameter, _election timeout_, to both detect
failure of the leader and perform a leader election round. A follower decides
the current leader to be crashed if the _election timeout_ elapses after the
last received `AppendEntriesRPC`. Moreover, a candidate starts a new leader
election round if the _election timeout_ elapses before it acquires the majority
votes or another candidate announces the leadership. The Raft paper discusses
the _election timeout_ to be configured around 500 milliseconds. Even though
such a low timeout value works just fine for leader elections, we have
experienced that it causes false positives for failure detection of the leader
when there is a hiccup in the system. When a leader slows down temporarily for
any reason, its followers may start a new leader election round very quickly. To
prevent such problems and allow users to tune availability of the system with
more granularity, MicroRaft uses another parameter, _leader heartbeat timeout_,
to detect failure of the leader.

Please note that having 2 timeout parameters does not make any difference in
terms of the safety property. If both values are configured the same,
MicroRaft's behaviour becomes identical to the behaviour described in the Raft
paper.

* __Leader heartbeat period seconds:__

Duration in seconds for a Raft leader node to send periodic heartbeat requests
to its followers in order to denote its liveliness. Periodic heartbeat requests
are actually append entries requests and can contain log entries. A periodic 
heartbeat request is not sent to a follower if an append entries request has
been sent to that follower recently.

* __Maximum pending log entry count:__

Maximum number of pending log entries in the leader's Raft log before
temporarily rejecting new requests of clients. This configuration enables a back
pressure mechanism to prevent OOME when a Raft leader cannot keep up with the
requests sent by the clients. When the _pending log entries buffer_ whose
capacity is specified with this configuration field is filled, new requests fail
with `CannotReplicateException` to slow down clients. You can configure this
field by considering the degree of concurrency of your clients.

* __Append entries request batch size:__

In MicroRaft, a leader Raft node sends log entries to its followers in batches
to improve the throughput. This configuration parameter specifies the maximum
number of Raft log entries that can be sent as a batch in a  single append
entries request.

* __Commit count to take snapshot:__

Number of new commits to initiate a new snapshot after the last snapshot taken
by a Raft node. This value must be configured wisely as it effects performance
of the system in multiple ways. If a small value is set, it means that snapshots
are taken too frequently and Raft nodes keep a very short Raft log. If snapshot
objects are large and the Raft state is persisted to disk, this can create an
unnecessary overhead on IO performance. Moreover, a Raft leader can send too
many snapshots to slow followers which can create a network overhead. On the
other hand, if a very large value is set, it can create a memory overhead since
Raft log entries are going to be kept in memory until the next snapshot.

* __Transfer snapshots from followers:__

MicroRaft's Raft log design ensures that every Raft node takes a snapshot at
exactly the same log index. This behaviour enables an optimization. When a
follower falls far behind the Raft leader and needs to install a snapshot, it
transfers the snapshot chunks from both the Raft leader and other followers in
parallel. By this way, we utilize the bandwidth of the followers to reduce the
load on the leader and speed up the snapshot transfer process.

* __Raft node report publish period seconds:__

It denotes how frequently a Raft node publishes a report of its internal Raft
state. `RaftNodeReport` objects can be used for monitoring a running Raft group.

-----

## HOCON Configuration

`RaftConfig` objects can be populated from HOCON files easily if you add the
`microraft-hocon` dependency to your classpath.

~~~~{.xml}
<dependency>
	<groupId>io.microraft</groupId>
	<artifactId>microraft-hocon</artifactId>
	<version>1.0</version>
</dependency>
~~~~

You can see a HOCON config string below:

~~~~{.hocon}
raft {
  leader-election-timeout-millis: 1000
  leader-heartbeat-timeout-secs: 10
  leader-heartbeat-period-secs: 2
  max-pending-log-entry-count: 5000
  append-entries-request-batch-size: 1000
  commit-count-to-take-snapshot: 50000
  transfer-snapshots-from-followers-enabled: true
  raft-node-report-publish-period-secs: 10
}
~~~~

You can parse a HOCON file as shown below:

~~~~{.java}
String configFilePath = "...";
Config hoconConfig = ConfigFactory.parseFile(new File(configFilePath));
RaftConfig raftConfig = HoconRaftConfigParser.parseConfig(hoconConfig);
~~~~

Please refer to
[MicroRaft HOCON project](https://github.com/MicroRaft/MicroRaft/tree/master/microraft-hocon) 
for details.

-----

## YAML Configuration

Similarly, `RaftConfig` objects can be populated from YAML files easily if you
add the `microraft-yaml` dependency to your classpath.

~~~~{.xml}
<dependency>
	<groupId>io.microraft</groupId>
	<artifactId>microraft-yaml</artifactId>
	<version>1.0</version>
</dependency>
~~~~

You can see a YAML config string below:

~~~~{.yaml}
raft:
 leader-election-timeout-millis: 1000
 leader-heartbeat-timeout-secs: 10
 leader-heartbeat-period-secs: 2
 max-pending-log-entry-count: 5000
 append-entries-request-batch-size: 1000
 commit-count-to-take-snapshot: 50000
 transfer-snapshots-from-followers-enabled: true
 raft-node-report-publish-period-secs: 10
~~~~

You can parse a YAML file as shown below:

~~~~{.java}
String configFilePath = "...";
RaftConfig raftConfig = YamlRaftConfigParser.parseFile(new Yaml(), configFilePath);
~~~~

Please refer to [MicroRaft YAML
project](https://github.com/MicroRaft/MicroRaft/tree/master/microraft-yaml) for
details.
