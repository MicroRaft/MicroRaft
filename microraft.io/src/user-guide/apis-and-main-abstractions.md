
## APIs and main abstractions

MicroRaft's APIs and main abstractions are listed below. 

### `RaftConfig`

<a href="https://github.com/MicroRaft/MicroRaft/blob/master/microraft/src/main/java/io/microraft/RaftConfig.java" target="_blank">`RaftConfig`</a> 
contains configuration options related to the Raft consensus algorithm and 
MicroRaft's implementation. Please check the 
[Configuration section](../../user-guide/configuration/) for details.


### `RaftNode`

<a href="https://github.com/MicroRaft/MicroRaft/blob/master/microraft/src/main/java/io/microraft/RaftNode.java" target="_blank">`RaftNode`</a> 
runs the Raft consensus algorithm as a member of a Raft group. A Raft group is
a cluster of `RaftNode` instances that runs the Raft consensus algorithm. 
`RaftNode` interface contains APIs for replicating operations, performing 
queries, applying membership changes in the Raft group, handling Raft RPCs and 
responses, etc.

A single JVM instance can run multiple `RaftNode`s that belong to different 
Raft groups or even the same Raft group. 

A `RaftNode` uses the `RaftNodeRuntime` abstraction to run the Raft consensus
algorithm on the underlying platform and the `StateMachine` abstraction to run
committed operations on a user-specified state machine. 


### `RaftEndpoint`

<a href="https://github.com/MicroRaft/MicroRaft/blob/master/microraft/src/main/java/io/microraft/RaftEndpoint.java" target="_blank">`RaftEndpoint`</a> 
represents an endpoint that participates to at least one Raft group and 
executes the Raft consensus algorithm with a `RaftNode`.
 
`RaftNode` differentiates members of a Raft group with a unique id. MicroRaft
users need to provide a unique id for each `RaftEndpoint`. Other than this 
information, `RaftEndpoint` implementations can contain custom fields, such as
network addresses and tags, to be utilized by `RaftNodeRuntime` 
implementations.


### `RaftRole`

<a href="https://github.com/MicroRaft/MicroRaft/blob/master/microraft/src/main/java/io/microraft/RaftRole.java" target="_blank">`RaftRole`</a> 
denotes the roles of `RaftNode`s as specified in the Raft consensus algorithm. 
Currently, MicroRaft implements the main roles defined in the paper: `LEADER`, 
`CANDIDATE`, and `FOLLOWER`. The popular extension roles, such as `LEARNER` and 
`WITNESS`, are not implemented yet, but they are on the roadmap.


### `RaftNodeStatus`

<a href="https://github.com/MicroRaft/MicroRaft/blob/master/microraft/src/main/java/io/microraft/RaftNodeStatus.java" target="_blank">`RaftNodeStatus`</a> 
denotes the statuses of a `RaftNode` during its own and its Raft group's 
lifecycle. A `RaftNode` is in the `INITIAL` status when it is created, and
moves to the `ACTIVE` status when it is started with a `RaftNode.start()` call. 
It stays in this status until either a membership change is triggered 
in the Raft group, or either the Raft group or Raft node is terminated. 


### `RaftNodeRuntime`

<a href="https://github.com/MicroRaft/MicroRaft/blob/master/microraft/src/main/java/io/microraft/runtime/RaftNodeRuntime.java" target="_blank">`RaftNodeRuntime`</a> 
enables execution of the Raft consensus algorithm by providing capabilities for 
task scheduling & execution, serialization & deserialization of Raft messages, 
and networking.

A `RaftNode` runs in a single-threaded manner. Even if a `RaftNodeRuntime` 
implementation makes use of multiple threads internally, it must ensure 
the serial execution and 
<a href="https://docs.oracle.com/javase/specs/jls/se8/html/jls-17.html" target="_blank">happens-before relationship</a>
for the tasks submitted by a single `RaftNode`.


### `StateMachine`

<a href="https://github.com/MicroRaft/MicroRaft/blob/master/microraft/src/main/java/io/microraft/statemachine/StateMachine.java" target="_blank">`StateMachine`</a> 
enables users to implement arbitrary services, such as an atomic register or 
a key-value store, and execute operations on them. 

`RaftNode` does not deal with the actual logic of committed operations. Once
a given operation is committed with the Raft consensus algorithm, i.e., it is
replicated to the majority of the Raft group, the operation is passed to 
the provided `StateMachine` implementation. It is 
the `StateMachine` implementation's responsibility to ensure deterministic 
execution of committed operations.
 
The operations committed in a `RaftNode` are executed in the same thread that 
runs the Raft consensus algorithm. Since `RaftNodeRuntime` ensures 
the thread-safe execution of `RaftNode` tasks, `StateMachine` implementations 
do not need to be thread-safe.


### `RaftStore` and `RestoredRaftState`

<a href="https://github.com/MicroRaft/MicroRaft/blob/master/microraft/src/main/java/io/microraft/persistence/RaftStore.java" target="_blank">`RaftStore`</a> 
is used for persisting the internal state of the Raft consensus algorithm. Its 
implementations must provide the durability guarantees defined 
in the interface.

If a `RaftNode` crashes, its persisted state could be read back from stable 
storage into a 
<a href="https://github.com/MicroRaft/MicroRaft/blob/master/microraft/src/main/java/io/microraft/persistence/RestoredRaftState.java" target="_blank">`RestoredRaftState`</a> 
object and the `RaftNode` could be restored back. `RestoredRaftState` contains 
all the necessary information to recover `RaftNode` instances from crashes.

![](/img/info.png){: style="height:25px;width:25px"} `RaftStore` does not 
persist internal state of `StateMachine` implementations. Upon recovery, 
a `RaftNode` starts with an empty state of the state machine, discovers 
the current commit index and re-executes all the operations in the Raft log up
to the commit index to re-populate the state machine. 


### `RaftModel` and `RaftModelFactory`

<a href="https://github.com/MicroRaft/MicroRaft/blob/master/microraft/src/main/java/io/microraft/model/RaftModel.java" target="_blank">`RaftModel`</a>
is the base interface for the objects that hit network and persistent storage. 
There are 2 other interfaces extending this interface:
<a href="https://github.com/MicroRaft/MicroRaft/blob/master/microraft/src/main/java/io/microraft/model/log/BaseLogEntry.java" target="_blank">`BaseLogEntry`</a> 
and <a href="https://github.com/MicroRaft/MicroRaft/blob/master/microraft/src/main/java/io/microraft/model/message/RaftMessage.java" target="_blank">`RaftMessage`</a>. 
`BaseLogEntry` is used for representing log and snapshot entries stored in 
the Raft log. `RaftMessage` is used for Raft RPCs and their responses. Please 
see the interfaces inside 
<a href="https://github.com/MicroRaft/MicroRaft/tree/master/microraft/src/main/java/io/microraft/model" target="_blank">`io.microraft.model`</a> 
for more details. In addition, there is a 
<a href="https://github.com/MicroRaft/MicroRaft/blob/master/microraft/src/main/java/io/microraft/model/RaftModelFactory.java" target="_blank">`RaftModelFactory`</a> 
interface for creating `RaftModel` objects. 

MicroRaft comes with a default POJO-style implementation of these interfaces 
available under the 
<a href="https://github.com/MicroRaft/MicroRaft/tree/master/microraft/src/main/java/io/microraft/model/impl" target="_blank">`io.microraft.model.impl`</a> 
package.


### `RaftException`

<a href="https://github.com/MicroRaft/MicroRaft/blob/master/microraft/src/main/java/io/microraft/exception/RaftException.java" target="_blank">`RaftException`</a>
is the base class for Raft-related exceptions. MicroRaft defines a number of 
<a href="https://github.com/MicroRaft/MicroRaft/tree/master/microraft/src/main/java/io/microraft/exception" target="_blank">custom exceptions</a> 
to report some certain failure scenarios to clients. 


## How to run a raft group 

In order to run a Raft group (i.e., Raft cluster) using MicroRaft, we need to:

* implement the `RaftEndpoint` interface to identify `RaftNode`s we are going 
to run,
 
* provide a `StateMachine` implementation for the actual state machine logic 
(key-value store, atomic register, etc.),

* __(optional)__ implement the `RaftModel` and `RaftModelFactory` interfaces to 
create Raft RPC request / response objects and Raft log entries, or simply 
use the default POJO-style implementation of these interfaces available under
the `io.microraft.model.impl` package,

* provide a `RafNodeRuntime` implementation to realize task execution, 
serialization of `RaftModel` objects and networking, 

* __(optional)__ provide a `RaftStore` implementation if we want to restore
crashed Raft nodes. We can persist the internal Raft node state to stable
storage via the `RaftStore` interface and recover from Raft node crashes by
restoring persisted Raft state. Otherwise, we could use the already-existing
`NopRaftStore` utility which makes the internal Raft state volatile and 
disables crash-recovery. If we don't implement persistence, crashed Raft nodes
cannot be restarted and need to be removed from the Raft group to not to damage
availability. Note that the lack of persistence limits the overall fault 
tolerance capabilities of Raft groups. Please refer to the 
[Resiliency and Fault Tolerance](resiliency-and-fault-tolerance.md) section to
learn more about MicroRaft's fault tolerance capabilities. 

* ![](/img/info.png){: style="height:25px;width:25px"} build a discovery and 
RPC mechanism to replicate and commit our operations on a Raft group. This is 
required because simplicity is the primary concern for MicroRaft's design
philosophy. MicroRaft offers a minimum API set to cover the fundamental
functionality and enables its users to implement higher-level abstractions, 
such as an RPC system with request routing, retries, and deduplication. For 
instance, MicroRaft neither broadcasts the Raft group members to any external 
discovery system, nor integrates with any observability tool, but it exposes
all necessary information, such as the current Raft group members, the leader
endpoint, term, commit index. via `RaftNode.getReport()`. We can use that
information to feed our discovery services and monitoring tools. Similarly, 
the public APIs on `RaftNode` do not employ request routing or retry 
mechanisms. For instance, if a client tries to replicate an operation via a
follower or a candidate `RaftNode`, `RaftNode` responds back with a
`NotLeaderException`, instead of internally forwarding the operation to 
the leader `RaftNode`. `NotLeaderException` contains `RaftEndpoint` of 
the current leader `RaftNode` so that clients can send their request to it.

In the next section, we will build an atomic register on top of MicroRaft to
demonstrate how to implement and use MicroRaft's main abstractions.

 
## Architectural overview of a Raft group

The following figure depicts an architectural overview of a Raft group based on
the main abstractions explained above. Clients talk to the leader `RaftNode` 
for replicating operations. They can talk to both the leader and follower 
`RaftNode`s for running queries with different consistency guarantees. 
`RaftNode` uses `RaftModelFactory` to create Raft log entries, snapshot 
entries, and Raft RPC request and response objects. Each `RaftNode` talks to
its `RaftNodeRuntime` to communicate with the other `RaftNode`s and execute the 
Raft consensus algorithm. It also uses `RaftStore` to persist Raft log entries
to stable storage. Last, once a log entry is committed, i.e, it is successfully
replicated to the majority of the Raft group, its operation is passed to 
`StateMachine` for execution, and output of the execution is returned to the 
client by the leader `RaftNode`.

![Integration](/img/architectural_overview.png){: style="height:592px;width:800px"}


## What is next?

In the [next section](tutorial-building-an-atomic-register.md), we will build an atomic
register on top of MicroRaft.

