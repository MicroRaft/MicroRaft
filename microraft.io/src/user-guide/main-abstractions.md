
The main APIs and abstractions of MicroRaft are listed below. 


## RaftConfig

`RaftConfig` contains configuration options related to the Raft consensus 
algorithm and MicroRaft's implementation. Please check 
the [Configuration section](../../user-guide/configuration/) for details.


## RaftNode

A `RaftNode` runs the Raft consensus algorithm as a member of a Raft group. 
A Raft group is a cluster of `RaftNode` instances that runs the Raft consensus
algorithm. `RaftNode` interface contains APIs for replicating operations, 
performing queries, applying membership changes in the Raft group, handling 
Raft RPCs and responses, etc.

A single JVM instance can run multiple `RaftNode`s that belong to different 
Raft groups or even the same Raft group. 

A `RaftNode` uses the `RaftNodeRuntime` abstraction to run the Raft consensus
algorithm on the underlying platform and the `StateMachine` abstraction to run
committed operations on a user-specified state machine. 


## RaftEndpoint

`RaftEndpoint` represents an endpoint that participates to at least one Raft
group and executes the Raft consensus algorithm with a `RaftNode`.
 
`RaftNode` differentiates members of a Raft group with a unique id. MicroRaft
users need to provide a unique id for each `RaftEndpoint`. Other than this 
information, `RaftEndpoint` implementations can contain custom fields, such as
network addresses, tags, etc, and those information pieces can be utilized by
`RaftNodeRuntime` implementations.


## RaftRole and RaftNodeStatus 

`RaftRole` denotes the roles of `RaftNode`s as specified in the Raft consensus 
algorithm.

`RaftNodeStatus` denotes the statuses of a `RaftNode` during its own and its
Raft group's lifecycle.


## RaftNodeRuntime

`RaftNodeRuntime` enables execution of the Raft consensus algorithm by 
providing capabilities for task scheduling & execution, serialization &
deserialization of Raft messages, and networking.

A `RaftNode` runs in a single-threaded manner. Even if a `RaftNodeRuntime` 
implementation makes use of multiple threads internally, it must ensure 
the serial execution and 
[happens-before relationship](https://docs.oracle.com/javase/specs/jls/se8/html/jls-17.html)
for the tasks submitted by a single `RaftNode`.


## StateMachine

`StateMachine` enables users to implement arbitrary services, such as an atomic 
register or a key-value store, and execute operations on them. 

`RaftNode` does not deal with the actual logic of committed operations. Once
a given operation is committed by Raft, i.e., it is replicated to the majority
of the Raft group, the operation is passed to the provided `StateMachine` 
implementation. It is the `StateMachine` implementation's responsibility to 
ensure deterministic execution of committed operations.
 
The operations committed in a `RaftNode` are executed in the same thread that 
runs the Raft consensus algorithm. Since `RaftNodeRuntime` ensures 
the thread-safe execution of `RaftNode` tasks, `StateMachine` implementations 
do not need to be thread-safe.


## RaftStore and RestoredRaftState

`RaftStore` is used for persisting the internal state of the Raft consensus 
algorithm. Implementations must provide the durability guarantees defined
in the methods of the interface. 

If a `RaftNode` crashes, its persisted state could be read back stable storage
into `RestoredRaftState` and the `RaftNode` could be restored back. 
`RestoredRaftState` contains all the necessary information to recover 
`RaftNode` instances from crashes.

![](/img/info.png){: style="height:25px;width:25px"} `RaftStore` does not 
persist internal state of `StateMachine` implementations. Upon recovery, 
a `RaftNode` discovers the current commit index and re-executes all of 
the operations in the Raft log up to the commit index.


## RaftModel, RaftMessage and RaftModelFactory

`RaftModel` is the base interface for the objects that hit network and 
persistent storage. There are 2 other interfaces extending this interface:
`BaseLogEntry` and `RaftMessage`. `BaseLogEntry` is used for representing
log and snapshot entries stored in the Raft log. `RaftMessage` is used for 
Raft RPCs and their responses. Please see the interfaces inside 
`io.microraft.model` for more details. In addition, there is a 
`RaftModelFactory` interface for creating `RaftModel` objects. Users of 
MicroRaft must provide an implementation of this interface while 
creating `RaftNode` instances. 

In short, users of MicroRaft can create their own representation of 
the `RaftModel` objects and implement the networking and persistence concerns
behind the `RaftNodeRuntime` and `RaftStore` interfaces.

 
## Architectural overview of a Raft group

The following figure depicts an architectural overview of a Raft group based on
the abstractions explained above. Clients talks to the consensus module of 
MicroRaft. The consensus module talks to `RaftNodeRuntime` to communicate with 
the other Raft nodes and execute the Raft consensus algorithm, and `RaftStore`
to persisted Raft log entries to stable storage. Once it commits a log entry,
it passes the operation in the log entry to `StateMachine` for execution.

![Integration](/img/architectural_overview.png)


## What is Next?

Wow! That was fast, wasn't it? I guess you are ready to 
[get your hands dirty](how-to-use-microraft.md)!
