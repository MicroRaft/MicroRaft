
MicroRaft is a future-complete, stable and production-grade implementation of 
the Raft consensus algorithm in Java. It requires Java 8 at minimum. It can be 
used for building fault tolerant and strongly-consistent (CP) data, metadata 
and coordination services. A few examples of possible use-cases are building
distributed file systems, distributed lock services, key-value stores, etc.

Consensus is one of the fundamental problems in distributed systems, involving
multiple servers agree on values. Once a value is decided, the decision is 
final. Majority-based consensus algorithms, such as Raft, make progress when 
the majority (i.e., more than half) of the servers are up an running and never 
return incorrect values.

Raft uses a replicated log to order requests sent by clients and apply them on
a set of state machine replicas in a coordinated, deterministic and fault 
tolerant manner (i.e., replicated state machines). For more details, please see 
[In Search of an Understandable Consensus Algorithm](https://raft.github.io/raft.pdf) 
by Diego Ongaro and John Ousterhout. 

MicroRaft works on top of a minimalistic and modular design. It consists of 
an isolated implementation of the Raft consensus algorithm and a set of 
accompanying abstractions to run it in a multi-threaded and distributed 
environment. These abstractions are defined to isolate the core Raft logic from
the concerns of persistence, thread-safety, serialization, networking, and
execution of committed operations. Users must provide their own implementations
of these abstractions.

MicroRaft implements the leader election, log replication, log compaction, and 
cluster membership changes components of the Raft consensus algorithm. 
Additionally, it offers a rich set of optimizations and enhancements:

* Pipelining and batching during log replication,
* Back pressure to prevent OOMEs on Raft leader and followers,
* Parallel snapshot chunk transfer from Raft leader and followers,
* Pre-voting and leader stickiness [(4 Modifications for Raft Consensus)](https://openlife.cc/system/files/4-modifications-for-Raft-consensus.pdf),
* Auto-demotion of Raft leader on loss of quorum heartbeats,
* Linearizable quorum reads without appending log entries [(Section 6.4 of the Raft dissertation)](https://github.com/ongardie/dissertation),
* Lease-based local queries on Raft leader [(Section 6.4.1 of the Raft dissertation)](https://github.com/ongardie/dissertation),
* Monotonic local queries on Raft followers [(Section 6.4.1 of the Raft dissertation)](https://github.com/ongardie/dissertation),
* Parallel disk writes on Raft leader and followers [(Section 10.2.1 of the Raft dissertation)](https://github.com/ongardie/dissertation),
* Leadership transfer [(Section 3.10 of the Raft dissertation)](https://github.com/ongardie/dissertation).
