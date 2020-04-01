
![Java CI](https://github.com/metanet/MicroRaft/workflows/Java%20CI/badge.svg) [![Integration](misc/license-Apache-2.svg)](https://github.com/metanet/MicroRaft/blob/master/LICENSE)

# MicroRaft

## Overview

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
Additionally, it offers the following optimizations and enhancements:

* Pipelining and batching during log replication,
* Back pressure to prevent OOMEs on Raft leader and followers,
* Parallel snapshot chunk transfer from Raft leader and followers,  
* Pre-voting and leader stickiness [(4 Modifications for Raft Consensus)](https://openlife.cc/system/files/4-modifications-for-Raft-consensus.pdf),
* Auto-demotion of Raft leader on loss of quorum heartbeats,
* Linearizable quorum reads without appending log entries [(Section 6.4 of the Raft dissertation)](https://github.com/ongardie/dissertation),
* Lease-based local queries on Raft leader [(Section 6.4.1 of the Raft dissertation)](https://github.com/ongardie/dissertation),
* Local queries on Raft followers,
* Parallel disk writes on Raft leader and followers [(Section 10.2.1 of the Raft dissertation)](https://github.com/ongardie/dissertation),
* Leadership transfer [(Section 3.10 of the Raft dissertation)](https://github.com/ongardie/dissertation).


## Who uses MicroRaft?

I am currently working on a proof-of-concept KV store implementation to 
demonstrate how to implement MicroRaft's abstractions. I am hoping to release
it soon. 


## Roadmap

I am planning to work on the following tasks in future:

- Learner nodes. When a new Raft node is added to a running Raft group, it can
start with the "learner" role. In this role, the new Raft node is excluded in 
the quorum calculations to not to hurt availability of the Raft group until
it catches up with the Raft group leader.

- Opt-in deduplication mechanism via implementation of the 
[Implementing Linearizability at Large Scale and Low Latency](https://dl.acm.org/doi/10.1145/2815400.2815416) 
paper. Currently, one can implement deduplication inside his custom 
`StateMachine` implementation. I would like to offer a generic and opt-in 
solution by MicroRaft.
 
- Witness replicas via implementation of the 
[Pirogue, a lighter dynamic version of the Raft distributed consensus algorithm](https://dl.acm.org/doi/10.1109/PCCC.2015.7410281) 
paper. Witness replicas participate in quorum calculations but do not keep any
state for `StateMachine` to reduce the storage overhead.


## Acknowledgements

MicroRaft originates from 
[the Raft implementation](https://github.com/hazelcast/hazelcast/tree/master/hazelcast/src/main/java/com/hazelcast/cp/internal/raft) 
that empowers Hazelcast IMDG's 
[CP Subsystem module](https://docs.hazelcast.org/docs/latest/manual/html-single/index.html#cp-subsystem),
and includes several significant improvements on the public API and internals.  
