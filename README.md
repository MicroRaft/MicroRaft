
![Java CI](https://github.com/metanet/MicroRaft/workflows/Java%20CI/badge.svg) [![Integration](license-apache-2.svg)](https://github.com/metanet/MicroRaft/blob/master/LICENSE)

# MicroRaft

## Overview

MicroRaft is a future-complete, stable and production-grade implementation of 
the Raft consensus algorithm in Java. It requires Java 8 at minimum. It can be 
used for building fault tolerant and strongly-consistent (CP) data, metadata 
and coordination services. A few examples of possible use-cases are building
distributed file systems, distributed lock services, key-value stores, etc.


## Who uses MicroRaft?

I am currently working on a proof-of-concept KV store implementation to 
demonstrate how to implement MicroRaft's abstractions. It internally uses gRPC 
to transfer Raft messages between Raft nodes running on different machines. I 
am hoping to release this project soon. 


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
