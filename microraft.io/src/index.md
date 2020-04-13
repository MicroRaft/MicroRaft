
![](img/logo.png){: style="height:64px;width:348px"}

MicroRaft is a feature-complete, stable and production-grade open-source 
implementation of the Raft consensus algorithm in Java. It can be used for
building fault tolerant and strongly-consistent (CP) data, metadata and
coordination services. A few examples of possible use-cases are building
distributed file systems, key-value stores, distributed lock services, etc.

MicroRaft works on top of a minimalistic and modular design. __It is a single 
lightweight JAR with a few hundred KBs of size and only logging dependency.__
It contains an isolated implementation of the Raft consensus algorithm, and 
a set of accompanying abstractions to run the algorithm in a multi-threaded and 
distributed environment. These abstractions are defined to isolate the core 
algorithm from the concerns of persistence, thread-safety, serialization, 
networking, and actual state machine logic. Users are required to provide their 
own implementations of these abstractions to build their custom CP distributed
systems with MicroRaft.

__Please note that MicroRaft is not a high-level solution like a distributed 
key-value store or a distributed lock service. It is a core library that offers
a set of abstractions and functionalities to help you build such high-level 
systems.__ 

## Features

MicroRaft implements the leader election, log replication, log compaction 
(snapshotting), and cluster membership changes components of the Raft consensus
algorithm. Additionally, it offers a rich set of optimizations and 
enhancements:

* Pipelining and batching during log replication,
* Back pressure to prevent OOMEs on Raft leader and followers,
* Parallel snapshot transfer from Raft leader and followers,
* Pre-voting and leader stickiness [(4 Modifications for Raft Consensus)](https://openlife.cc/system/files/4-modifications-for-Raft-consensus.pdf),
* Auto-demotion of Raft leader on loss of quorum heartbeats,
* Linearizable quorum reads without appending log entries [(Section 6.4 of the Raft dissertation)](https://github.com/ongardie/dissertation),
* Lease-based local queries on Raft leader [(Section 6.4.1 of the Raft dissertation)](https://github.com/ongardie/dissertation),
* Monotonic local queries on Raft followers [(Section 6.4.1 of the Raft dissertation)](https://github.com/ongardie/dissertation),
* Parallel disk writes on Raft leader and followers [(Section 10.2.1 of the Raft dissertation)](https://github.com/ongardie/dissertation),
* Leadership transfer [(Section 3.10 of the Raft dissertation)](https://github.com/ongardie/dissertation).


## Getting Started

The following commands start a 3-node local Raft cluster on your machine and
commits a number of operations. Just try them on your terminal for a sneak peek
at MicroRaft.

~~~~{.bash}
$ git clone https://github.com/metanet/MicroRaft.git
$ cd MicroRaft && ./mvnw clean test -Dtest=io.microraft.tutorial.OperationCommitTest -DfailIfNoTests=false -Ptutorial
~~~~

If you want to learn more about how to use MicroRaft for building a CP 
distributed system, you can check out the 
[APIs and Main Abstractions](user-guide/apis-and-main-abstractions.md) section
first, then read the 
[tutorial](user-guide/tutorial-building-an-atomic-register.md) to build 
an atomic register on top of MicroRaft.


## Getting Involved

MicroRaft is a new open-source library and there is tons of work to do! So 
any kind of feedback and contribution is welcome! You can improve the source
code, add new tests, create issues or feature requests, or just ask questions!

The development happens on [Github](https://github.com/metanet/microraft). 
There is also a [Slack group](https://join.slack.com/t/microraft/shared_invite/zt-dc6utpfk-84P0VbK7EcrD3lIme2IaaQ) 
for discussions and questions. Last, you can follow [@MicroRaft](https://twitter.com/microraft) 
on Twitter for announcements. 


## Who uses MicroRaft?

I am currently working on a POC project to demonstrate how to implement 
a distributed KV store on top of MicroRaft's abstractions. It internally uses
gRPC to transfer Raft messages between Raft nodes running on different 
machines. I am hoping to release this project soon. 


## What is Consensus?

Consensus is one of the fundamental problems in distributed systems. It 
involves multiple servers agree on values. Once a value is decided, 
the decision is final. Majority-based consensus algorithms, such as Raft, make
progress when the majority (i.e., more than half) of the servers are up and 
running, and never return incorrect responses.

Raft uses a replicated log to order requests sent by clients and apply them on
a set of state machine replicas in a coordinated, deterministic and fault 
tolerant manner (i.e., replicated state machines). For more details, please see 
[In Search of an Understandable Consensus Algorithm](https://raft.github.io/raft.pdf) 
by Diego Ongaro and John Ousterhout. 


## Acknowledgements

MicroRaft originates from 
[the Raft implementation](https://github.com/hazelcast/hazelcast/tree/master/hazelcast/src/main/java/com/hazelcast/cp/internal/raft) 
that empowers Hazelcast IMDG's 
[CP Subsystem module](https://docs.hazelcast.org/docs/latest/manual/html-single/index.html#cp-subsystem),
and includes several significant improvements on the public APIs and internals. 
