
![Java CI](https://github.com/metanet/MicroRaft/workflows/Java%20CI/badge.svg) [![Integration](license-apache-2.svg)](https://github.com/metanet/MicroRaft/blob/master/LICENSE)


![](microraft-docs/docs/img/logo.png)

MicroRaft is a future-complete, stable and production-grade open-source 
implementation of the Raft consensus algorithm in Java. It requires Java 8 
at minimum. It can be used for building fault tolerant and strongly-consistent (CP) data, metadata 
and coordination services. A few examples of possible use-cases are building
distributed file systems, distributed lock services, key-value stores, etc.

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

See [Getting Started Guide](https://microraft.io/user-guide/getting-started)


## Documentation

See the documentation at [microraft.io](https://microraft.io)


## Building from Source

Pull the latest code with `git pull origin master` and build with 
`maven clean install`. 


## Source Code Layout 

`microraft` module contains the source code of MicroRaft along with its unit 
and integration test suite. 

`microraft-hocon` and `microraft-yaml` modules are utility libraries for 
parsing HOCON and YAML files to start Raft nodes.  

## Contributing to MicroRaft

Any kind of contribution is welcome. Just pull the source code and start 
rocking! No direct commits are allowed. Please create a pull request for your
code changes, and a Github issue for feature requests.

## License

MicroRaft is available under [the Apache 2 License](https://github.com/metanet/MicroRaft/blob/master/LICENSE). 

