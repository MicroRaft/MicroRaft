## Monitoring

`RaftNodeReport` class contains detailed information about internal state of
a `RaftNode`, such as its Raft role, term, leader, last log index, commit 
index, etc. MicroRaft provides multiple mechanisms to monitor internal state of 
`RaftNode` instances via `RaftNodeReport` objects:

1. You can build a simple pull-based system to query `RaftNodeReport` objects via the `RaftNode.getReport()` API and publish those objects to any external monitoring system. 

2. The `RaftNodeRuntime` abstraction contains a hook method, 
`RaftNodeRuntime#handleRaftNodeReport()`, which is called by MicroRaft anytime 
there is an important change in internal state of a `RaftNode`, such as 
a leader change, a term change, or a snapshot installation. You can also use 
that method to capture `RaftNodeReport` objects and notify external monitoring
systems promptly. 

