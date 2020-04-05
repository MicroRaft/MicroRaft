
The availability of a Raft group depends on if the majority (i.e., more than 
half) of the Raft nodes are alive or not. For instance, a 3-member Raft group
can tolerate failure of 1 Raft node and still remain available. Similarly, 
a 5-member Raft group can tolerate failure of 2 Raft nodes and remain 
available.

![](/img/info.png){: style="height:25px;width:25px"} In terms of safety, 
the fundamental guarantee of the Raft consensus algorithm and hence MicroRaft 
is, operations are committed in a single global order, and a committed 
operation is never lost, as long as there is no Byzantine failure in 
the system. In MicroRaft, restarting a Raft node that has no persistence layer
with the same identity, or restarting it with a corrupted persistence state
are examples of the Byzantine failure.


## Handling High Load 

Even if the majority of a Raft group is alive, we may encounter unavailability 
issues if our Raft group is under high load and cannot keep up with the request
rate. In this case, the leader temporarily stops accepting new requests and 
notifies the futures returned from the `RaftNode` methods with 
`CannotReplicateException`. Clients should apply some backoff and retry their
requests afterwards. Please refer to `CannotReplicateException` 
[JAVADOC](https://github.com/metanet/MicroRaft/blob/master/microraft/src/main/java/io/microraft/exception/CannotReplicateException.java)
for details.  

We will demonstrate this scenario in a code sample. We are forming a 3-node
Raft group below. In MicroRaft, a leader does not replicate requests one by 
one. Instead, it keeps a buffer for incoming requests, and replicates them to
the followers in batches, in order to improve the throughput. Once this buffer
is filled up, it stops accepting new requests. In this code sample, we allow 
__the uncommitted log entry buffer__ to keep at most 10 requests. We also slow
down our followers by making them sleep for 3 seconds. Then, we start sending 
requests to the leader. After some time, our requests fail with 
`CannotReplicateException`.

<script src="https://gist.github.com/metanet/3350f8107c01171f46bf08644cec582c.js"></script>

To run this code sample on your machine, try the following:

~~~~{.bash}
$ git clone git@github.com:metanet/MicroRaft.git
$ cd MicroRaft && ./mvnw clean test -Dtest=io.microraft.examples.HighLoadTest -DfailIfNoTests=false -Pcode-sample
~~~~

## Minority Failure

Failure of the minority (i.e, less than half) of the Raft nodes cause the Raft
group to lose its availability completely. The Raft group continues to accept
and commit new requests. If we have a persistence-layer implementation 
(i.e, `RaftStore`), we can recover failed Raft nodes. On the other hand, if we
don't have a persistence-layer or cannot recover the persisted Raft data, we
can remove the failed Raft nodes from the Raft group. Please note that when you
remove a Raft node from a Raft group, the majority value will be re-calculated
based on the new size of the Raft group. In order to replace a non-recoverable
Raft node without hurting the overall availability of the Raft group, we should
remove the crashed Raft node first and then add a fresh-new one.

![](/img/warning.png){: style="height:25px;width:25px"} If Raft nodes are
created without an actual `RaftStore` implementation in the beginning, 
restarting crashed Raft nodes with the same `RaftEndpoint` identity breaks
the safety of the Raft consensus algorithm. Therefore, when there is no 
persistence layer, the only recovery option for a failed Raft node is to 
remove it from the Raft group, which is possible only if the majority of 
the Raft group is up and running. 

To recover a crashed or terminated Raft node, we can restore its persisted 
state from the storage layer into a `RestoredRaftState` object. Then, we can 
use this object to create the Raft node back. __Please note that terminating 
a Raft node manually without a persistence layer implementation has the same 
outcome with the Raft node's crash since there is no way to restore it back.__

MicroRaft provides a basic in-memory `RaftStateStore` to enable crash-recover
testing. In the following code sample, we use this utility, i.e., 
`InMemoryRaftStore`, to demonstrate how to recover from Raft node failures. 

<script src="https://gist.github.com/metanet/14e9ef6d9a5f3992a03de5cd8a874589.js"></script>

To run this code sample on your machine, try the following:

~~~~{.bash}
$ git clone git@github.com:metanet/MicroRaft.git
$ cd MicroRaft && ./mvnw clean test -Dtest=io.microraft.examples.RestoreCrashedRaftNodeTest -DfailIfNoTests=false -Pcode-sample
~~~~

This time we provide a factory object to enable `LocalRaftGroup` to create 
`InMemoryRaftStore` objects while creating our Raft node instances. Our Raft
nodes are starting with `InMemoryRaftStore`, so once we terminate them, we will
get their persisted state. Once we start our Raft group, we commit a value via
the leader, observe that value with a local query on a follower, and crash that
follower. Then, we read the persisted state of the crashed follower via our
`InMemoryRaftStore` object and restore it back with 
`group.restoreRaftNode(restoredState, raftStore)`. Please ignore the details of
`RaftTestUtils.getRestoredState()` and `RaftTestUtils.getRaftStore()`. Once our
follower starts running again, it talks to the other Raft nodes, discovers 
the leader and the latest commit index, and replays all committed operations.

Our `sysout` lines in this code sample print the following:

~~~~{.text}
replicate result: value, commit index: 1
monotonic local query successful on follower. query result: value, commit index: 1
monotonic local query successful on restarted follower. query result: value, commit index: 1
~~~~

![](/img/warning.png){: style="height:25px;width:25px"} When a Raft node is
created with a restored Raft state, it discovers the current commit index of
the Raft group and replays the Raft log, i.e., automatically applies all of 
the log entries up to the commit index. We should be careful about 
the operations that have side-effects because the Raft log replay triggers 
those side-effects again. Please refer to the [State Machine Javadoc](https://github.com/metanet/MicroRaft/blob/master/microraft/src/main/java/io/microraft/integration/StateMachine.java#L64)
for more details.


## Majority Failure

Failure of the majority causes the Raft group to lose its availability and stop
handling new requests. The only recovery option is to recover some of 
the failed Raft nodes so that the majority becomes available again. Otherwise, 
the Raft group cannot be recovered. MicroRaft does not support unsafe recovery 
for now. 

![](/img/warning.png){: style="height:25px;width:25px"} Please note that you 
need to have a persistence-layer (i.e., `RaftStore` implementation) to make 
this recovery option work. 


## Raft Leader Failure

When the current leader Raft node of the Raft group fails, the Raft group 
temporarily loses its availability until the other Raft nodes notice 
the failure and elect a new leader. Delay of the failure detection of 
the leader depends on the __leader heartbeat timeout__ configuration. Please 
refer to the [Configuration section](/user-guide/configuration/) to learn more
about the leader election and leader heartbeat timeout configuration 
parameters.   

If a Raft leader fails before a client receives response for an operation 
passed to the `RaftNode.replicate()` method, there are multiple possibilities:
 
- If the leader failed before replicating the given operation to any follower, 
then the operation is certainly not committed and hence it is lost. 

- If the failed leader managed to replicate the given operation to at least one
follower, then that operation might be committed if that follower wins the new 
election. However, another follower could become the new leader and overwrite 
that operation if it was not replicated to the majority by the crashed leader.

- The good thing about queries is, they are idempotent. Clients can safely 
retry their queries on the new leader. 

![](/img/warning.png){: style="height:25px;width:25px"} It is up to the client 
to retry a operation whose result is not received, because a retry could cause 
the operation to be committed twice based on the actual failure scenario. 
MicroRaft goes for simplicity and does not employ deduplication (I have plans
to implement an opt-in deduplication mechanism in future). If deduplication is
needed, it can be done in `StateMachine` implementations for now.

We will see the second scenario above in a code sample. In the following code,
we replicate an operation with the Raft leader but block the responses sent 
back from the followers. Hence, even though the leader managed to replicate our
operation to the majority, it is not able to commit our operation. At this 
step, we crash the leader. We won't get any response for our operation now 
since the leader is gone, so we will just re-replicate it with the new leader.
The thing is, the previous leader managed to replicate our request to 
the majority, so the new leader will commit our first operation. Since we also
replicate it for the second time, we caused a duplicate commit. When we query
the new leader, we see that there are 2 values added to our state machine.
 

<script src="https://gist.github.com/metanet/125d33a0e009e9119f5c9a96061ec69e.js"></script>

To run this code sample on your machine, try the following:

~~~~{.bash}
$ git clone git@github.com:metanet/MicroRaft.git
$ cd MicroRaft && ./mvnw clean test -Dtest=io.microraft.examples.RaftLeaderFailureTest -DfailIfNoTests=false -Pcode-sample
~~~~

![](/img/info.png){: style="height:25px;width:25px"} Another trick could be 
designing our operations in an idempotent way so that duplicate commits do 
not make any harm, however it is not easy to make every type of operation
idempotent.


## Network Partition

Behaviour of a Raft group during a network partition depends on how Raft nodes
are divided to different sides of the network partition and with which Raft 
nodes our clients are interacting with. If any subset of the Raft nodes manage 
to form the majority, they remain available. If the Raft leader falls into 
the minority side, the Raft nodes in the majority side elect a new leader and 
restore their availability.

If our clients cannot talk to the majority side, it means that the Raft group
is unavailable from the perspective of the clients.
    
If the leader falls into a minority side of the network partition, it demotes
itself to the follower role after __the leader heartbeat timeout__, and fails 
all pending operations with `IndeterminateStateException`. This exception means
that the demoted leader cannot decide if those operations have been committed 
or not.

![](/img/warning.png){: style="height:25px;width:25px"} It is up to the client 
to retry a operation which is notified with `IndeterminateStateException`, 
because a retry could cause the operation to be committed twice. MicroRaft goes
for simplicity and does not employ deduplication (I have plans to implement 
an opt-in deduplication mechanism in future). If deduplication is needed, it
can be done in `StateMachine` implementations for now. 
 
When the network problem is resolved, the Raft nodes connect to each other 
again. The Raft nodes that was on a minority side of the network partition 
catch up with the other Raft nodes and the Raft group continues its normal 
operation.

![](/img/info.png){: style="height:25px;width:25px"} One of the key points of 
the Raft consensus algorithm's and hence MicroRaft's network partition 
behaviour is the absence of split-brain. In any network partition scenario, 
there can be at most one Raft leader handling and committing operations.

We will see how our Raft nodes behave in a network partitioning scenario in 
the following code sample. Again, we have a 3-node Raft group here and we 
create an artificial network disconnection between the leader and 
the followers. Since the leader cannot talk to the majority side anymore,
after the __leader heartbeat timeout__ duration elapses, our leader demotes
to the follower role, and the followers on the other side elect a new leader
among themselves and even commit a new operation. Once we resolve the network
problem, we see that our old leader connects back to the other nodes, discover
the new leader and get the committed operation. Phew! 

<script src="https://gist.github.com/metanet/ac66fbb2f6e2ef5e8224ac387d2e2b44.js"></script>

To run this code sample on your machine, try the following:

~~~~{.bash}
$ git clone git@github.com:metanet/MicroRaft.git
$ cd MicroRaft && ./mvnw clean test -Dtest=io.microraft.examples.NetworkPartitionTest -DfailIfNoTests=false -Pcode-sample
~~~~

## Corruption or Loss of Persisted Raft State

If a `RestoredRaftState` object is created with corrupted or partially-lost
Raft state, the safety guarantees of the Raft consensus algorithm no longer 
hold. For instance, if a flushed log entry is not present in 
the `RestoredRaftState` object, then the restored `RaftNode` may not have a
a committed operation.

![](/img/warning.png){: style="height:25px;width:25px"} It is 
the responsibility of `RaftStore` implementations to ensure durability and 
integrity of the persisted Raft state. `RaftNode` does not perform any error 
checks when they are restored with `RestoredRaftState` objects.

