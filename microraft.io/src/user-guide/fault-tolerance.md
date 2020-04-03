
The availability of a Raft group depends on if the majority (i.e., more than 
half) of the Raft nodes are alive or not. For instance, a 3-member Raft group
can tolerate failure of 1 Raft node and still remain available. Similarly, 
a 5-member Raft group can tolerate failure of 2 Raft nodes and remain 
available.

![](/img/info.png){: style="height:25px;width:25px"} In terms of safety, 
the fundamental guarantee of the Raft consensus algorithm and hence MicroRaft 
is, operations are committed in a single global order, and a committed 
operation is never lost, as long as there is no Byzantine failure in 
the system.


## Minority Failure

Based on the reasoning above, failure of the minority (i.e, less than half) of 
the Raft nodes does not hurt availability of the Raft group. The Raft group
continues to accept and commit new requests. If you have a persistence-layer
implementation (i.e, `RaftStore`), you can try to recover failed Raft nodes. 
If you don't have a persistence-layer or cannot recover the persisted Raft 
data, you can remove the failed Raft nodes from the Raft group. Please note 
that when you remove a Raft node from a Raft group, the majority value will be 
re-calculated based on the new size of the Raft group.


## Majority Failure

Dually, failure of the majority causes the Raft group to lose its 
availability and stop handling new requests. The only recovery option is to 
recover some of the failed Raft nodes so that the majority becomes available 
again. Otherwise, the Raft group cannot be recovered. MicroRaft does not 
support unsafe recovery for now. 

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
that operation. 

![](/img/warning.png){: style="height:25px;width:25px"} It is up to the client 
to retry a operation whose result is not received, because a retry could cause 
the operation to be committed twice based on the actual failure scenario. 
MicroRaft goes for simplicity and does not employ deduplication. If 
deduplication is needed, it can be implemented inside the operation objects 
passed to `RaftNode.replicate()`.


## Network Partition

Behaviour of a Raft group during a network partition depends on how Raft nodes
are divided in different sides of the network partition and with which 
Raft nodes clients are interacting with. If any subset of the Raft nodes manage 
to form the majority, they remain available. If the Raft leader falls into 
the minority side, the Raft nodes in the majority side elect a new leader and 
restore their availability.

If your clients cannot talk to the majority side, it means that the Raft group
is unavailable from the perspective of the clients.
    
If the leader falls into a minority side of the network partition, it demotes
itself to the follower role after __the leader heartbeat timeout__, and fails 
all pending operations with `IndeterminateStateException`. This exception means
that the demoted leader cannot decide if those operations have been committed 
or not.

![](/img/warning.png){: style="height:25px;width:25px"} It is up to the client 
to retry a operation which is notified with `{IndeterminateStateException}`, 
because a retry could cause the operation to be committed twice. MicroRaft goes
for simplicity and does not employ deduplication. If deduplication is needed, 
it can be implemented inside the operation objects passed to 
`RaftNode.replicate()`.
 
When the network problem is resolved, the Raft nodes connect to each other 
again. The Raft nodes that was on a minority side of the network partition 
catch up with the other Raft nodes and the Raft group continues its normal 
operation.

![](/img/info.png){: style="height:25px;width:25px"} One of the key points of 
the Raft consensus algorithm's and hence MicroRaft's network partition 
behaviour is the absence of split-brain. In any network partition scenario, 
there can be at most one Raft leader handling and committing operations.


## Corruption or Loss of the Persisted Raft State

If a `RestoredRaftState` object is created with corrupted or partially-lost
Raft state, the safety guarantees of the Raft consensus algorithm no longer 
hold. For instance, if a flushed log entry is not present in 
the `RestoredRaftState` object, then the restored `RaftNode` may not have a
a committed operation.

![](/img/warning.png){: style="height:25px;width:25px"} It is 
the responsibility of `RaftStore` implementations to ensure durability and 
integrity of the persisted Raft state. `RaftNode` does not perform any error 
checks when they are restored with `RestoredRaftState` objects.

