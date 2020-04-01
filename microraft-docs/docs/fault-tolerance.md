
The availability of a Raft group depends on if the majority (i.e., more than 
half) of the `RaftNode`s are alive or not. For instance, a 3-member Raft group
can tolerate failure of 1 `RaftNode` and still remain available. Similarly, 
a 5-member Raft group can tolerate failure of 2 `RaftNode`s and remain 
available. 

> :information_source: In terms of safety, the fundamental guarantee of 
> the Raft consensus algorithm and hence MicroRaft is, operations are committed
> in a single global order, and a committed operation is never lost, as long as
> there is no Byzantine failure in the system.


## Minority Failure

Based on the reasoning above, failure of the minority (i.e, less than half) of 
the `RaftNode`s does not hurt availability of the Raft group. The Raft group
continues to accept and commit new requests. If you have a persistence-layer
implementation (i.e, `RaftStore`), you can try to recover failed `RaftNode`s. 
If you don't have a persistence-layer or cannot recover the persisted Raft 
data, you can remove failed `RaftNode`s from the Raft group. Please note that
when you remove a `RaftNode` from a Raft group, the majority value will be 
re-calculated based on the new size of the Raft group.


## Majority Failure

Based on the reasoning above, failure of the majority causes the Raft group to
lose its availability and stop handling new requests. The only recovery option 
is to recover some of the failed `RaftNode`s so that the majority becomes 
available again. Otherwise, the Raft group cannot be recovered. MicroRaft does 
not support an unsafe recovery policy for now. 

> :warning: Please note that you need to have a persistence-layer to make this
> recovery option work. 


## Raft Leader Failure

When the current leader of the Raft group fails, the Raft group temporarily
loses its availability until the other `RaftNode`s notice the failure and
elect a new leader. 

If a Raft leader fails before a client receives response for an operation 
passed to the `RaftNode.replicate()` method, there are multiple possibilities:
 
- If the leader failed before replicating the given operation to any follower, 
then the operation is certainly not committed and hence it is lost. 

- If the failed leader managed to replicate the given operation to at least one
follower, then that operation might be committed if that follower wins the new 
election. However, another follower could become the new leader and overwrite 
that operation. 

> :warning: It is up to the client to retry a operation whose result is not 
> received, because a retry could cause the operation to be committed twice 
> based on the actual failure scenario. MicroRaft goes for simplicity and does
> not employ deduplication. If deduplication is needed, it can be implemented
> inside the operation objects passed to `RaftNode.replicate()`.

Raft leaders use a heartbeat mechanism to denote their liveliness. Followers do
not trigger a new leader election round as long as the current leader continues
to send `AppendEntries` RPCs. The Raft paper uses a single parameter, 
__election timeout__, to both detect failure of the leader and perform a leader 
election round. A follower decides the current leader to be crashed if the
__election timeout__ duration passes after its last `AppendEntriesRPC`. 
Moreover, a candidate starts a new leader election round if the __election 
timeout__ elapses before it acquires the majority votes or another candidate 
announces the leadership. The Raft paper discusses the __election timeout__ to 
be configured around 500 milliseconds. Even though such a low timeout value
works just fine for leader elections, we have experienced that it causes false
positives for failure detection of the leader when there is a hiccup in 
the system. When a leader slows down temporarily for any reason, its followers 
may start a new leader election round very quickly. To prevent such problems
and allow users to tune availability of the system with more granularity,
MicroRaft uses 2 timeout configurations: __leader heartbeat timeout__, which is
specified by `RaftConfig.getLeaderHeartbeatTimeoutMillis()` and 10 seconds by
default, to detect failure of the leader and __leader election timeout__, which
is specified by `RaftConfig.getLeaderElectionTimeoutMillis()` and 500 
milliseconds by default, to terminate the current leader election round and 
start a new one. If both values are configured the same, MicroRaft's behaviour
becomes identical to the behaviour described in the Raft paper. Please note
that having 2 timeout parameters does not make any difference in terms of 
the safety property. 


## Network Partition

Behaviour of a Raft group during a network partition depends on how `RaftNode`s
are divided in different sides of the network partition and with which 
`RaftNode`s you are interacting with. If any subset of the `RaftNode`s manage 
to form the majority, they remain available. If the Raft leader before 
the network partition is not present in the majority side, the `RaftNode`s 
in the majority side elect a new leader and restore their availability.
    
If the leader falls into a minority side of the network partition, it demotes
itself to the follower role after __the leader heartbeat timeout__, and fails 
all pending operations with `IndeterminateStateException`. This exception means
that the demoted leader cannot decide if those operations have been committed 
or not. 
 
When the network problem is resolved, the `RaftNode`s connect to each other 
again.`RaftNode`s that was on a minority side of the network partition catch up
with the other `RaftNode`s and the Raft group continues its normal operation.

> :information_source: The most important point of the Raft consensus 
> algorithm's and hence MicroRaft's network partition behaviour is the absence
> of split-brain. In any network partition scenario, there can be at most one
> Raft leader handling and committing operations.


## Corruption or Loss of the Persisted Raft State

If a `RestoredRaftState` object is created with corrupted or partially-lost
Raft state, the safety guarantees of the Raft consensus algorithm no longer 
holds. For instance, if a flushed (`io.microraft.persistence.RaftStore#flush()`) 
log entry is not present in the `RestoredRaftState` object, then it can cause 
to lose a committed operation and break determinism of the `StateMachine`.

> :warning: It is the responsibility of `RaftStore` implementations to ensure 
> durability and integrity of the persisted Raft state. `RaftNode`s do not 
> perform any checks when they are restored with `RestoredRaftState` objects.

