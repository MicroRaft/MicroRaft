
In order to run a Raft group (i.e., Raft cluster) using MicroRaft, you need to:

* implement the `RaftEndpoint` interface to denote unique IDs of `RaftNode`s 
you are going to run,
 
* provide a `StateMachine` implementation for your actual state machine logic 
(key-value store, atomic register, etc.),

* implement the `RaftModel` and `RaftModelFactory` interfaces to create Raft 
RPC request and response objects and Raft log entries,
    
* provide a `RafNodeRuntime` implementation to realize task execution, 
serialization of your `RaftModel` objects and networking, 

* __(optional)__ provide a `RaftStore` implementation if you want to restore
failed Raft nodes. You can persist the internal Raft node state to stable
storage via the `RaftStore` interface and recover from Raft node failures by
restoring persisted Raft state. Otherwise, you could use the already-existing
`io.microraft.persistence.NopRaftStore` utility which makes the internal Raft
state volatile and disables crash-recovery, i.e, recovery of failed 
Raft nodes. If persistence is not implemented, failed Raft nodes cannot be 
restarted and they need to be removed from the Raft group to not to damage 
availability.

* ![](/img/info.png){: style="height:25px;width:25px"} build a discovery and 
RPC mechanism to replicate and commit your operations on a Raft group. This is 
required because simplicity is the primary concern for MicroRaft's design
philosophy. MicroRaft offers a minimum API set to cover the fundamental
functionality and enable its users to implement higher-level abstractions, such
as an RPC system with request routing, retries, deduplication, etc. For 
instance, MicroRaft does not broadcast the Raft group members to any external 
discovery system, or it does not integrate with any observability tool, but it
exposes all necessary information, such as the current Raft group members, 
the leader endpoint, term, commit index, etc. via `RaftNode.getReport()`. You 
can use that information to feed your discovery system and monitoring tools. 
Relatedly, the public APIs on `RaftNode` do not employ request routing or retry mechanisms. For instance, if a client tries to replicate an operation via a
follower or a candidate Raft node, the returned `CompletableFuture` object is simply notified with `NotLeaderException`, instead of internally forwarding 
the operation to the leader Raft node. `NotLeaderException` also contains
`RaftEndpoint` of the current leader so that the client can send its request to 
the leader Raft node. 


## How to Bootstrap a Raft Group

Before starting a Raft group, you first decide on its initial member list, 
i.e., the list of Raft endpoints. The identical initial Raft group member list 
must be provided to all Raft nodes.

The following code shows how to create a 3-member Raft group in a single JVM.

~~~~{.java}
RaftEndpoint member1 = ... 
RaftEndpoint member2 = ... 
RaftEndpoint member3 = ...

List<RaftEndpoint> initialGroupMembers 
	= Arrays.asList(member1, member2, member3);

List<RaftNode> raftNodes = new ArrayList<>();
for (RaftEndpoint endpoint : initialGroupMembers) {
    RaftNodeRuntime runtime = ... // create Raft node runtime
    StateMachine stateMachine = ... // create state machine 
    RaftConfig config = ... // create Raft config

    RaftNode raftNode = RaftNode.newBuilder()
                                .setInitialGroupMembers(initialGroupMembers)
                                .setLocalEndpoint(member1)
                                .setRuntime(runtime)
                                .setStateMachine(stateMachine)
				.setConfig(config)
                                .build();
    
    raftNode.start(); 
}
~~~~

When a Raft node is created, its initial status is `RaftNodeStatus.INITIAL` 
and it does not execute the Raft consensus algorithm in this status. When 
`RaftNode.start()` is called, the Raft node moves to `RaftNodeStatus.ACTIVE` 
and it automatically checks if there is a leader already or it should trigger
a new leader election round. Once the leader election is done, operations can
be replicated and queries can be executed on the Raft group. You can learn 
the leader endpoint via `RaftNode.getLeaderEndpoint()`. 


## How to Commit an Operation on the Raft Group

Let's see how to replicate and commit an operation via the Raft group leader. 
Our operation returns a `String` result. 

~~~~{.java}
RaftNode leader = ... // discover the leader Raft node 
Object operation = ... // create your operation

Future<Ordered<String>> future = leader.replicate(operation);
Ordered<String> result = future.get();

Sytem.out.println("Commit index: " + result.getCommitIndex() 
	+ ", result: " + result.getResult());
~~~~
 
Most of the `RaftNode` methods, including `RaftNode.replicate()` return 
a `CompletableFuture<Ordered>` object. `Ordered` provides both the result of
the operation execution and on which Raft log index the operation has
been committed and executed.

When `RaftNode.replicate()` is called on a follower or a candidate, 
the returned `CompletableFuture<Ordered>` object is simply notified with 
`NotLeaderException`, which also provides `RaftEndpoint` of the leader 
Raft node. You can build a retry mechanism in your RPC layer to forward
the operation to the `RaftEndpoint` given in the exception. It is also possible
that there is an ongoing leader election round. This time, the returned 
`NotLeaderException` does not specify who is the leader. In this case, your RPC
layer could retry your operation on each Raft node in a round robin fashion
until it discovers the new leader. 

When a leader Raft node is under high load, it may not keep up with 
the incoming requests and notify the returned `CompletableFuture<Ordered>`
objects with `CannotReplicateException`. In this case, clients should apply
some backoff and retry the operation on the same Raft node instance 
afterwards.


## How to Perform a Query

`RaftNode.query()` method executes query (i.e., read-only) operations on 
the Raft group in an efficient manner. MicroRaft differentiates updates and
queries to employ some optimizations for query operations. Namely, it offers
3 policies for queries:

* `QueryPolicy.LINEARIZABLE`: You can perform a linearizable query on 
the leader Raft node. MicroRaft employs the optimization described in 
*Section: 6.4 Processing read-only queries more efficiently* of 
[the Raft dissertation](https://github.com/ongardie/dissertation) to preserve
linearizability without growing the internal Raft log.

* `QueryPolicy.LEADER_LOCAL`: You can run a query locally on the leader 
Raft node without making the leader talk to the followers. If the called 
Raft node is not the leader, the returned `CompletableFuture<Ordered>` object 
fails with `NotLeaderException`. 

* `QueryPolicy.ANY_LOCAL`: You can use this policy to run a query locally on
any Raft node independent of its current Raft role. 


MicroRaft also employs leader stickiness and auto-demotion of leaders on loss 
of quorum heartbeats. Leader stickiness means that a follower does not vote for
another Raft node before the __leader heartbeat timeout__ elapses after 
the last received __AppendEntries__ or __InstallSnapshot__ RPC. Dually, 
a leader Raft node automatically demotes itself to the follower role if it has
not received `AppendEntriesRPC` responses from the majority during the __leader
heartbeat timeout__ duration. Along with these techniques, 
`QueryPolicy.LEADER_LOCAL` can be used for performing linearizable queries 
without talking to the majority if the clock drifts and network delays are 
bounded. However, bounding clock drifts and network delays is not an easy task.
`LEADER_LOCAL` may cause reading stale data if a Raft node still considers 
itself as the leader because of a clock drift even though the other Raft nodes 
have elected a new leader and committed new operations. Moreover, 
`LEADER_LOCAL` and `LINEARIZABLE` policies have the same processing cost since
only the leader Raft node runs a given query for both policies. `LINEARIZABLE`
policy guarantees linearizability with an extra cost of 1 RTT latency overhead
compared to the `LEADER_LOCAL` policy. For these reasons, `LINEARIZABLE` is 
the recommended policy for linearizable queries and `LEADER_LOCAL` should be 
used with care. 

Nevertheless, `LEADER_LOCAL` and `ANY_LOCAL` policies can be easily used if 
monotonicity is sufficient for the query results returned to the clients. 
A client can track the commit indices observed via the returned `Ordered` 
objects and use the highest observed commit index to preserve monotonicity
while performing a local query on a Raft node. If the local commit index of
a Raft node is smaller than the commit index passed to the `RaftNode.query()` 
call, the returned `CompletableFuture` object fails with 
`LaggingCommitIndexException`. The client can send its query to another 
Raft node upon receiving this exception.

Please refer to 
[Section 6.4.1 of theRaft dissertation](https://github.com/ongardie/dissertation)
for more details.

~~~~{.java}
// track and persist the highest commit index observed by the client
long highestObservedCommitIndex = ... 

CompletableFuture<Ordered<String>> future = follower.query(operation, 
	QueryPolicy.ANY_LOCAL, highestObservedCommitIndex);
Ordered<String> result = future.get(); 
~~~~

## How to Restore a Crashed Raft Node 

MicroRaft contains the `RaftStore` interface which is a contract for persisting 
internal Raft state to storage. However, MicroRaft does not provide any actual
persistence implementation. You need to write your own `RaftStore` 
implementation to be able to recover from JVM or machine crashes. Your custom 
persistence implementation must satisfy all the durability guarantees defined 
in the `RaftStore` interface in order to preserve the safety properties of 
the Raft consensus algorithm.

If you need to terminate a Raft node, for instance, because you will move that
Raft node to another machine, you can move the Raft node to 
`RaftNodeStatus.TERMINATED` status via `RaftNode.terminate()`. Please note that 
its termination just makes it unavailable to the other Raft nodes since it 
will only stop executing the Raft consensus algorithm, however it will be still
in the Raft group member list. Once the Raft node is terminated, you can move
its persisted data to the new server and restart it.

To recover a crashed or terminated Raft node, you can restore its persisted 
state from the storage layer into a `RestoredRaftState` object. Then, you can 
use this object to create the Raft node back. 

~~~~{.java}
RaftNodeRuntime runtime = ... // create Raft node runtime 
StateMachine stateMachine = ... // create state machine 
RestoredRaftState restoredState = ... // restore state from storage

RaftNode raftNode = RaftNode.newBuilder()
                            .setRestoredState(restoredState)
                            .setRuntime(runtime)
                            .setStateMachine(stateMachine)
                            .build();
    
raftNode.start(); 
~~~~

![](/img/warning.png){: style="height:25px;width:25px"} When a Raft node is
created with a restored Raft state, it discovers the current commit index of
the Raft group and replays the Raft log, i.e., automatically applies all of 
the log entries up to the commit index. You should be careful about 
the operations that have side-effects because the Raft log replay triggers 
those side-effects again.

![](/img/warning.png){: style="height:25px;width:25px"} If Raft nodes are not
created with an actual `RaftStore` implementation in the beginning, restarting
crashed Raft nodes with the same `RaftEndpoint` identity breaks the safety of
the Raft consensus algorithm. When there is no persistence layer, the only 
recovery option for a failed Raft node is to remove it from the Raft group.
  

## How to Change Member List of a Raft Group

MicroRaft supports membership changes in Raft groups via 
the `RaftNode.changeMembership()` method.

![](/img/info.png){: style="height:25px;width:25px"} Raft group membership
changes are appended to the internal Raft log as custom log entries and 
committed just like user-supplied operations. Therefore, Raft group membership 
changes require the majority of the Raft group to be operational. 

When a membership change is committed, its commit index is used to denote 
the new member list of the Raft group and called 
__group members commit index__. When a new membership change is triggered via 
`RaftNode.changeMembership()`, the current __group members commit index__ must 
be provided as well. 

MicroRaft allows one Raft group membership change at a time and more complex 
changes must be applied as a series of single changes. Suppose you have a 
3-member Raft group and you want to improve its degree of fault tolerance by
adding 2 new members (majority of 3 = 2 -> majority of 5 = 3). 

~~~~{.java}
RaftEndpoint member1 = ... 
RaftEndpoint member2 = ... 
RaftEndpoint member3 = ...

List<RaftEndpoint> initialGroupMembers 
	= Arrays.asList(member1, member2, member3);

// We have our first 3 RaftNode instances up and running

RaftEndpoint member4 = ...

RaftNode leader = ...

// group members commit index of the initial Raft group members is 0.
CompletableFuture<Ordered<RaftGroupMembers>> future1 
	= leader.changeMembership(member4, MembershipChangeMode.ADD, 0);

Ordered<RaftGroupMembers> groupMembersAfterFirstChange = future1.get();

// Our RaftNode is added to the Raft group. We can start it now. 
// Notice that we need to provide the same initial Raft group member list
// to the new RaftNode instance.

RaftNode raftNode4 = RaftNode.newBuilder()
                             .setInitialGroupMembers(initialGroupMembers)
                             .setLocalEndpoint(member4)
                             .setRuntime(...)
                             .setStateMachine(...)
                             .build();
                             
raftNode4.start();

// Now our Raft group has 4 members, whose majority is 3. 
// It means that the new RaftNode does not make a difference in terms of
// the degree of fault tolerance. We will add one more RaftNode.

RaftEndpoint member5 = ...

// here, we provide the commit index of the current Raft group member list.
long commitIndex = groupMembersAfterFirstChange.getCommitIndex();
CompletableFuture<Ordered<RaftGroupMembers>> future2 
	= leader.changeMembership(member5, MembershipChangeMode.ADD, commitIndex);

future2.get();

// 5th RaftNode is also added to the Raft group. 
// The majority is 3 now and we can tolerate failure of 2 RaftNode instances.

RaftNode raftNode5 = RaftNode.newBuilder()
                             .setInitialGroupMembers(initialGroupMembers)
                             .setLocalEndpoint(member4)
                             .setRuntime(...)
                             .setStateMachine(...)
                             .build();

raftNode5.start();
~~~~

Similarly, crashed Raft nodes can be removed from the Raft group. In order to
replace a non-recoverable Raft node without hurting the overall availability
of the system, you should remove the crashed Raft node first and then add 
a new `RaftEndpoint` to the Raft group. 
