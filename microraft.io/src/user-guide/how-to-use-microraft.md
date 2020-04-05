
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
crashed Raft nodes. You can persist the internal Raft node state to stable
storage via the `RaftStore` interface and recover from Raft node crashes by
restoring persisted Raft state. Otherwise, you could use the already-existing
`NopRaftStore` utility which makes the internal Raft state volatile and 
disables crash-recovery. If persistence is not implemented, crashed Raft nodes
cannot be restarted and need to be removed from the Raft group to not to damage
availability. Note that the lack of persistence limits the overall fault 
tolerance capabilities of your Raft group. Please refer to the 
[Fault Tolerance](/fault-tolerance) section to learn more about 
MicroRaft's fault tolerance capabilities. 

* ![](/img/info.png){: style="height:25px;width:25px"} build a discovery and 
RPC mechanism to replicate and commit your operations on a Raft group. This is 
required because simplicity is the primary concern for MicroRaft's design
philosophy. MicroRaft offers a minimum API set to cover the fundamental
functionality and enables its users to implement higher-level abstractions, 
such as an RPC system with request routing, retries, deduplication, etc. For 
instance, MicroRaft does not broadcast the Raft group members to any external 
discovery system, or it does not integrate with any observability tool, but it
exposes all necessary information, such as the current Raft group members, 
the leader endpoint, term, commit index, etc. via `RaftNode.getReport()`. You 
can use that information to feed your discovery services and monitoring tools. 
Similarly, the public APIs on `RaftNode` do not employ request routing or retry
mechanisms. For instance, if a client tries to replicate an operation via a 
follower or a candidate `RaftNode`, the returned `CompletableFuture` object is 
simply notified with `NotLeaderException`, instead of internally forwarding 
the operation to the leader `RaftNode`. `NotLeaderException` also contains
`RaftEndpoint` of the current leader so that the client can send its request to 
the leader `RaftNode`. 

## Code Samples

In this part, we will walk through several code samples to see MicroRaft in 
action. All of the code samples here are compiling and available in the Github
repo. You can just clone the repository and run the code samples on your 
machine to witness the magic!

MicroRaft offers a set of utilities to enable local testing and we will use
them here. These utilities are mainly used for testing MicroRaft to a great 
extend without a distributed setting. Here, we will use them to run our Raft
group in a single JVM process. Each Raft node will run on its own thread and
send send Raft messages to each other via multi-threaded queues. 
Ok, let's rock n roll!  

## Bootstrapping a Raft Group

The following code shows how to create a 3-member Raft group in a single JVM.

<script src="https://gist.github.com/metanet/7e0a3160192994373bec225cd351297d.js"></script>

To run this code sample on your machine, try the following:

~~~~{.bash}
 $ git clone git@github.com:metanet/MicroRaft.git
 $ cd MicroRaft && ./mvnw clean test -Dtest=io.microraft.examples.RaftGroupBootstrapTest -DfailIfNoTests=false -Pcode-sample
~~~~

Ok. That is a big piece of code, but no worries. We will swallow it one piece
at a time. 

Before bootstrapping a new Raft group, we first decide on its initial member 
list, i.e., the list of Raft endpoints. The very same initial Raft group member
list must be provided to all Raft nodes, including the ones we will start and 
join to our already-running Raft group later. It is what we do in the beginning
of the code, by creating 3 unique `RaftEndpoint` objects and populating our 
`initialMembers` list. `LocalEndpoint` is a simple class consisting of only a 
unique id field to differentiate our Raft nodes and each time we call 
`LocalRaftEndpoint.newEndpoint()`, we get a new unique Raft endpoint.

Then, for the sake of the example, we just populate our `RaftConfig` object but
we don't really need to do it if we are happy with the default configuration.

In the for loop, we create and start our Raft node instances. First, we create
our `RaftRuntime` object, which is a `LocalRaftNodeRuntime`. This class 
internally uses a single-threaded scheduled executor to run the Raft algorithm,
and handle requests coming from clients and Raft messages coming from the other
Raft node instances. We also create a `SimpleStateMachine` object as our 
`StateMachine` implementation about which we will talk in the next part. 

Once we create a Raft node via `RaftNodeBuilder`, its initial status is 
`RaftNodeStatus.INITIAL` and it does not execute the Raft consensus algorithm
in this status. When `RaftNode.start()` is called, the Raft node moves to
`RaftNodeStatus.ACTIVE` and starts doing its magic!  

Just after the for loop, we pass our Raft node runtime and Raft node objects to
the `enableDiscovery()` method. This is because we have our 
`LocalRaftNodeRuntime` objects which don't know how to talk to each other yet.
So, in this method, we just pass `RaftNode` instances to each other so that 
when a Raft message is sent to any Raft endpoint, our `LocalRaftNodeRuntime`
can pass it to the corresponding Raft node.

Let's run this code and see the logs. The logs start like the following:

~~~~{.text}
23:54:38.121 INFO - [RaftNode] node2<default> Starting for default with 3 members: [LocalRaftEndpoint{id=node1}, LocalRaftEndpoint{id=node2}, LocalRaftEndpoint{id=node3}]
23:54:38.121 INFO - [RaftNode] node3<default> Starting for default with 3 members: [LocalRaftEndpoint{id=node1}, LocalRaftEndpoint{id=node2}, LocalRaftEndpoint{id=node3}]
23:54:38.121 INFO - [RaftNode] node1<default> Starting for default with 3 members: [LocalRaftEndpoint{id=node1}, LocalRaftEndpoint{id=node2}, LocalRaftEndpoint{id=node3}]
23:54:38.123 INFO - [RaftNode] node2<default> Status is set to ACTIVE
23:54:38.123 INFO - [RaftNode] node3<default> Status is set to ACTIVE
23:54:38.123 INFO - [RaftNode] node1<default> Status is set to ACTIVE
23:54:38.125 INFO - [RaftNode] node3<default> Raft Group Members {groupId: default, size: 3, term:0, logIndex: 0} [
	node1
	node2
	node3 - FOLLOWER this (ACTIVE)
] reason: STATUS_CHANGE

23:54:38.125 INFO - [RaftNode] node2<default> Raft Group Members {groupId: default, size: 3, term:0, logIndex: 0} [
	node1
	node2 - FOLLOWER this (ACTIVE)
	node3
] reason: STATUS_CHANGE

23:54:38.125 INFO - [RaftNode] node3<default> started.
23:54:38.125 INFO - [RaftNode] node1<default> Raft Group Members {groupId: default, size: 3, term:0, logIndex: 0} [
	node1 - FOLLOWER this (ACTIVE)
	node2
	node3
] reason: STATUS_CHANGE

23:54:38.125 INFO - [RaftNode] node2<default> started.
23:54:38.125 INFO - [RaftNode] node1<default> started.
~~~~

Each RaftNode first prints the id and initial member list of the Raft group.
When we call `RaftNode.start()`, they switch to the `ACTIVE` status and print
a summary of the Raft group, including the member list. Each node also marks
itself in the Raft group member list, and the reason of why the log is printed
so that we can follow what is going on. 

After this part, our Raft nodes start a leader election. Raft can be very 
chatty during leader elections. For the sake of simplicity, we will just skip
the leader election logs of MicroRaft and look at the final status.

~~~~{.text}
23:54:39.141 INFO - [VoteResponseHandler] node3<default> Vote granted from node1 for term: 2, number of votes: 2, majority: 2
23:54:39.141 INFO - [VoteResponseHandler] node3<default> We are the LEADER!
23:54:39.149 INFO - [AppendEntriesRequestHandler] node1<default> Setting leader: node3
23:54:39.149 INFO - [RaftNode] node3<default> Raft Group Members {groupId: default, size: 3, term:2, logIndex: 0} [
	node1
	node2
	node3 - LEADER this (ACTIVE)
] reason: ROLE_CHANGE

23:54:39.149 INFO - [AppendEntriesRequestHandler] node2<default> Setting leader: node3
23:54:39.149 INFO - [RaftNode] node1<default> Raft Group Members {groupId: default, size: 3, term:2, logIndex: 0} [
	node1 - FOLLOWER this (ACTIVE)
	node2
	node3 - LEADER
] reason: ROLE_CHANGE

23:54:39.149 INFO - [RaftNode] node2<default> Raft Group Members {groupId: default, size: 3, term:2, logIndex: 0} [
	node1
	node2 - FOLLOWER this (ACTIVE)
	node3 - LEADER
] reason: ROLE_CHANGE
~~~~   

As you see, we managed to elect our leader in the second term. It means that
we had a split-vote situation in the first term. This is quite normal because
in our example we start our Raft nodes at the same time and each Raft node just
votes for itself during the first term.

Ok. we are done with our first step and now we know how to bootstrap a Raft 
group. In the following parts, we will use another utility class: 
`LocalRaftGroup` to simplify our code samples. `LocalRaftGroup` will do 
the heavy lifting for us, such as enabling discovery, waiting until a leader is
elected, etc.   

## Committing an Operation on the Raft Group

Let's see how to replicate and commit an operation via the Raft group leader.
Just we discussed earlier, we will continue with `LocalRaftGroup` to simplify
our code samples. Here, we start a Raft group of 3 Raft nodes with the default
MicroRaft configuration. Then, we call 
`LocalRaftGroup.waitUntilLeaderElected()` to wait for the leader election to be
completed, and then get our leader `RaftNode` instance.
 
<script src="https://gist.github.com/metanet/96fc904c59da940b7e6b92a6b9e20778.js"></script>

To run this code sample on your machine, try the following:

~~~~{.bash}
$ git clone git@github.com:metanet/MicroRaft.git
$ cd MicroRaft && ./mvnw clean test -Dtest=io.microraft.examples.OperationCommitTest -DfailIfNoTests=false -Pcode-sample
~~~~
 
Ok, now let's talk about `SimpleStateMachine`. `SimpleStateMachine` just 
collects all committed values along with their commit indices. We call 
`SimpleStateMachine.apply("value")` to create an operation to commit `"value"`.
MicroRaft will execute this operation once it is committed and as a result it
will just return the value we provided to it. 

Most of the APIs in the `RaftNode` interface return 
`CompletableFuture<Ordered>` objects. For `RaftNode.replicate()`, `Ordered` 
provides the result of the operation execution, and on which Raft log index our
operation has been committed and executed. So our `sysout` line prints 
the following:

~~~~{.text}
operation result: value, commit index: 1
~~~~

If we call `RaftNode.replicate()` on a follower or a candidate Raft node, 
the returned `CompletableFuture<Ordered>` object is simply notified with 
`NotLeaderException`, which also provides `RaftEndpoint` of the leader 
Raft node. I am not going to build an RPC system in front of MicroRaft here,
but when we use MicroRaft in a distributed setting, we can build a retry 
mechanism in the RPC layer to forward our failed operation to 
the `RaftEndpoint` given in the exception. `NotLeaderException` may not 
specify any leader as well, for example if there is an ongoing leader election
round or the Raft node we contacted does not know the leader yet. In this case,
our RPC layer could retry the operation on each Raft node in a round robin 
fashion until it discovers the new leader. 

## Performing a Query

MicroRaft offers a separate API: `RaftNode.query()` to handle queries (i.e., 
read-only operations). MicroRaft differentiates updates and queries to employ
some optimizations for the execution of the queries. Namely, it offers 3 
policies for queries, each with a different consistency guarantee:

* `QueryPolicy.LINEARIZABLE`: We can perform a linearizable query with this
policy. MicroRaft employs the optimization described in *Section: 6.4 
Processing read-only queries more efficiently* of 
[the Raft dissertation](https://github.com/ongardie/dissertation) to preserve
linearizability without growing the internal Raft log. We need to hit 
the leader `RaftNode` to execute a linearizable query.

* `QueryPolicy.LEADER_LOCAL`: We can run a query locally on the leader Raft
node without making the leader talk to the quorum. If the called Raft node 
is not the leader, the returned `CompletableFuture<Ordered>` object 
is notified with with `NotLeaderException`. 

* `QueryPolicy.ANY_LOCAL`: We can use this policy to run a query locally on
any Raft node independent of its current role. This policy provides the weakest
consistency guarantee but it can help us to distribute our read-workload by
utilizing follower Raft nodes.

MicroRaft also employs leader stickiness and auto-demotion of leaders on loss 
of majority heartbeats. Leader stickiness means that a follower does not vote 
for another Raft node before the __leader heartbeat timeout__ elapses after 
the last received __AppendEntries__ or __InstallSnapshot__ RPC. Dually, 
a leader Raft node automatically demotes itself to the follower role if it has
not received `AppendEntriesRPC` responses from the majority during the __leader
heartbeat timeout__ duration. Along with these techniques, 
`QueryPolicy.LEADER_LOCAL` can be used for performing linearizable queries 
without talking to the majority if the clock drifts and network delays are 
bounded. However, bounding clock drifts and network delays is not an easy task.
Hence, `LEADER_LOCAL` may cause reading stale data if a Raft node still 
considers itself as the leader because of a clock drift even though the other 
Raft nodes have elected a new leader and committed new operations. Moreover, 
`LEADER_LOCAL` and `LINEARIZABLE` policies have the same processing cost since
only the leader Raft node runs a given query for both policies. `LINEARIZABLE`
policy guarantees linearizability with an 1 extra RTT latency overhead compared
to the `LEADER_LOCAL` policy. For these reasons, `LINEARIZABLE` is 
the recommended policy for linearizable queries and `LEADER_LOCAL` should be 
used carefully.

Ok. Let's see some code samples for querying. We will first perform a query 
with linearizability. In the code sample below, we first replicate 
an operation and keep its commit index. Then, we create a query operation
and pass it to `RaftNode.query()` along with `QueryPolicy.LINEARIZABLE`. Just
ignore the third parameter, we will talk about it in a minute.   

<script src="https://gist.github.com/metanet/4c3536653a2bd152899a41aa654b3f2d.js"></script>

To run this code sample on your machine, try the following:

~~~~{.bash}
$ git clone git@github.com:metanet/MicroRaft.git
$ cd MicroRaft && ./mvnw clean test -Dtest=io.microraft.examples.LinearizableQueryTest -DfailIfNoTests=false -Pcode-sample
~~~~

If we run this log, we see that our `sysout` lines print the following:

~~~~{.text}
operation result: value, commit index: 1
query result: value, commit index: 1
~~~~

As I mentioned earlier, MicroRaft handles linearizable queries without growing
the Raft log. That is we we are seeing both commit indices as `1` here.

`LEADER_LOCAL` and `ANY_LOCAL` policies can be easily used if monotonicity is
sufficient for the query results returned to the clients. A client can track 
the commit indices observed via the returned `Ordered` objects and use 
the highest observed commit index to preserve monotonicity while performing 
a local query on a Raft node. If the local commit index of a Raft node is 
smaller than the commit index passed to the `RaftNode.query()` call, 
the returned `CompletableFuture` object fails with 
`LaggingCommitIndexException`. In this case, the client can retry its query on
another Raft node. Please refer to 
[Section 6.4.1 of the Raft dissertation](https://github.com/ongardie/dissertation)
for more details.

We will use the firewall functionality of `LocalRaftGroup` to demonstrate
how we can maintain monotonicity for our `QueryPolicy.ANY_LOCAL` policies. 
In this code sample, we first replicate a value via the Raft leader and block 
the communication between the leader and a follower afterwards. Then we
replicate a second value. Since we blocked the communication between the leader
and a follower, our follower will not have this second value. After the second
value is also committed, we issue a `QueryPolicy.ANY_LOCAL` query. We are 
tracking the commit indices we observe. If you look carefully, you will see 
that we pass the last commit index we observed to our query. Now, we switch to
our `disconnectedFollower` and issue a new query by passing the last observed
commit index again. Since the `disconnectedFollower` does not have the second
commit yet, it cannot satisfy the monotonicity we require, hence it fails by
throwing `LaggingCommitIndexException`.
 
<script src="https://gist.github.com/metanet/020956b893d68f13aced8dfb911f2e81.js"></script>

To run this code sample on your machine, try the following:

~~~~{.bash}
$ git clone git@github.com:metanet/MicroRaft.git
$ cd MicroRaft && ./mvnw clean test -Dtest=io.microraft.examples.MonotonicLocalQueryTest -DfailIfNoTests=false -Pcode-sample
~~~~

Our `sysout` lines print the following logs for this code sample:

~~~~{.text}
replicate1 result: value1, commit index: 1
replicate2 result: value2, commit index: 2
query1 result: value2, commit index: 2
Disconnected follower could not preserve monotonicity for local query at commit index: 2
~~~~


## Changing the Member List of the Raft Group

MicroRaft supports membership changes in Raft groups via 
the `RaftNode.changeMembership()` method.

![](/img/info.png){: style="height:25px;width:25px"} Raft group membership
changes are appended to the internal Raft log as regular log entries and 
committed just like user-supplied operations. Therefore, Raft group membership 
changes require the majority of the Raft group to be operational. 

![](/img/info.png){: style="height:25px;width:25px"} When a membership change 
is committed, its commit index is used to denote the new member list of 
the Raft group and called __group members commit index__. When a new membership
change is triggered via `RaftNode.changeMembership()`, the current __group 
members commit index__ must be provided as well. 

![](/img/info.png){: style="height:25px;width:25px"} Last, MicroRaft allows one
Raft group membership change at a time and more complex changes must be applied
as a series of single changes. 

Since we know the rules for member list changes now, let's see some code.
Suppose we have a 3-member Raft group and we want to improve its degree of fault 
tolerance by adding 2 new members (majority of 3 = 2 -> majority of 5 = 3). 
 
<script src="https://gist.github.com/metanet/4f0ab94b78b369a1cc9ac58ef0e6f011.js"></script>

To run this code sample on your machine, try the following:

~~~~{.bash}
$ git clone git@github.com:metanet/MicroRaft.git
$ cd MicroRaft && ./mvnw clean test -Dtest=io.microraft.examples.ChangeMemberListOfRaftGroupTest -DfailIfNoTests=false -Pcode-sample
~~~~

We start by creating a 3-node Raft group in the `init()` method and wait until
a leader is elected. Then, we create a new Raft endpoint `endpoint4` and add it
to the Raft group with the following line:

~~~~{.java}
RaftGroupMembers newMemberList1 = leader.changeMembership(endpoint4, MembershipChangeMode.ADD, 0).join().getResult();
System.out.println("New member list 1: " + newMemberList1.getMembers() + ", majority: " + newMemberList1.getMajority()
                           + ", commit index: " + newMemberList1.getLogIndex());
// endpoint4 is now part of the member list. Let's start its Raft node
RaftNode raftNode4 = createRaftNode(endpoint4);
raftNode4.start();
~~~~   

Please notice that we pass `0` for the third argument, because before this line
our Raft group operates with the initial member list. After this part, 
`endpoint4` is part of the Raft group, so we start its Raft node as well. 

Our `sysout` line here prints the following: 

~~~~{.text}
New member list: [LocalRaftEndpoint{id=node1}, LocalRaftEndpoint{id=node2}, LocalRaftEndpoint{id=node3}, LocalRaftEndpoint{id=node4}], majority: 3, commit index: 3
~~~~

As you see, our Raft group now has 4 members, whose majority is 3. Actually, 
running a 4-node Raft group has no advantage over running a 3-node Raft group 
in terms of the degree of fault tolerance because both of them have the same
majority value. Since we want to improve our degree of fault tolerance, we will
add one more Raft node to our Raft group. 

So we create a new Raft endpoint, `endpoint5`, add it to the Raft group, and
start its Raft node. But this time, as the __group members commit index__ 
parameter, we pass `newMemberList1.getLogIndex()` which is the log index 
`endpoint4` is added to the Raft group. Our second `sysout` line prints 
the following:

~~~~{.text}
New member list: [LocalRaftEndpoint{id=node1}, LocalRaftEndpoint{id=node2}, LocalRaftEndpoint{id=node3}, LocalRaftEndpoint{id=node4}, LocalRaftEndpoint{id=node5}], majority: 3, commit index: 5
~~~~

Now we have 4 nodes in our Raft group and the majority is 2. It means that now
our Raft group can tolerate failure of 2 Raft nodes and still remain operational. 
Voila! 


## Monitoring the Raft Group

`RaftNodeReport` class contains detailed information about internal state of
a `RaftNode`, such as its Raft role, term, leader, last log index, commit 
index, etc. MicroRaft provides multiple mechanisms to monitor internal state of 
`RaftNode` instances via `RaftNodeReport` objects:

1. We can build a simple pull-based system to query `RaftNodeReport` objects 
via the `RaftNode.getReport()` API and publish those objects to any external
monitoring system. 

2. The `RaftNodeRuntime` abstraction contains a hook method, 
`RaftNodeRuntime#handleRaftNodeReport()`, which is called by MicroRaft anytime 
there is an important change in internal state of a `RaftNode`, such as 
a leader change, a term change, or a snapshot installation. We can also use 
that method to capture `RaftNodeReport` objects and notify external monitoring
systems promptly. 

We have seen a lot of code samples until here. I am just leaving this part as
an exercise to the reader.


## What's Next?

In the next part, we will see how MicroRaft deals with failures. Just 
[keep calm and carry on](resiliency-and-fault-tolerance.md) if you are still 
interested!
