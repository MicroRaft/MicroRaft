
# Tutorial: Building an Atomic Register

In this section, we will build an atomic register on top of MicroRaft to
demonstrate how to implement and use MicroRaft's main abstractions. Our atomic
register will consist of a single value and only 3 operations: `set`,
`compare-and-set`, and `get`. In order to keep things simple, we will not run
our Raft group in a distributed setting. Instead, will run each Raft node on a
different thread and make them communicate with each other via multi-threaded
queues.

If you haven't read the [Main Abstractions](main-abstractions.md)
section yet, I highly recommend you to read that section before this tutorial.

All the code shown here are compiling and available in the <a
href="https://github.com/MicroRaft/MicroRaft/tree/master/microraft-tutorial"
target="_blank">MicroRaft Github repository</a>. You can clone the repository
and run the code samples on your machine to try each part yourself. I
intentionally duplicated a lot of code in the test classes below to put all
pieces together so that you can see what is going on without navigating through
multiple classes.

Let's crank up the engine!

## 1. Implementing the Main Abstractions

We will start with writing our <a
href="https://github.com/MicroRaft/MicroRaft/blob/master/microraft/src/main/java/io/microraft/RaftEndpoint.java"
target="_blank">`RaftEndpoint`</a>, <a
href="https://github.com/MicroRaft/MicroRaft/blob/master/microraft/src/main/java/io/microraft/statemachine/StateMachine.java"
target="_blank">`StateMachine`</a> and <a
href="https://github.com/MicroRaft/MicroRaft/blob/master/microraft/src/main/java/io/microraft/transport/Transport.java"
target="_blank">`Transport`</a> classes. We can use the default implementations
of <a
href="https://github.com/MicroRaft/MicroRaft/blob/master/microraft/src/main/java/io/microraft/executor/RaftNodeExecutor.java"
target="_blank">`RaftNodeExecutor`</a>, <a
href="https://github.com/MicroRaft/MicroRaft/blob/master/microraft/src/main/java/io/microraft/model/RaftModel.java"
target="_blank">`RaftModel`</a> and <a
href="https://github.com/MicroRaft/MicroRaft/blob/master/microraft/src/main/java/io/microraft/model/RaftModelFactory.java"
target="_blank">`RaftModelFactory`</a> abstractions. Since all of our Raft nodes
will run in the same JVM process, we also don't need any serialization logic
inside our `Transport` implementation. Last, we will also skip persistence. Our
Raft nodes will keep their state only in memory.

### `RaftEndpoint`

First, we need to implement `RaftEndpoint` to represent identity of our Raft
nodes. Since we don't distribute our Raft nodes to multiple servers in this
tutorial, we don't really need IP addresses. We can simply identify our Raft
nodes with strings IDs and keep a mapping of unique IDs to Raft nodes so that we
can deliver <a
href="https://github.com/MicroRaft/MicroRaft/blob/master/microraft/src/main/java/io/microraft/model/message/RaftMessage.java"
target="_blank">`RaftMessage`</a> objects to target Raft nodes.

Let's write a `LocalRaftEndpoint` class as below. We will generate unique Raft
endpoints via its static `LocalRaftEndpoint.newEndpoint()` method.

<script src="https://gist.github.com/metanet/8d33a46f3c927bfd3af38fcde35d8161.js"></script>
  
You can also see this class in the <a
href="https://github.com/MicroRaft/MicroRaft/blob/master/microraft-tutorial/src/main/java/io/microraft/tutorial/LocalRaftEndpoint.java"
target="_blank">MicroRaft Github repository</a>.  
  
### `StateMachine`

We will implement our atomic register state machine iteratively. In the first
iteration, we will create our Raft nodes and elect a leader without committing
any atomic register operation. So the first version of our state machine, which
is shown below, does not have any execution and snapshotting logic for our
atomic register.

We implement <a
href="https://github.com/MicroRaft/MicroRaft/blob/master/microraft/src/main/java/io/microraft/statemachine/StateMachine.java"
target="_blank">`StateMachine.getNewTermOperation()`</a> in our first version of
state machine. This method returns an operation which will be committed every
time a new leader is elected. This is actually related to the <a
href="https://groups.google.com/forum/#!msg/raft-dev/t4xj6dJTP6E/d2D9LrWRza8J"
target="_blank">single-server membership change bug</a> in the Raft consensus
algorithm rather than our atomic register logic.

Once we see that we are able to form a Raft group and elect a leader, we will
extend this class to implement the missing functionality.

<script src="https://gist.github.com/metanet/0acbb6426640f02fc88ad078fa8f59f2.js"></script>

You can also see this class in the <a
href="https://github.com/MicroRaft/MicroRaft/blob/master/microraft-tutorial/src/main/java/io/microraft/tutorial/atomicregister/AtomicRegister.java"
target="_blank">MicroRaft Github repository</a>.

### `Transport`

We are almost there to run our first test for bootstrapping a Raft group and
electing a leader. The only missing piece is <a
href="https://github.com/MicroRaft/MicroRaft/blob/master/microraft/src/main/java/io/microraft/Transport/Transport.java"
target="_blank">`Transport`</a>. Recall that `Transport` is responsible for
sending Raft messages to other Raft nodes (serialization and networking). Since
our Raft nodes will run in a single JVM process in this tutorial, we will skip
serialization. To mimic networking, we will keep a mapping of Raft endpoints to
Raft nodes on each `Transport` object and pass Raft messages to between Raft
nodes by using this mapping.

Our `LocalTransport` class is shown below.

<script src="https://gist.github.com/metanet/efac8da9b92529391729221625b1ea75.js"></script>

You can also see this class in the <a
href="https://github.com/MicroRaft/MicroRaft/blob/master/microraft-tutorial/src/main/java/io/microraft/tutorial/LocalTransport.java"
target="_blank">MicroRaft Github repository</a>.

-----

## 2. Bootstrapping the Raft group

Now we have all the required functionality to start our Raft group and elect a
leader. Let's write our first test.

<script src="https://gist.github.com/metanet/7e0a3160192994373bec225cd351297d.js"></script>

To run this test on your machine, try the following:

~~~~{.bash}
 $ git clone https://github.com/MicroRaft/MicroRaft.git
 $ cd MicroRaft && ./mvnw clean test -Dtest=io.microraft.tutorial.LeaderElectionTest -DfailIfNoTests=false -Ptutorial
~~~~

You can also see it in the <a
href="https://github.com/MicroRaft/MicroRaft/blob/master/microraft-tutorial/src/test/java/io/microraft/tutorial/LeaderElectionTest.java"
target="_blank">MicroRaft Github repository</a>.

Ok. That is a big piece of code, but no worries. We will swallow it one piece at
a time.

Before bootstrapping a new Raft group, we first decide on its initial member
list, i.e., the list of Raft endpoints. The very same initial Raft group member
list must be provided to all Raft nodes, including the ones we will start later
and join to our already-running Raft group. It is what we do in the beginning of
the class by populating `initialMembers` with 3 unique Raft endpoints.

`startRaftGroup()` calls `createRaftNode()` for each Raft endpoint and then
starts the created Raft nodes. When a new Raft node is created, it does not know
how to talk to the other Raft nodes. Therefore, we pass created Raft node
objects to `enableDiscovery()` to enable them to talk to each other. This method
just adds a given Raft node to the *discovery maps* of the `LocalTransport`
objects of the other Raft nodes.

Once we create a Raft node via <a
href="https://github.com/MicroRaft/MicroRaft/blob/master/microraft/src/main/java/io/microraft/RaftNode.java"
target="_blank">`RaftNodeBuilder`</a>, its initial status is <a
href="https://github.com/MicroRaft/MicroRaft/blob/master/microraft/src/main/java/io/microraft/RaftNodeStatus.java"
target="_blank">`RaftNodeStatus.INITIAL`</a> and it does not execute the Raft
consensus algorithm in this status. When `RaftNode.start()` is called, its
status becomes <a
href="https://github.com/MicroRaft/MicroRaft/blob/master/microraft/src/main/java/io/microraft/RaftNodeStatus.java"
target="_blank">`RaftNodeStatus.ACTIVE`</a> and the Raft node internally submits
a task to its `RaftNodeExecutor` to check if there is a leader. Since we are
starting a new Raft group in this test, obviously there is no leader yet so our
Raft nodes will start a new leader election round.  

The actual test method is rather short. It calls `waitUntilLeaderElected()` to
get the leader Raft node and asserts that a Raft node is returned as leader.
Please note that this method offers the discovery functionality to find the
leader. It just waits until all Raft nodes report the same Raft node as the
leader.

Let's run this code and see the logs. The logs start like following:

~~~~{.text}
23:54:38.121 INFO - [RaftNode] node2<default> Starting for default with 3 members: [LocalRaftEndpoint{id=node1}, LocalRaftEndpoint{id=node2}, LocalRaftEndpoint{id=node3}]
23:54:38.121 INFO - [RaftNode] node3<default> Starting for default with 3 members: [LocalRaftEndpoint{id=node1}, LocalRaftEndpoint{id=node2}, LocalRaftEndpoint{id=node3}]
23:54:38.121 INFO - [RaftNode] node1<default> Starting for default with 3 members: [LocalRaftEndpoint{id=node1}, LocalRaftEndpoint{id=node2}, LocalRaftEndpoint{id=node3}]
23:54:38.123 INFO - [RaftNode] node2<default> Status is set to ACTIVE
23:54:38.123 INFO - [RaftNode] node3<default> Status is set to ACTIVE
23:54:38.123 INFO - [RaftNode] node1<default> Status is set to ACTIVE
23:54:38.125 INFO - [RaftNode] node3<default> Raft Group Members {groupId: default, size: 3, term: 0, logIndex: 0} [
	node1
	node2
	node3 - FOLLOWER this (ACTIVE)
] reason: STATUS_CHANGE

23:54:38.125 INFO - [RaftNode] node2<default> Raft Group Members {groupId: default, size: 3, term: 0, logIndex: 0} [
	node1
	node2 - FOLLOWER this (ACTIVE)
	node3
] reason: STATUS_CHANGE

23:54:38.125 INFO - [RaftNode] node3<default> started.
23:54:38.125 INFO - [RaftNode] node1<default> Raft Group Members {groupId: default, size: 3, term: 0, logIndex: 0} [
	node1 - FOLLOWER this (ACTIVE)
	node2
	node3
] reason: STATUS_CHANGE

23:54:38.125 INFO - [RaftNode] node2<default> started.
23:54:38.125 INFO - [RaftNode] node1<default> started.
~~~~

Each RaftNode first prints the id and initial member list of the Raft group.
When we call `RaftNode.start()`, they switch to the `RaftNodeStatus.ACTIVE`
status and print a summary of the Raft group, including the member list. Each
node also marks itself in the Raft group member list, and the reason of why the
log is printed so that we can follow what is going on.

After this part, our Raft nodes start a leader election. Raft can be very chatty
during leader elections. For the sake of simplicity, we will just skip the
leader election logs and look at the final status.

~~~~{.text}
23:54:39.141 INFO - [VoteResponseHandler] node3<default> Vote granted from node1 for term: 2, number of votes: 2, majority: 2
23:54:39.141 INFO - [VoteResponseHandler] node3<default> We are the LEADER!
23:54:39.149 INFO - [AppendEntriesRequestHandler] node1<default> Setting leader: node3
23:54:39.149 INFO - [RaftNode] node3<default> Raft Group Members {groupId: default, size: 3, term: 2, logIndex: 0} [
	node1
	node2
	node3 - LEADER this (ACTIVE)
] reason: ROLE_CHANGE

23:54:39.149 INFO - [AppendEntriesRequestHandler] node2<default> Setting leader: node3
23:54:39.149 INFO - [RaftNode] node1<default> Raft Group Members {groupId: default, size: 3, term: 2, logIndex: 0} [
	node1 - FOLLOWER this (ACTIVE)
	node2
	node3 - LEADER
] reason: ROLE_CHANGE

23:54:39.149 INFO - [RaftNode] node2<default> Raft Group Members {groupId: default, size: 3, term: 2, logIndex: 0} [
	node1
	node2 - FOLLOWER this (ACTIVE)
	node3 - LEADER
] reason: ROLE_CHANGE
~~~~   

As you see, we managed to elect our leader in the 2nd term. It means that we had
a split-vote situation in the 1st term. This is quite normal because in our test
code we start our Raft nodes at the same time and each Raft node just votes for
itself during the first term. Please keep in mind that in your run, another Raft
node could become the leader.

-----

## 3. Sending requests

Now it is time to implement the `set`, `compare-and-set`, and `get` operations
we talked before for our atomic register state machine. We must ensure that they
are implemented in a deterministic way, because it is a fundamental requirement
of the _replicated state machines_ approach. MicroRaft guarantees that each Raft
node will execute committed operations in the same order and since our
operations run deterministically, we know that our state machines will end up
with the same state after they execute committed operations.

To implement these operations, we will extend our `AtomicRegister` class instead
of modifying it so that the readers can follow the different stages of this
tutorial. The new state machine class is below. We define an interface,
`AtomicRegisterOperation`, as a marker for the operations we will execute on our
state machine. There are 3 inner classes implementing this interface for the
`set`, `compare-and-set` and `get` operations, and accompanying static methods
to create their instances. We will pass an `AtomicRegisterOperation` object to
`RaftNode.replicate()` and once it is committed by MicroRaft it will be passed
to our state machine for execution. Our new state machine class handles these
`AtomicRegisterOperation` objects in the `runOperation()` method. `set` updates
the atomic register with the given value and returns its previous value,
`compare-and-set` updates the atomic register with the given new value only if
its current value is equal to the given current value, and `get` simply returns
the current value of the atomic register. Please note that the snapshotting
logic is still missing and will be implemented later in the tutorial.  
 
<script src="https://gist.github.com/metanet/bda7eee359766b0a046b70dd262e2618.js"></script>

You can also see this class in the 
<a href="https://github.com/MicroRaft/MicroRaft/blob/master/microraft-tutorial/src/main/java/io/microraft/tutorial/atomicregister/OperableAtomicRegister.java" target="_blank">MicroRaft Github repository</a>.

### Committing operations

In the next test we will use the new state machine to start our Raft nodes. We
will commit a number of operations on the Raft group and verify their results.
We replicate 2 `set` operations, 2 `compare-and-set` operations, and a `get`
operation at the end. After each operation, we verify that its commit index is
greater than the commit index of the previous operation.  

<script src="https://gist.github.com/metanet/96fc904c59da940b7e6b92a6b9e20778.js"></script>

~~~~{.bash}
$ git clone https://github.com/MicroRaft/MicroRaft.git
$ cd MicroRaft && ./mvnw clean test -Dtest=io.microraft.tutorial.OperationCommitTest -DfailIfNoTests=false -Ptutorial
~~~~

You can also see it in the 
<a href="https://github.com/MicroRaft/MicroRaft/blob/master/microraft-tutorial/src/test/java/io/microraft/tutorial/OperationCommitTest.java" target="_blank">MicroRaft Github repository</a>.

We use `RaftNode.replicate()` to replicate and commit operations on the Raft
group. Most of the Raft node APIs, including `RaftNode.replicate()`, return
`CompletableFuture<Ordered>` objects. For `RaftNode.replicate()`, <a
href="https://github.com/MicroRaft/MicroRaft/blob/master/microraft/src/main/java/io/microraft/Ordered.java"
target="_blank">`Ordered`</a> provides return value of the executed operation
and on which Raft log index the operation has been committed.

The output of the first 2 `sysout` lines are below. Please note that `set`
returns previous value of the atomic register.

~~~~{.text}
1st operation commit index: 2, result: null
2nd operation commit index: 3, result: value1
~~~~

Then we commit 2 `cas` operations. The first `cas` manages to update the atomic
register, but the second one fails because the atomic register value is
different from the expected value.

~~~~{.text}
3rd operation commit index: 4, result: true
4th operation commit index: 5, result: false
~~~~

The last operation is a `get` to read the current value of the atomic register.

~~~~{.text}
5th operation commit index: 6, result: value3
~~~~

If we call `RaftNode.replicate()` on a follower or candidate Raft node, the
returned `CompletableFuture<Ordered>` object is simply notified with <a
href="https://github.com/MicroRaft/MicroRaft/blob/master/microraft/src/main/java/io/microraft/exception/NotLeaderException.java"
target="_blank">`NotLeaderException`</a>, which also provides Raft endpoint of
the leader Raft node. I am not going to build an advanced RPC system in front of
MicroRaft here, but when we use MicroRaft in a distributed setting, we can build
a retry mechanism in the RPC layer to forward a failed operation to the Raft
endpoint given in the exception. `NotLeaderException` may not specify any leader
as well, for example if there is an ongoing leader election round, or the Raft
node we contacted does not know the leader yet. In this case, our RPC layer
could retry the operation on each Raft node in a round robin fashion until it
discovers the new leader.

### Performing queries

The last operation we committed in our previous test is `get`, which is actually
a query (i.e, read only) operation. Even though it does not mutate the state
machine, since we used `RaftNode.replicate()`, it is appended to the Raft log
and committed similar to the other mutating operations. If we had persistence,
it means this approach would increase our disk usage unnecessarily because we
will persist every new entry in the Raft log, even if it is a query. Actually,
this is a sub-optimal approach.

MicroRaft offers a separate API, `RaftNode.query()`, to handle queries more
efficiently. There are <a
href="https://github.com/MicroRaft/MicroRaft/blob/master/microraft/src/main/java/io/microraft/QueryPolicy.java"
target="_blank">3 policies for queries</a>, each with a different consistency
guarantee:

* `QueryPolicy.LINEARIZABLE`: We can perform a linearizable query with this
  policy. MicroRaft employs the optimization described in *§ 6.4:
  Processing read-only queries more efficiently* of <a
  href="https://github.com/ongardie/dissertation" target="_blank">the Raft
  dissertation</a> to preserve linearizability without growing the internal Raft
  log. We need to hit the leader Raft node to execute a linearizable query.

* `QueryPolicy.LEADER_LOCAL`: We can run a query locally on the leader Raft node
  without talking to the majority. If the called Raft node is not the leader,
  the returned `CompletableFuture<Ordered>` object is notified with
  `NotLeaderException`.

* `QueryPolicy.ANY_LOCAL`: We can use this policy to run a query locally on any
  Raft node independent of its current role. This policy provides the weakest
  consistency guarantee, but it can help us to distribute our query-workload by
  utilizing followers. We can also utilize `Ordered` to preserve a monotonic
  view of the Raft group state.

MicroRaft also employs leader stickiness and auto-demotion of leaders on loss of
majority heartbeats. Leader stickiness means that a follower does not vote for
another Raft node before the *leader heartbeat timeout* elapses after the last
received *Append Entries* or *Install Snapshot* RPC. Dually, a leader Raft node
automatically demotes itself to the follower role if it does not receive *Append
Entries* RPC responses from the majority during the *leader heartbeat timeout*
duration. Along with these techniques, `QueryPolicy.LEADER_LOCAL` can be used
for performing linearizable queries without talking to the majority if clock
drifts and network delays are bounded. However, bounding clock drifts and
network delays is not an easy task. Hence, `QueryPolicy.LEADER_LOCAL` may cause
reading stale data if a Raft node still considers itself as the leader because
of a clock drift, while the other Raft nodes have elected a new leader and
committed new operations. Moreover, `QueryPolicy.LEADER_LOCAL` and
`QueryPolicy.LINEARIZABLE` have the same processing cost since only the leader
Raft node runs a given query for both policies. `QueryPolicy.LINEARIZABLE`
guarantees linearizability with an extra RTT latency overhead compared to
`QueryPolicy.LEADER_LOCAL`. For these reasons, `QueryPolicy.LINEARIZABLE` is the
recommended policy for linearizable queries, and `QueryPolicy.LEADER_LOCAL`
should be used carefully.

#### Linearizable queries

Ok. Let's write another test to use the query API for `get`. We will first set a
value to the atomic register and then get the value back with a linearizable
query. Just ignore the third parameter passed to the `RaftNode.query()` call for
now. We will talk about it in a minute.

<script src="https://gist.github.com/metanet/4c3536653a2bd152899a41aa654b3f2d.js"></script>

To run this test on your machine, try the following:

~~~~{.bash}
$ git clone https://github.com/MicroRaft/MicroRaft.git
$ cd MicroRaft && ./mvnw clean test -Dtest=io.microraft.tutorial.LinearizableQueryTest -DfailIfNoTests=false -Ptutorial
~~~~

You can also see it in the <a
href="https://github.com/MicroRaft/MicroRaft/blob/master/microraft-tutorial/src/test/java/io/microraft/tutorial/LinearizableQueryTest.java"
target="_blank">MicroRaft Github repository</a>.

The output of the `sysout` lines are below:

~~~~{.text}
set operation commit index: 2
get operation commit index: 2, result: value
~~~~

As we discussed above, MicroRaft handles linearizable queries without appending
a new entry to the Raft log. So our query is executed at the last committed log
index. That is why both commit indices are the same in the output.

#### Monotonic local queries

`QueryPolicy.LEADER_LOCAL` and `QueryPolicy.ANY_LOCAL` can be easily used if
monotonicity is sufficient for query results. This is where <a
href="https://github.com/MicroRaft/MicroRaft/blob/master/microraft/src/main/java/io/microraft/Ordered.java"
target="_blank">`Ordered`</a> comes in handy. A client can track commit indices
observed via returned `Ordered` objects and use the greatest observed commit
index to preserve monotonicity while issuing a local query to a Raft node. If
the local commit index of a Raft node is smaller than the commit index passed to
the `RaftNode.query()` call, the returned `CompletableFuture` object fails with
<a
href="https://github.com/MicroRaft/MicroRaft/blob/master/microraft/src/main/java/io/microraft/exception/LaggingCommitIndexException.java"
target="_blank">`LaggingCommitIndexException`</a>. This exception means that the
state observed by the client is more up-to-date than the contacted Raft node's
state. In this case, the client can retry its query on another Raft node. Please
refer to <a href="https://github.com/ongardie/dissertation"
target="_blank">§ 6.4.1 of the Raft dissertation</a> for more details.

We will make a little trick to demonstrate how to maintain the monotonicity of
the observed Raft group state for the *local query policies*. Recall that
`LocalTransport` keeps a *discovery map* to send Raft messages to target Raft
nodes. When we create a new Raft node, we add it to the other Raft nodes'
*discovery maps*. This time, we will do exactly the reverse to block the
communication between the leader and the follower. Let's add the following
method to our `LocalTransport` class:

~~~~{.java}
public void undiscoverNode(RaftNode node) {
    RaftEndpoint endpoint = node.getLocalEndpoint();
    if (localEndpoint.equals(endpoint)) {
        throw new IllegalArgumentException(localEndpoint + " cannot undiscover itself!");
    }

    nodes.remove(node.getLocalEndpoint(), node);
}
~~~~
 
We use this method in our new test below. We block the communication between the
leader and follower after we set a value to the atomic register. Then we set
another value which will not be replicated to the disconnected follower. After
this step, we issue a query on the leader. We are also tracking the commit index
we observed. Now, we switch to the disconnected follower and issue a new local
query by passing the last observed commit index. Since the disconnected follower
does not have the second commit, it cannot satisfy the monotonicity we demand,
hence our query fails with `LaggingCommitIndexException`.
 
<script src="https://gist.github.com/metanet/020956b893d68f13aced8dfb911f2e81.js"></script>

To run this test on your machine, try the following:

~~~~{.bash}
$ git clone https://github.com/MicroRaft/MicroRaft.git
$ cd MicroRaft && ./mvnw clean test -Dtest=io.microraft.tutorial.MonotonicLocalQueryTest -DfailIfNoTests=false -Ptutorial
~~~~

You can also see it in the <a
href="https://github.com/MicroRaft/MicroRaft/blob/master/microraft-tutorial/src/test/java/io/microraft/tutorial/MonotonicLocalQueryTest.java"
target="_blank">MicroRaft Github repository</a>.

-----

## 4. Snapshotting

Now it is time to implement the missing snapshotting functionality in our atomic
register state machine. Let's talk about snapshotting first. The Raft log grows
during the lifecycle of the Raft group as more operations are committed.
However, in a real-world system we cannot allow it to grow unboundedly. As the
Raft log grows longer, it will consume more space both in memory and disk, and
cause a lagging follower to catch up with the majority in a longer duration.
Raft solves this problem by taking a snapshot of the state machine at current
commit index and discarding all log entries up to it. MicroRaft implements
snapshotting by putting an upper bound on the number of log entries kept in Raft
log. It takes a snapshot of the state machine at every `N` commits and shrinks
the log. `N` is configurable via <a
href="https://github.com/MicroRaft/MicroRaft/blob/master/microraft/src/main/java/io/microraft/RaftConfig.java"
target="_blank">`RaftConfig.setCommitCountToTakeSnapshot()`</a>. A snapshot is
represented as a list of chunks, where a chunk can be any object provided by the
state machine. 

`StateMachine` contains 2 methods for the snapshotting functionality:
`takeSnapshot()` and `installSnapshot()`. Similar to what we did in the previous
part, we will extend `OperableAtomicRegister` to implement these methods. Since
our atomic register state machine consists of a single value, we just create a
single snapshot chunk object in `takeSnapshot()`. Dually, to install a snapshot,
we overwrite the value of the atomic register with the value present in the
received snapshot chunk object. MicroRaft guarantees that commit index of an
installed snapshot is always greater than the last commit index observed by the
state machine.  

<script src="https://gist.github.com/metanet/337f5bb9a8e82b637f9c66c46f6476be.js"></script>

You can also see this class in the <a
href="https://github.com/MicroRaft/MicroRaft/blob/master/microraft-tutorial/src/main/java/io/microraft/tutorial/atomicregister/SnapshotableAtomicRegister.java"
target="_blank">MicroRaft Github repository</a>.

We have the following test to demonstrate how snapshotting works in MicroRaft.
In `createRaftNode()`, we configure our Raft nodes to take a new snapshot at
every 100 commits. Similar to what we did in the previous test, we block the
communication between the leader and a follower, and fill up the leader's Raft
log with new commits until it takes a snapshot. When we allow the leader to
communicate with the follower again, the follower catches up with the leader by
transferring the snapshot.

<script src="https://gist.github.com/metanet/4565a0e995e64960f8f3248934d9c430.js"></script>

To run this test on your machine, try the following:

~~~~{.bash}
$ git clone https://github.com/MicroRaft/MicroRaft.git
$ cd MicroRaft && ./mvnw clean test -Dtest=io.microraft.tutorial.SnapshotInstallationTest -DfailIfNoTests=false -Ptutorial
~~~~

You can also see it in the <a
href="https://github.com/MicroRaft/MicroRaft/blob/master/microraft-tutorial/src/test/java/io/microraft/tutorial/SnapshotInstallationTest.java"
target="_blank">MicroRaft Github repository</a>.

-----

## 5. Extending the Raft group

Ok. We covered a lot of topics up until this part. There are only a few things
left to discuss. In this part, we will see how we can perform changes in Raft
group member lists.

### Changing the Raft group member list

MicroRaft supports membership changes in Raft groups via
`RaftNode.changeMembership()`. Let's first see the rules to realize membership
changes in Raft groups.

![](/img/info.png){: style="height:25px;width:25px"} Raft group membership
changes are appended to the internal Raft log as regular log entries and
committed similar to user-supplied operations. Therefore, Raft group membership
changes require the majority of the Raft group to be operational.

![](/img/info.png){: style="height:25px;width:25px"} When a membership change is
committed, its commit index is used to denote the new member list of the Raft
group and called *group members commit index*. Relatedly, when a membership
change is triggered via `RaftNode.changeMembership()`, the current *group
members commit index* must be provided.

![](/img/info.png){: style="height:25px;width:25px"} Last, MicroRaft allows one
membership change at a time in a Raft group and more complex changes must be
applied as a series of single changes.

Since we know the rules for member list changes now, let's see some code. In our
last test, we want to improve our 3-member Raft group's degree of fault
tolerance by adding 2 new members (majority quorum size of 3 = 2 -> majority
quorum size of 5 = 3).
 
<script src="https://gist.github.com/metanet/4f0ab94b78b369a1cc9ac58ef0e6f011.js"></script>

To run this test on your machine, try the following:

~~~~{.bash}
$ git clone https://github.com/MicroRaft/MicroRaft.git
$ cd MicroRaft && ./mvnw clean test -Dtest=io.microraft.tutorial.ChangeRaftGroupMemberListTest -DfailIfNoTests=false -Ptutorial
~~~~

You can also see it in the <a
href="https://github.com/MicroRaft/MicroRaft/blob/master/microraft-tutorial/src/test/java/io/microraft/tutorial/ChangeRaftGroupMemberListTest.java"
target="_blank">MicroRaft Github repository</a>.

In this test, we create a new Raft endpoint, `endpoint4`, and add it to the Raft
group in the following lines:

~~~~{.java}
RaftEndpoint endpoint4 = LocalRaftEndpoint.newEndpoint();
// group members commit index of the initial Raft group members is 0.
RaftGroupMembers newMemberList1 = leader.changeMembership(endpoint4, MembershipChangeMode.ADD_OR_PROMOTE_TO_FOLLOWER, 0).join().getResult();
System.out.println("New member list: " + newMemberList1.getMembers() + ", majority: " + newMemberList1.getMajorityQuorumSize()
                           + ", commit index: " + newMemberList1.getLogIndex());

// endpoint4 is now part of the member list. Let's start its Raft node
RaftNode raftNode4 = createRaftNode(endpoint4);
raftNode4.start();
~~~~

Please notice that we pass `0` for the third argument to
`RaftNode.changeMembership()`, because our Raft group operates with the initial
member list whose group members commit index is `0`. After this call returns,
`endpoint4` is part of the Raft group, so we start its Raft node as well.

Our `sysout` line here prints the following:

~~~~{.text}
New member list: [LocalRaftEndpoint{id=node1}, LocalRaftEndpoint{id=node2}, LocalRaftEndpoint{id=node3}, LocalRaftEndpoint{id=node4}], majority: 3, commit index: 3
~~~~

As you see, our Raft group has 4 members now, whose majority quorum size is 3.
Actually, running a 4-node Raft group has no advantage over running a 3-node
Raft group in terms of the degree of availability because both of them can
handle failure of only 1 Raft node. Since we want to achieve better
availability, we will add one more Raft node to our Raft group.

So we create another Raft endpoint, `endpoint5`, add it to the Raft group, and
start its Raft node. However, as the *group members commit index* parameter, we 
pass `newMemberList1.getLogIndex()` which is the log index `endpoint4` is added 
to the Raft group. Please note that we could also get the current Raft group
members commit index via `RaftNode.getCommittedMembers()`.

Our second `sysout` line prints the following:

~~~~{.text}
New member list: [LocalRaftEndpoint{id=node1}, LocalRaftEndpoint{id=node2}, LocalRaftEndpoint{id=node3}, LocalRaftEndpoint{id=node4}, LocalRaftEndpoint{id=node5}], majority: 3, commit index: 5
~~~~

Now we have 5 nodes in our Raft group with the majority quorum size of 3. It 
means that now our Raft group can tolerate failure of 2 Raft nodes and still
remain operational. Voila!

In this example, for the sake of simplicity, we add new Raft nodes to the Raft 
group with `MembershipChangeMode.ADD_OR_PROMOTE_TO_FOLLOWER`. With this mode,
the Raft node is directly added as a _follower_, and the majority quorum size 
of the Raft group increases to 3. However, in real life use cases, Raft nodes
can contain large state, and it may take some time until the new Raft node 
catches up with the leader. Because of this, increasing the majority quorum 
size may cause availability gaps if failures occur before the new Raft node
catches up. To prevent this, MicroRaft offers another membership change mode,
`MembershipChangeMode.ADD_LEARNER`, to add new Raft nodes. With this mode, the
new Raft node is added with the _learner_ role. Learner Raft nodes are excluded
in majority calculations, hence adding a new learner Raft node to the Raft 
group does not change the majority quorum size. Once the new learner Raft node
catches up, it can be promoted to the _follower_ role by triggering another 
membership change: `MembershipChangeMode.ADD_OR_PROMOTE_TO_FOLLOWER`.


-----

## What's next?

In the next section, we will see how MicroRaft deals with failures. Just [keep
calm and carry on](resiliency-and-fault-tolerance.md)!
