## Implementing the log

_November 12, 2022 | Ensar Basri Kahveci_

This article is the second in _the ins and outs of MicroRaft_ series. Here we
dissect the log.

MicroRaft implements the log with 2 components: [`RaftLog`](https://github.com/MicroRaft/MicroRaft/blob/v0.3/microraft/src/main/java/io/microraft/impl/log/RaftLog.java) and [`RaftStore`](https://github.com/MicroRaft/MicroRaft/blob/v0.3/microraft/src/main/java/io/microraft/persistence/RaftStore.java).
RaftLog is an internal component that keeps log entries in memory. On the other
hand, RaftStore is a public API. It is implemented by users. RaftNode does not
interact with RaftStore directly. RaftNode manipulates the log via RaftLog, and
RaftLog calls RaftStore to reflect changes to disk.

![Figure 1](https://microraft.io/img/blog3-fig1.png)

RaftLog is implemented as a fixed-size ring-buffer. As shown in Figure 1, it
consists of 3 sections from head to tail: _snapshotted_, _committed_, and _in
progress_.

## Appending new log entries

New log entries are appended to the _in progress_ section at the tail.  Its size
is specified by [`RaftConfig.getMaxPendingLogEntryCount()`](https://github.com/MicroRaft/MicroRaft/blob/v0.3/microraft/src/main/java/io/microraft/RaftConfig.java#L213). Log entries reside in
this section until they are committed or truncated. When a client calls
[`RaftNode.replicate()`](https://github.com/MicroRaft/MicroRaft/blob/v0.3/microraft/src/main/java/io/microraft/RaftNode.java#L258), if the _in progress_ section has no empty slots, the
client gets a [`CannotReplicateException`](https://github.com/MicroRaft/MicroRaft/blob/v0.3/microraft/src/main/java/io/microraft/exception/CannotReplicateException.java). This exception means that the
operation is not appended to the log because there are too many in progress log
entries at the moment. A client can retry its operation later upon receiving
this exception. By this way, we prevent OOME on a leader Raft node if it cannot
keep up with the request rate. The size of the _in progress_ section should be
decided by taking the degree of the concurrency of the clients into account.

Log entries in the _in progress_ section can be truncated before they are
committed in some failure scenarios. Consider the scenario where a leader
appends a new log entry to an index, but disconnects from the rest of the Raft
group before it replicates the new log entry to the other Raft nodes. After the
_leader heartbeat timeout_ elapses, the other Raft nodes can elect a new leader
and the new leader can append a new log entry to the same log index. Once the
previous leader reconnects to the other Raft nodes, it notices that a new
leader has appended a new log entry to the same log index. In this case, the
client of the initial log entry gets a [`NotLeaderException`](https://github.com/MicroRaft/MicroRaft/blob/v0.3/microraft/src/main/java/io/microraft/exception/NotLeaderException.java). This exception
means that the client's operation is not committed and  the client can retry its
operation on the new leader safely.

## Committing log entries

The commit index moves from head to tail. Once a log entry is committed, it is
moved to the _committed_ section. A new snapshot is taken from [`StateMachine`](https://github.com/MicroRaft/MicroRaft/blob/v0.3/microraft/src/main/java/io/microraft/statemachine/StateMachine.java)
when the _committed_ section is full. The size of the _committed_ section is
specified by [`RaftConfig.getCommitCountToTakeSnapshot()`](https://github.com/MicroRaft/MicroRaft/blob/v0.3/microraft/src/main/java/io/microraft/RaftConfig.java#L231). Since all Raft nodes
are created with the same config, they take snapshots at the same log indices.
For instance, when `RaftConfig.getCommitCountToTakeSnapshot()` is 10000, Raft
nodes take snapshots at log indices: 10000, 20000, 30000, and so on. This
deterministic behaviour enables a snapshotting optimization which we will cover
later.

## Truncating committed log entries after snapshots

Recall that log entries are committed once they are replicated to the majority.
When a leader Raft node decides to take a snapshot, there can be some Raft nodes
that have not received the recently committed log entries yet. If the leader
immediately deletes all log entries preceding the snapshot, it may need to send
a snapshot to those Raft nodes instead of the recent log entries. MicroRaft
applies a simple heuristic to prevent this situation. Upon taking a new
snapshot, the leader Raft node checks the smallest match index of the minority
followers. If the difference between the leader's commit index and the smallest
follower match index is less than `0.1 *
RaftConfig.getCommitCountToTakeSnapshot()`, the leader moves all log entries
after that match index to the _snapshotted_ section. If there is no such
follower, i.e., all minority followers are far behind, the leader truncates all
log entries preceding the snapshotted log index.

Followers apply the same heuristic with a slight difference. Since followers do
not know each other's match indices, when a follower Raft node takes a snapshot,
it moves the `0.1 * RaftConfig.getCommitCountToTakeSnapshot()` log entries
preceding the snapshot index to the _snapshotted_ section. This behaviour is
useful in case the current leader crashes and a follower becomes the new leader
just after it takes a snapshot.

Assume that `RaftConfig.getCommitCountToTakeSnapshot()` is 1000 and the current
commit index is 5000. If there is any follower whose match index is greater than
or equal to 4500 when the leader takes a snapshot, the leader moves the log
entries after that match index into the _snapshotted_ area. Otherwise, the
leader truncates all log entries preceding 5000. When a follower takes a
snapshot, it moves the log entries between 4500 and 5000 into the _snapshotted_
area.

## Amortizing the cost of disk writes

[RaftStore](https://github.com/MicroRaft/MicroRaft/blob/v0.3/microraft/src/main/java/io/microraft/persistence/RaftStore.java) is designed to amortize the cost of disk writes. RaftStore has 2
methods to reflect changes in RaftLog to disk: `RaftStore.persistLogEntry()` and
`RaftStore.truncateLogEntriesFrom()`. Both of these methods are called before a log entry
is committed. RaftStore implementations are allowed to buffer disk writes caused
by these methods instead of immediately flushing them. There is also a third
method to flush all buffered disk writes: `RaftStore.flush()`. When this method
is called by Raft node, the RaftStore implementation must guarantee the
durability of all buffered writes on the disk.

Since flushing typically involves costly `fsync` calls on the kernel level,
MicroRaft amortizes the cost of disk writes by performing multiple
`RaftStore.persistLogEntry()` calls before a `RaftStore.flush()` call.

## Wrap up

In this article, we explored the details of MicroRaft's log implementation.
Next, we will investigate how MicroRaft realizes log replication.
