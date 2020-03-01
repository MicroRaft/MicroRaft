package com.hazelcast.raft;

import com.hazelcast.raft.impl.log.SnapshotEntry;

import java.util.concurrent.TimeUnit;

/**
 * The abstraction used by {@link RaftNode} instances as the integration point
 * between the Raft consensus algorithm implementation and the the underlying
 * platform which is responsible for task/operation execution, scheduling,
 * networking and state machine implementation.
 * <p>
 * A Raft node runs in a single-threaded manner. Even if multiple threads are
 * utilized by the underlying platform, given tasks must be executed
 * in a single threaded manner and the happens-before relationship must be
 * maintained between tasks of a single Raft node.
 *
 * @author mdogan
 * @author metanet
 * @see RaftNode
 */
public interface RaftIntegration {

    /**
     * Returns true if underlying platform is ready to operate,
     * false otherwise.
     * <p>
     * This method is used inside the {@link RaftNode#start()} method. When
     * a Raft node is created, the underlying platform might be still doing
     * some initialization work. In this case, the Raft node will not start
     * executing the Raft consensus algorithm and {@link RaftNode#start()}
     * will schedule itself for a future time.
     *
     * @return true if underlying platform is ready to operate, false otherwise
     */
    boolean isReady();

    /**
     * Called when a Raft node leaves its Raft group or the Raft group is
     * terminated. With this call, the underlying platform can do its shutdown
     * process and cancel scheduled tasks of the Raft node. It can also
     * silently ignore future {@link #execute(Runnable)} and
     * {@link #schedule(Runnable, long, TimeUnit)} calls.
     */
    void close();

    /**
     * Executes the given task on the underlying platform.
     * <p>
     * Please note that all tasks of a single Raft node must be executed
     * in a single-threaded manner and the happens-before relationship
     * must be maintained between given tasks of the Raft node.
     * <p>
     * The underlying platform is free to execute the given task on the
     * caller thread if it fits to the defined guarantees.
     *
     * @param task the task to be executed.
     */
    void execute(Runnable task);

    /**
     * Schedules the task on the underlying platform to be executed after
     * the given delay.
     * <p>
     * Please note that even though the scheduling can be offloaded to another
     * thread, the given task must be executed in a single-threaded manner and
     * the happens-before relationship must be maintained between given tasks
     * of the Raft node.
     *
     * @param task     the task to be executed in future
     * @param delay    the time from now to delay execution
     * @param timeUnit the time unit of the delay
     */
    void schedule(Runnable task, long delay, TimeUnit timeUnit);

    /**
     * Returns true if the given endpoint is supposedly reachable by the time
     * this method is called, false otherwise.
     *
     * @param endpoint endpoint
     * @return true if given endpoint is reachable, false otherwise
     */
    boolean isReachable(RaftEndpoint endpoint);

    /**
     * Attempts to send the given {@link RaftMsg} object to the given endpoint
     * in a best-effort manner. This method should not block the caller.
     * <p>
     * If the given {@link RaftMsg} object is not sent to the given endpoint
     * for sure, this method returns {@code false}. If an attempt is made,
     * this method returns {@code true}.
     *
     * @param msg    the {@link RaftMsg} object to be sent
     * @param target the target endpoint to send the Raft message
     * @return false if the message is not sent definitely,
     * true if a attempt is made for sending the message
     */
    boolean send(RaftMsg msg, RaftEndpoint target);

    /**
     * Executes the given operation on the state machine and returns
     * result of the operation.
     * <p>
     * Please note that the given operation must be deterministic and return
     * the same result on all members of the Raft group.
     *
     * @param operation   custom operation to execute
     * @param commitIndex Raft log index the given operation is committed at
     * @return result of the executed operation
     */
    Object runOperation(Object operation, long commitIndex);

    /**
     * Takes a snapshot of the state machine for the given commit index
     * which is the current commit index.
     *
     * @param commitIndex commit index on which a snapshot is taken
     * @return snapshot object to put into the {@link SnapshotEntry}
     */
    Object takeSnapshot(long commitIndex);

    /**
     * Restores the given snapshot object for the given commit index, which is
     * same with the commit index on which the snapshot is taken.
     *
     * @param object      snapshot object given by {@link #takeSnapshot(long)}
     * @param commitIndex commit index of the snapshot
     */
    void restoreSnapshot(Object object, long commitIndex);

    /**
     * Returns the operation to be appended after a new leader is elected.
     * If null is returned, no entry will be appended.
     * <p>
     * See <a href="https://groups.google.com/forum/#!msg/raft-dev/t4xj6dJTP6E/d2D9LrWRza8J">
     * <i>Bug in single-server membership changes</i></a> post by Diego Ongaro
     * for more information.
     * <p>
     * At least a NOP object is recommended to be returned on production
     * because it has almost zero overhead. Null can be returned for testing
     * purposes.
     */
    Object getOperationToAppendAfterLeaderElection();

    /**
     * Called when term, role, status, known leader, or member list
     * of the Raft node changes.
     *
     * @param summary summary of the new state of the Raft node
     */
    void onRaftStateChange(RaftStateSummary summary);

}
