package com.hazelcast.raft;

import com.hazelcast.raft.exception.CannotReplicateException;
import com.hazelcast.raft.exception.LeaderDemotedException;
import com.hazelcast.raft.exception.NotLeaderException;
import com.hazelcast.raft.exception.OperationResultUnknownException;
import com.hazelcast.raft.exception.RaftGroupTerminatedException;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.concurrent.TimeoutException;

/**
 * A Raft node runs the Raft consensus algorithm as a member of a Raft group.
 * <p>
 * Raft nodes run in a non-blocking manner and no method in this interface is
 * allowed to block callers.
 * <p>
 * Operations that are committed to Raft groups and queries that run on the
 * underlying state machine must be deterministic.
 * <p>
 * Multiple Raft groups can run in a single distributed environment or even
 * in a single JVM, and they can be discriminated from each other via unique
 * group ids. A single JVM instance can run multiple Raft nodes that belong to
 * the different Raft groups or even the same Raft group. The same Raft group id
 * must be provided to all Raft nodes of the Raft group.
 * <p>
 * Before a new Raft group is created, its initial member list must be decided.
 * Then, a Raft node is created for each one of its members. When a new member
 * is added to an existing Raft group, it must be initialized with this initial
 * member list as well.
 * <p>
 * When a new Raft node is created, its status is
 * {@link RaftNodeStatus#INITIAL} and then it switches to
 * {@link RaftNodeStatus#ACTIVE} when {@link #start()} is called. After that,
 * it starts executing the Raft consensus algorithm by triggering a leader
 * election.
 * <p>
 * No further operations can be triggered on a Raft node after it
 * leaves its Raft group or the Raft group is terminated.
 * <p>
 * A Raft node uses a {@link RaftIntegration} object to handle async task
 * execution, threading, networking, operation execution and state machine
 * concerns.
 *
 * @author mdogan
 * @author metanet
 * @see RaftEndpoint
 * @see RaftGroupMembers
 * @see RaftRole
 * @see RaftNodeStatus
 * @see RaftMsg
 * @see RaftIntegration
 */
public interface RaftNode {

    /**
     * Returns unique id of the Raft group which this Raft node belongs to.
     */
    @Nonnull
    Object getGroupId();

    /**
     * Returns the local endpoint of this Raft node.
     */
    @Nonnull
    RaftEndpoint getLocalEndpoint();

    /**
     * Returns the locally known leader endpoint.
     * <p>
     * The leader endpoint known by this Raft node can be a stale information.
     */
    @Nullable
    RaftEndpoint getLeaderEndpoint();

    /**
     * Returns the current status of this Raft node.
     */
    @Nonnull
    RaftNodeStatus getStatus();

    /**
     * Returns the initial member list of the Raft group
     * this Raft node belongs to.
     */
    @Nonnull
    RaftGroupMembers getInitialMembers();

    /**
     * Returns the last committed member list of the Raft group this Raft node
     * belongs to. Please note that the returned member list is read from
     * the local state and can be different from the currently effective
     * applied member list, if there is an ongoing (appended but not-yet
     * committed) membership change in the group. It can be different from
     * the current committed member list of the Raft group, also if a new
     * membership change is committed by other Raft nodes of the group but
     * not learnt by this Raft node yet.
     */
    @Nonnull
    RaftGroupMembers getCommittedMembers();

    /**
     * Returns the currently effective member list of the Raft group this Raft
     * node belongs to. Please note that the returned member list is read from
     * the local state and can be different from the committed member list,
     * if there is an ongoing (appended but not-yet committed) membership
     * change in the Raft group.
     */
    @Nonnull
    RaftGroupMembers getEffectiveMembers();

    /**
     * Triggers this Raft node to start executing the Raft consensus algorithm.
     * If the underlying {@link RaftIntegration} is not ready yet, i.e.,
     * the start procedure is internally scheduled to a future time.
     * <p>
     * This method has no effect if the Raft node status is already
     * {@link RaftNodeStatus#ACTIVE}.
     *
     * @throws IllegalStateException if the local status is not
     *                               {@link RaftNodeStatus#INITIAL}
     */
    void start();

    /**
     * Forcefully sets the status of this Raft node to
     * {@link RaftNodeStatus#TERMINATED}. The Raft group termination process is
     * completed once it is committed to the majority and Raft nodes will stop
     * running the Raft consensus algorithm afterwards. If there is any Raft
     * node that has not learn this commit in the mean time, it will not be
     * able to terminate itself. In this case, this method can be used to
     * terminate that hanging Raft node.
     * <p>
     * One can use this method to terminate minority Raft nodes also when
     * majority Raft nodes have failed and will not come back.
     */
    void forceTerminate();

    /**
     * Handles the given Raft message which can be either a Raft RPC request
     * or a response.
     * <p>
     * Ignores the given Raft message if this Raft node has already terminated
     * or stepped down.
     *
     * @param msg the object sent by another Raft node of this Raft group
     * @throws IllegalArgumentException if an unknown message is received.
     * @see RaftMsg
     */
    void handle(@Nonnull RaftMsg msg);

    /**
     * Replicates, commits, and executes the given operation via this Raft
     * node. The given operation is executed once it is committed in the Raft
     * group, and the returned {@link Future} object is notified with its
     * execution result.
     * <p>
     * Please note that the given operation must be deterministic.
     * <p>
     * The returned future can be notified with {@link NotLeaderException},
     * {@link CannotReplicateException}, {@link RaftGroupTerminatedException},
     * {@link LeaderDemotedException}, {@link OperationResultUnknownException}.
     * See individual exception classes for more details.
     *
     * @param operation operation to replicate
     * @return future to get notified about result of the operation
     */
    @Nonnull
    CompletableFuture<Object> replicate(@Nullable Object operation);

    /**
     * Executes the given query operation based on the given query policy.
     * <p>
     * The returned future can be notified with {@link NotLeaderException},
     * {@link CannotReplicateException}, {@link RaftGroupTerminatedException},
     * {@link LeaderDemotedException}.
     * See individual exception classes for more details.
     *
     * @param operation   query operation
     * @param queryPolicy query policy to decide how to execute the query
     * @return future to get notified about result of the query
     * @see QueryPolicy
     */
    @Nonnull
    CompletableFuture<Object> query(@Nullable Object operation, @Nonnull QueryPolicy queryPolicy);

    /**
     * Replicates and commits the given membership change to the Raft group,
     * if the given members commit index is equal to the current members
     * commit index in the local Raft state. The initial group members commit
     * index is 0. The current group members commit index can be accessed via
     * {@link #getCommittedMembers()}.
     * <p>
     * The returned future is notified with {@link CannotReplicateException},
     * {@link RaftGroupTerminatedException}, {@link NotLeaderException},
     * {@link LeaderDemotedException}, {@link OperationResultUnknownException}.
     * See individual exception classes for more details.
     * <p>
     * Majority number of the Raft group can increase or decrease by 1 after
     * the requested membership change is committed.</p>
     *
     * @param endpoint                        endpoint to add or remove
     * @param mode                            type of membership change
     * @param expectedGroupMembersCommitIndex expected members commit index
     * @return future to get notified about result of the membership change
     * @see #replicate(Object)
     */
    @Nonnull
    CompletableFuture<Long> changeMembership(@Nonnull RaftEndpoint endpoint, @Nonnull MembershipChangeMode mode,
                                             long expectedGroupMembersCommitIndex);

    /**
     * Transfers group leadership to the given endpoint, if this Raft node
     * is the current Raft group leader with ACTIVE status and the given
     * endpoint is a group member.
     * <p>
     * Leadership transfer is considered to be completed when this Raft node
     * moves to a newer term. There is no strict guarantee that the given
     * endpoint will be the new leader in the new term. However, it is very
     * unlikely that another endpoint will become the new leader.
     * <p>
     * This Raft node will not replicate any new operation during a leadership
     * transfer and new calls to the {@link #replicate(Object)} method will
     * fail with {@link CannotReplicateException}.
     * <p>
     * The returned future is notified with {@link CannotReplicateException},
     * {@link NotLeaderException}, {@link LeaderDemotedException},
     * {@link IllegalArgumentException} if the given endpoint is not a
     * committed group member and {@link TimeoutException}
     * if the leadership transfer has timed out.
     * See individual exception classes for more details.
     *
     * @return future to get notified about result of the leadership transfer
     */
    @Nonnull
    CompletableFuture<Object> transferLeadership(@Nonnull RaftEndpoint endpoint);

    /**
     * Replicates and commits an internal operation to the Raft group to terminate
     * the Raft group gracefully. After this commit, no new operations can be
     * committed. The Raft group termination process is committed like
     * a regular operation, so all rules that are valid for
     * {@link #replicate(Object)} apply.
     * <p>
     * After a Raft group is terminated, its Raft nodes stop running the Raft
     * consensus algorithm.
     * <p>
     * The returned future is notified with {@link CannotReplicateException},
     * {@link NotLeaderException}, {@link LeaderDemotedException},
     * {@link IllegalArgumentException} if the given endpoint is not a
     * committed group member and {@link TimeoutException}
     * if the leadership transfer has timed out.
     * See individual exception classes for more details.
     *
     * @return future to get notified about result of the group termination process
     */
    @Nonnull
    CompletableFuture<Object> terminateGroup();

}
