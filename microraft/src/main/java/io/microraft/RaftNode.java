/*
 * Original work Copyright (c) 2008-2020, Hazelcast, Inc.
 * Modified work Copyright (c) 2020, MicroRaft.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.microraft;

import java.time.Clock;
import java.util.Collection;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeoutException;

import javax.annotation.Nonnull;

import io.microraft.exception.CannotReplicateException;
import io.microraft.exception.IndeterminateStateException;
import io.microraft.exception.LaggingCommitIndexException;
import io.microraft.exception.MismatchingRaftGroupMembersCommitIndexException;
import io.microraft.exception.NotLeaderException;
import io.microraft.executor.RaftNodeExecutor;
import io.microraft.executor.impl.DefaultRaftNodeExecutor;
import io.microraft.impl.RaftNodeBuilderImpl;
import io.microraft.lifecycle.RaftNodeLifecycleAware;
import io.microraft.model.RaftModelFactory;
import io.microraft.model.impl.DefaultRaftModelFactory;
import io.microraft.model.message.RaftMessage;
import io.microraft.persistence.NopRaftStore;
import io.microraft.persistence.RaftStore;
import io.microraft.persistence.RestoredRaftState;
import io.microraft.report.RaftGroupMembers;
import io.microraft.report.RaftNodeReport;
import io.microraft.report.RaftNodeReportListener;
import io.microraft.report.RaftTerm;
import io.microraft.statemachine.StateMachine;
import io.microraft.transport.Transport;

/**
 * A Raft node runs the Raft consensus algorithm as a member of a Raft group.
 * <p>
 * Operations and queries passed to Raft nodes must be deterministic, i.e., they
 * must produce the same result independent of when or on which Raft node it is
 * being executed.
 * <p>
 * Raft nodes are identified by [group id, node id] pairs. Multiple Raft groups
 * can run in the same environment, distributed or even in a single JVM process,
 * and they can be discriminated from each other with unique group ids. A single
 * JVM process can run multiple Raft nodes that belong to different Raft groups
 * or even the same Raft group.
 * <p>
 * Before a new Raft group is created, its initial member list must be decided.
 * Then, a Raft node is created for each one of its members. When a new member
 * is added to an existing Raft group later on, its Raft node must be
 * initialized with the same initial member list as well.
 * <p>
 * Status of a Raft node is {@link RaftNodeStatus#INITIAL} on creation, and it
 * moves to {@link RaftNodeStatus#ACTIVE} and starts executing the Raft
 * consensus algorithm when {@link #start()} is called.
 * <p>
 * No further operations can be triggered on a Raft node after it is terminated
 * or leaves its Raft group, i.e., removed from the Raft group member list.
 * <p>
 * Raft nodes execute the Raft consensus algorithm with the Actor model. You can
 * read about the Actor Model at the following link:
 * https://en.wikipedia.org/wiki/Actor_model In this model, each Raft node runs
 * in a single-threaded manner. It uses a {@link RaftNodeExecutor} to
 * sequentially handle API calls and {@link RaftMessage} objects created via
 * {@link RaftModelFactory}.
 * <p>
 * The communication between Raft nodes are implemented with the message-passing
 * approach and abstracted away with the {@link Transport} interface.
 * <p>
 * Raft nodes use {@link StateMachine} to execute queries and committed
 * operations.
 * <p>
 * Last, Raft nodes use {@link RaftStore} to persist internal Raft state to
 * stable storage to be able to recover from crashes.
 *
 * @see RaftNodeBuilder
 * @see RaftEndpoint
 * @see RaftRole
 * @see RaftNodeStatus
 * @see RaftNodeExecutor
 * @see Transport
 * @see StateMachine
 * @see RaftStore
 * @see RaftNodeReportListener
 * @see RaftNodeLifecycleAware
 */
public interface RaftNode {

    /**
     * Returns a new builder to configure RaftNode that is going to be created.
     *
     * @return a new builder to configure RaftNode that is going to be created
     */
    static RaftNodeBuilder newBuilder() {
        return new RaftNodeBuilderImpl();
    }

    /**
     * Returns the unique id of the Raft group which this Raft node belongs to.
     *
     * @return the unique id of the Raft group which this Raft node belongs to
     */
    @Nonnull
    Object getGroupId();

    /**
     * Returns the local endpoint of this Raft node.
     *
     * @return the local endpoint of this Raft node
     */
    @Nonnull
    RaftEndpoint getLocalEndpoint();

    /**
     * Returns the config object this Raft node is initialized with.
     *
     * @return the config object this Raft node is initialized with
     */
    @Nonnull
    RaftConfig getConfig();

    /**
     * Returns the locally known term information.
     * <p>
     * Please note that the other Raft nodes in the Raft group may have already
     * switched to a higher term.
     *
     * @return the locally known term information
     */
    @Nonnull
    RaftTerm getTerm();

    /**
     * Returns the current status of this Raft node.
     *
     * @return the current status of this Raft node
     */
    @Nonnull
    RaftNodeStatus getStatus();

    /**
     * Returns the initial member list of the Raft group.
     *
     * @return the initial member list of the Raft group
     */
    @Nonnull
    RaftGroupMembers getInitialMembers();

    /**
     * Returns the last committed member list of the Raft group this Raft node
     * belongs to.
     * <p>
     * Please note that the returned member list is read from the local state and
     * can be different from the currently effective applied member list, if this
     * Raft node is part of the minority and there is an ongoing (appended but
     * not-yet-committed) membership change in the majority of the Raft group.
     * Similarly, it can be different from the current committed member list of the
     * Raft group, also if a new membership change is committed by the majority Raft
     * nodes but not learnt by this Raft node yet.
     *
     * @return the last committed member list of the Raft group
     */
    @Nonnull
    RaftGroupMembers getCommittedMembers();

    /**
     * Returns the currently effective member list of the Raft group this Raft node
     * belongs to.
     * <p>
     * Please note that the returned member list is read from the local state and
     * can be different from the committed member list, if there is an ongoing
     * (appended but not-yet committed) membership change in the Raft group.
     *
     * @return the currently effective member list of the Raft group
     */
    @Nonnull
    RaftGroupMembers getEffectiveMembers();

    /**
     * Triggers this Raft node to start executing the Raft consensus algorithm.
     * <p>
     * The returned future is completed with {@link IllegalStateException} if this
     * Raft node has already started.
     *
     * @return the future object to be notified after this Raft node starts
     */
    @Nonnull
    CompletableFuture<Ordered<Object>> start();

    /**
     * Forcefully sets the status of this Raft node to
     * {@link RaftNodeStatus#TERMINATED} and makes the Raft node stops executing the
     * Raft consensus algorithm.
     *
     * @return the future object to be notified after this Raft node terminates
     */
    @Nonnull
    CompletableFuture<Ordered<Object>> terminate();

    /**
     * Handles the given Raft message which can be either a Raft RPC request or a
     * response.
     * <p>
     * Silently ignores the given Raft message if this Raft node has already
     * terminated or left the Raft group.
     *
     * @param message
     *            the object sent by another Raft node of this Raft group
     *
     * @throws IllegalArgumentException
     *             if an unknown message is received.
     *
     * @see RaftMessage
     */
    void handle(@Nonnull RaftMessage message);

    /**
     * Replicates, commits, and executes the given operation via this Raft node. The
     * given operation is executed once it is committed in the Raft group, and the
     * returned future object is notified with its execution result.
     * <p>
     * Please note that the given operation must be deterministic.
     * <p>
     * The returned future is notified with an {@link Ordered} object that contains
     * the log index on which the given operation is committed and executed.
     * <p>
     * The returned future be can notified with {@link NotLeaderException},
     * {@link CannotReplicateException} or {@link IndeterminateStateException}.
     * Please see individual exception classes for more information.
     *
     * @param operation
     *            the operation to be replicated on the Raft group
     * @param <T>
     *            type of the result of the operation execution
     *
     * @return the future to be notified with the result of the operation execution,
     *         or the exception if the replication fails
     *
     * @see NotLeaderException
     * @see CannotReplicateException
     * @see IndeterminateStateException
     */
    @Nonnull
    <T> CompletableFuture<Ordered<T>> replicate(@Nonnull Object operation);

    /**
     * Executes the given query operation based on the given query policy.
     * <p>
     * The returned future is notified with an {@link Ordered} object that contains
     * the commit index on which the given query is executed.
     * <p>
     * If the caller is providing a query policy which is weaker than
     * {@link QueryPolicy#LINEARIZABLE}, it can also provide a minimum commit index.
     * Then, the local Raft node executes the given query only if its local commit
     * index is greater than or equal to the required commit index. If the local
     * commit index is smaller than the required commit index, then the returned
     * future is notified with {@link LaggingCommitIndexException} so that the
     * caller could retry its query on the same Raft node after some time or forward
     * it to another Raft node. This mechanism enables callers to execute queries on
     * Raft nodes without hitting the log replication quorum and preserve
     * monotonicity of query results. Please see the <i>Section: 6.4 Processing
     * read-only queries more efficiently</i> of the Raft dissertation for more
     * details.
     * <p>
     * The returned future can be notified with {@link NotLeaderException},
     * {@link CannotReplicateException} or {@link LaggingCommitIndexException}.
     * Please see individual exception classes for more information.
     *
     * @param operation
     *            the query operation to be executed
     * @param queryPolicy
     *            the query policy to decide how to execute the given query
     * @param minCommitIndex
     *            the minimum commit index that this Raft node has to have in order
     *            to execute the given query.
     * @param <T>
     *            type of the result of the query execution
     *
     * @return the future to be notified with the result of the query execution, or
     *         the exception if the query cannot be executed
     *
     * @see QueryPolicy
     * @see NotLeaderException
     * @see CannotReplicateException
     * @see LaggingCommitIndexException
     */
    @Nonnull
    <T> CompletableFuture<Ordered<T>> query(@Nonnull Object operation, @Nonnull QueryPolicy queryPolicy,
            long minCommitIndex);

    /**
     * Replicates and commits the given membership change to the Raft group, if the
     * given group members commit index is equal to the current group members commit
     * index in the local Raft state.
     * <p>
     * The initial group members commit index is 0. The current group members commit
     * index can be accessed via {@link #getCommittedMembers()}.
     * <p>
     * When the membership change process is completed successfully, the returned
     * future is notified with an {@link Ordered} object that contains the new
     * member list of the Raft group and the log index at which the given membership
     * change is committed.
     * <p>
     * The majority quorum size of the Raft group can increase or decrease by 1 if
     * the membership change updates the number of the voting members. Relatedly,
     * since adding a new voting member increases the majority quorum size, if there
     * are already failed Raft endpoints in the Raft group, it is recommended to
     * remove them first before adding a new voting member in order to avoid
     * availability issues.
     * <p>
     * The current leader Raft node can be removed from the Raft group member list
     * safely without requiring a leadership transfer. The leader Raft node commits
     * the membership change without its own vote, and the followers trigger a new
     * leader election round on the next periodic heartbeat tick. Please note that
     * in this scenario there is a short availability gap which is equal to the
     * leader heartbeat period duration.
     * <p>
     * A new Raft endpoint can be added to the Raft group as a
     * {@link RaftRole#LEARNER} via {@link MembershipChangeMode#ADD_LEARNER}. In
     * this case, the quorum size of the Raft group does not change. In addition, a
     * new Raft endpoint can be added to the Raft group as a
     * {@link RaftRole#FOLLOWER}, or an existing {@link RaftRole#LEARNER} Raft
     * endpoint can be promoted to {@link RaftRole#FOLLOWER} with
     * {@link MembershipChangeMode#ADD_OR_PROMOTE_TO_FOLLOWER}. In this case, the
     * quorum size of the Raft group is re-calculated based on the new number of
     * voting members. The leader Raft node does not check if the given Raft
     * endpoint's local Raft log is sufficiently up to date before this
     * re-calculation. Hence, it is the caller's responsibility to check the
     * promoted Raft endpoint's local Raft log in order to prevent availability gaps
     * if the quorum size will increase after the membership change. This check can
     * be done by getting {@link RaftNodeReport} from the leader Raft node and
     * comparing last log indices of the leader and the {@link RaftRole#LEARNER}
     * Raft endpoint.
     * <p>
     * If the given group members commit index is different than the current group
     * members commit index in the local Raft state, then the returned future is
     * notified with {@link MismatchingRaftGroupMembersCommitIndexException}.
     * <p>
     * If the Raft group contains a single voting member and that member is
     * attempted to be removed, then the returned future is notified with
     * {@link IllegalStateException}.
     * <p>
     * If the given Raft endpoint is already in the committed Raft group member
     * list, or it is being added as a {@link RaftRole#LEARNER} while there are
     * {@link RaftGroupMembers#MAX_LEARNER_COUNT} {@link RaftRole#LEARNER}s in the
     * Raft group member list, then the returned future is notified with
     * {@link IllegalArgumentException}.
     * <p>
     * The returned future can be notified with {@link NotLeaderException},
     * {@link CannotReplicateException} or {@link IndeterminateStateException}.
     * Please see individual exception classes for more information.
     *
     * @param endpoint
     *            the endpoint to add to or remove from the Raft group
     * @param mode
     *            the type of the membership change
     * @param expectedGroupMembersCommitIndex
     *            the expected members commit index
     *
     * @return the future to be notified with the new member list of the Raft group
     *         if the membership change is successful, or the exception if the
     *         membership change failed
     *
     * @see #replicate(Object)
     * @see MismatchingRaftGroupMembersCommitIndexException
     * @see NotLeaderException
     * @see CannotReplicateException
     * @see IndeterminateStateException
     * @see IllegalArgumentException
     * @see IllegalStateException
     */
    @Nonnull
    CompletableFuture<Ordered<RaftGroupMembers>> changeMembership(@Nonnull RaftEndpoint endpoint,
            @Nonnull MembershipChangeMode mode, long expectedGroupMembersCommitIndex);

    /**
     * Transfers the leadership role to the given endpoint, if this Raft node is the
     * current Raft group leader with the {@link RaftNodeStatus#ACTIVE} status and
     * the given endpoint is in the committed member list of the Raft group.
     * <p>
     * The leadership transfer process is considered to be completed when this Raft
     * node moves to the follower role. There is no strict guarantee that the given
     * endpoint will be the new leader in the new term. However, it is very unlikely
     * that another endpoint will become the new leader.
     * <p>
     * The returned future is notified with an {@link Ordered} object that contains
     * the commit index on which this Raft node turns into a follower.
     * <p>
     * This Raft node does not replicate any new operation until the leadership
     * transfer process is completed and new {@link #replicate(Object)} calls fail
     * with {@link CannotReplicateException}.
     * <p>
     * The returned future can be notified with {@link NotLeaderException} if this
     * Raft node is not leader, {@link IllegalStateException} if the Raft node
     * status is not {@link RaftNodeStatus#ACTIVE}, {@link IllegalArgumentException}
     * if the given endpoint is not a voting member in the committed Raft group
     * member list, and {@link TimeoutException} if the leadership transfer process
     * has timed out w.r.t {@link RaftConfig#getLeaderHeartbeatTimeoutSecs()}.
     *
     * @param endpoint
     *            the Raft endpoint to which the leadership will be transferred
     *
     * @return the future to be notified when the leadership transfer is done, or
     *         with the execution if the leader transfer could not be done.
     *
     * @see CannotReplicateException
     * @see NotLeaderException
     */
    @Nonnull
    CompletableFuture<Ordered<Object>> transferLeadership(@Nonnull RaftEndpoint endpoint);

    /**
     * Returns a report object that contains information about this Raft node's
     * local state related to the execution of the Raft consensus algorithm.
     *
     * @return a report object that contains information about this Raft node's
     *         local state
     */
    @Nonnull
    CompletableFuture<Ordered<RaftNodeReport>> getReport();

    /**
     * Takes a new snapshot at the local RaftNode at the current commit index. If a
     * snapshot is already taken at the current commit index, calling this method is
     * a no-op.
     *
     * @return a report object that contains information about this Raft node's
     *         local state if a new snapshot is taken, otherwise null
     */
    @Nonnull
    CompletableFuture<Ordered<RaftNodeReport>> takeSnapshot();

    /**
     * The builder interface for configuring and creating Raft node instances.
     */
    interface RaftNodeBuilder {

        /**
         * Sets the unique ID of the Raft group that this Raft node belongs to.
         *
         * @param groupId
         *            the group id to create the Raft node with
         *
         * @return the builder object for fluent calls
         */
        @Nonnull
        RaftNodeBuilder setGroupId(@Nonnull Object groupId);

        /**
         * Sets the endpoint of the Raft node being created.
         * <p>
         * This method must be used along with one of the
         * {@link #setInitialGroupMembers(Collection)} overloads when either a new Raft
         * group is bootstrapping for the first time or a new Raft node is being added
         * to a running Raft group.
         *
         * @param localEndpoint
         *            the Raft endpoint to create the Raft node with
         *
         * @return the builder object for fluent calls
         */
        @Nonnull
        RaftNodeBuilder setLocalEndpoint(@Nonnull RaftEndpoint localEndpoint);

        /**
         * Sets the initial member list of the Raft group that the Raft node belongs to.
         * All members are voting members.
         * <p>
         * On bootstrapping a new Raft group, the initial member list must be decided
         * externally and provided to all Raft nodes. In addition, if a new Raft node is
         * going to be added to a running Raft group, the same initial Raft group member
         * list must be provided to the new Raft node.
         * <p>
         * This method must be called along with {@link #setLocalEndpoint(RaftEndpoint)}
         * when either a new Raft group is bootstrapping for the first time or a new
         * Raft node is being added to a running Raft group.
         *
         * @param initialGroupMembers
         *            the initial group members of the Raft group which the Raft node
         *            belongs to
         *
         * @return the builder object for fluent calls
         */
        @Nonnull
        RaftNodeBuilder setInitialGroupMembers(@Nonnull Collection<RaftEndpoint> initialGroupMembers);

        /**
         * Sets the initial member list of the Raft group that the Raft node belongs to.
         * The initial member list contains voting and non-voting members together.
         * There must be at least one voting member in the initial Raft group member
         * list.
         * <p>
         * On bootstrapping a new Raft group, the initial member list must be decided
         * externally and provided to all Raft nodes. In addition, if a new Raft node is
         * going to be added to a running Raft group, the same initial Raft group member
         * list must be provided to the new Raft node.
         * <p>
         * This method must be called along with {@link #setLocalEndpoint(RaftEndpoint)}
         * when either a new Raft group is bootstrapping for the first time or a new
         * Raft node is being added to a running Raft group.
         *
         * @param initialGroupMembers
         *            the initial group members of the Raft group which the Raft node
         *            belongs to
         * @param initialVotingGroupMembers
         *            the list of voting members in the initial Raft group member list
         *
         * @return the builder object for fluent calls
         */
        @Nonnull
        RaftNodeBuilder setInitialGroupMembers(@Nonnull Collection<RaftEndpoint> initialGroupMembers,
                @Nonnull Collection<RaftEndpoint> initialVotingGroupMembers);

        /**
         * Sets the {@link RestoredRaftState} to be used for creating the Raft node
         * instance.
         * <p>
         * {@link RestoredRaftState} is used in crash-recover scenarios to recover a
         * Raft node after a crash or a restart. Namely, when a
         * {@link RestoredRaftState} is provided, the created Raft node initializes its
         * internal Raft state from this object and continues its operation as if it has
         * not crashed.
         * <p>
         * {@link #setLocalEndpoint(RaftEndpoint)} and
         * {@link #setInitialGroupMembers(Collection)} must not be called when a
         * {@link RestoredRaftState} object is provided via this method.
         *
         * @param restoredState
         *            the restored Raft state which will be used while creating the Raft
         *            node
         *
         * @return the builder object for fluent calls
         *
         * @see RestoredRaftState
         */
        @Nonnull
        RaftNodeBuilder setRestoredState(@Nonnull RestoredRaftState restoredState);

        /**
         * Sets the Raft config.
         * <p>
         * If not set, {@link RaftConfig#DEFAULT_RAFT_CONFIG} is used.
         *
         * @param config
         *            the config object to create the Raft node with
         *
         * @return the builder object for fluent calls
         *
         * @see RaftConfig
         */
        @Nonnull
        RaftNodeBuilder setConfig(@Nonnull RaftConfig config);

        /**
         * Sets the Raft node executor object to be used for running submitted and
         * scheduled tasks.
         * <p>
         * If not set, {@link DefaultRaftNodeExecutor} is used.
         *
         * @param executor
         *            the Raft node executor object to be used for running submitted and
         *            scheduled tasks
         *
         * @return the builder object for fluent calls
         *
         * @see RaftNodeExecutor
         * @see DefaultRaftNodeExecutor
         */
        @Nonnull
        RaftNodeBuilder setExecutor(@Nonnull RaftNodeExecutor executor);

        /**
         * Sets the transport object to be used for communicating with other Raft nodes.
         *
         * @param transport
         *            the transport object to be used for communicating with other Raft
         *            nodes
         *
         * @return the builder object for fluent calls
         *
         * @see Transport
         */
        @Nonnull
        RaftNodeBuilder setTransport(@Nonnull Transport transport);

        /**
         * Sets the state machine object to be used for execution of queries and
         * committed operations.
         *
         * @param stateMachine
         *            the state machine object which will be used for execution of
         *            queries and committed operations
         *
         * @return the builder object for fluent calls
         *
         * @see StateMachine
         */
        @Nonnull
        RaftNodeBuilder setStateMachine(@Nonnull StateMachine stateMachine);

        /**
         * Sets the Raft state object to be used for persisting internal Raft state to
         * stable storage.
         * <p>
         * If not set, {@link NopRaftStore} is used which keeps the internal Raft state
         * in memory and disables crash-recover scenarios.
         *
         * @param store
         *            the Raft state object to be used for persisting internal Raft
         *            state to stable storage.
         *
         * @return the builder object for fluent calls
         *
         * @see RaftStore
         */
        @Nonnull
        RaftNodeBuilder setStore(@Nonnull RaftStore store);

        /**
         * Sets the Raft model factory object to be used for creating Raft model
         * objects.
         * <p>
         * If not set, {@link DefaultRaftModelFactory} is used.
         *
         * @param modelFactory
         *            the factory object to be used for creating Raft model objects
         *
         * @return the builder object for fluent calls
         *
         * @see RaftModelFactory
         * @see DefaultRaftModelFactory
         */
        @Nonnull
        RaftNodeBuilder setModelFactory(@Nonnull RaftModelFactory modelFactory);

        /**
         * Sets the Raft node report listener object to be notified about events related
         * to the execution of the Raft consensus algorithm.
         *
         * @param listener
         *            the Raft node report listener object to be notified about events
         *            related to the execution of the Raft consensus algorithm
         *
         * @return the builder object for fluent calls
         *
         * @see RaftNodeReportListener
         */
        RaftNodeBuilder setRaftNodeReportListener(@Nonnull RaftNodeReportListener listener);

        /**
         * Sets the Random instance used in parts of the Raft algorithm.
         *
         * @param random
         *            the Random instance to be used by this RaftNode.
         *
         * @return the builder object for fluent calls
         */
        RaftNodeBuilder setRandom(Random random);

        /**
         * Sets the Clock instance used by parts of the Raft algorithm.
         *
         * @param clock
         *            the Clock instance to be used by this RaftNode.
         *
         * @return the builder object for fluent calls
         */
        RaftNodeBuilder setClock(Clock clock);

        /**
         * Builds and returns the RaftNode instance with the given settings.
         *
         * @return the built RaftNode instance.
         */
        @Nonnull
        RaftNode build();

    }

}
