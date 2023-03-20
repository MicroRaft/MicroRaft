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

package io.microraft.impl.task;

import static io.microraft.RaftNodeStatus.INITIAL;
import static io.microraft.RaftNodeStatus.isTerminal;
import static io.microraft.RaftRole.LEADER;

import java.time.Duration;
import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.microraft.QueryPolicy;
import io.microraft.RaftNodeStatus;
import io.microraft.exception.RaftException;
import io.microraft.impl.RaftNodeImpl;
import io.microraft.impl.state.QueryState;
import io.microraft.impl.state.RaftState;
import io.microraft.impl.state.QueryState.QueryContainer;
import io.microraft.impl.util.OrderedFuture;
import io.microraft.model.groupop.RaftGroupOp;
import io.microraft.statemachine.StateMachine;

/**
 * Scheduled by {@link RaftNodeImpl#query(Object, QueryPolicy, long)} to perform
 * a query on the {@link StateMachine}.
 *
 * @see QueryPolicy
 */
public final class QueryTask implements Runnable {

    private static final Logger LOGGER = LoggerFactory.getLogger(QueryTask.class);

    private final RaftNodeImpl raftNode;
    private final RaftState state;
    private final Object operation;
    private final QueryPolicy queryPolicy;
    private final long minCommitIndex;
    private final Optional<Duration> timeout;
    private final OrderedFuture future;

    public QueryTask(RaftNodeImpl raftNode, Object operation, QueryPolicy policy, long minCommitIndex,
            Optional<Duration> timeout, OrderedFuture future) {
        this.raftNode = raftNode;
        this.state = raftNode.state();
        this.operation = operation;
        this.queryPolicy = policy;
        this.minCommitIndex = minCommitIndex;
        this.timeout = timeout;
        this.future = future;
    }

    @Override
    public void run() {
        try {
            if (!verifyOperation() || !verifyRaftNodeStatus()) {
                return;
            }

            switch (queryPolicy) {
                case EVENTUAL_CONSISTENCY :
                    queryWithEventualConsistency();
                    break;
                case BOUNDED_STALENESS :
                    queryWithBoundedStaleness();
                    break;
                case LEADER_LEASE :
                    queryWithLeaderLease();
                    break;
                case LINEARIZABLE :
                    queryWithLinearizability();
                    break;
                default :
                    future.fail(new IllegalArgumentException("Invalid query policy: " + queryPolicy));
            }
        } catch (Throwable t) {
            LOGGER.error(raftNode.localEndpointStr() + queryPolicy + " query failed", t);
            future.fail(new RaftException("Internal failure", raftNode.getLeaderEndpoint(), t));
        }
    }

    private void queryWithLeaderLease() {
        if (raftNode.demoteToFollowerIfQuorumHeartbeatTimeoutElapsed()) {
            future.fail(raftNode.newNotLeaderException());
        } else {
            long commitIndex = state.commitIndex();
            if (commitIndex < minCommitIndex) {
                future.fail(raftNode.newLaggingCommitIndexException(minCommitIndex));
                return;
            }

            queryWithEventualConsistency();
        }
    }

    private void queryWithEventualConsistency() {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug(raftNode.localEndpointStr() + " Querying: " + operation + " with policy: " + queryPolicy
                    + " in term: " + state.term());
        }

        raftNode.runOrScheduleQuery(new QueryContainer(operation, future), minCommitIndex, timeout);
    }

    private void queryWithBoundedStaleness() {
        if (state.leader() == null) {
            future.fail(raftNode.newCannotReplicateException());
            return;
        }

        raftNode.runOrScheduleQuery(new QueryContainer(operation, future), minCommitIndex, timeout);
    }

    private void queryWithLinearizability() {
        if (state.role() != LEADER) {
            future.fail(raftNode.newNotLeaderException());
            return;
        } else if (!raftNode.canQueryLinearizable()) {
            future.fail(raftNode.newCannotReplicateException());
            return;
        }

        long commitIndex = state.commitIndex();
        if (commitIndex < minCommitIndex) {
            future.fail(raftNode.newLaggingCommitIndexException(minCommitIndex));
            return;
        }

        if (state.logReplicationQuorumSize() > 1) {
            QueryState queryState = state.leaderState().queryState();

            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug(raftNode.localEndpointStr() + " Adding query at commit index: " + commitIndex
                        + ", query sequence number: " + queryState.querySequenceNumber());
            }

            if (queryState.addQuery(commitIndex, operation, future)) {
                raftNode.broadcastAppendEntriesRequest();
            }
        } else {
            queryWithEventualConsistency();
        }
    }

    private boolean verifyOperation() {
        if (operation instanceof RaftGroupOp) {
            future.fail(new IllegalArgumentException("cannot run query: " + operation));
            return false;
        }

        return true;
    }

    private boolean verifyRaftNodeStatus() {
        RaftNodeStatus status = raftNode.getStatus();
        if (status == INITIAL) {
            LOGGER.debug("{} Won't {} query {}, since Raft node is not started.", raftNode.localEndpointStr(),
                    queryPolicy, operation);
            future.fail(new IllegalStateException("Cannot query because Raft node not started"));
            return false;
        } else if (isTerminal(status)) {
            LOGGER.debug("{} Won't {} query {}, since Raft node is {}.", raftNode.localEndpointStr(), queryPolicy,
                    operation, status);
            future.fail(raftNode.newNotLeaderException());
            return false;
        }

        return true;
    }

}
