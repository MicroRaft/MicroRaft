/*
 * Original work Copyright (c) 2008-2020, Hazelcast, Inc.
 * Modified work Copyright 2020, MicroRaft.
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

import io.microraft.QueryPolicy;
import io.microraft.RaftNodeStatus;
import io.microraft.exception.LaggingCommitIndexException;
import io.microraft.exception.RaftException;
import io.microraft.impl.RaftNodeImpl;
import io.microraft.impl.state.QueryState;
import io.microraft.impl.state.RaftState;
import io.microraft.impl.util.OrderedFuture;
import io.microraft.model.groupop.RaftGroupOp;
import io.microraft.statemachine.StateMachine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static io.microraft.RaftNodeStatus.INITIAL;
import static io.microraft.RaftNodeStatus.isTerminal;
import static io.microraft.RaftRole.LEADER;

/**
 * Scheduled by {@link RaftNodeImpl#query(Object, QueryPolicy, long)}
 * to perform a query on the {@link StateMachine}.
 *
 * @see QueryPolicy
 */
public final class QueryTask
        implements Runnable {

    private static final Logger LOGGER = LoggerFactory.getLogger(QueryTask.class);

    private final RaftNodeImpl raftNode;
    private final RaftState state;
    private final Object operation;
    private final QueryPolicy queryPolicy;
    private final long minCommitIndex;
    private final OrderedFuture future;

    public QueryTask(RaftNodeImpl raftNode, Object operation, QueryPolicy policy, long minCommitIndex, OrderedFuture future) {
        this.raftNode = raftNode;
        this.state = raftNode.state();
        this.operation = operation;
        this.queryPolicy = policy;
        this.minCommitIndex = minCommitIndex;
        this.future = future;
    }

    @Override
    public void run() {
        try {
            if (!verifyOperation() || !verifyRaftNodeStatus()) {
                return;
            }

            switch (queryPolicy) {
                case LEADER_LOCAL:
                    handleLeaderLocalRead();
                    break;
                case ANY_LOCAL:
                    handleAnyLocalRead();
                    break;
                case LINEARIZABLE:
                    handleLinearizableRead();
                    break;
                default:
                    future.fail(new IllegalArgumentException("Invalid query policy: " + queryPolicy));
            }
        } catch (Throwable t) {
            LOGGER.error(raftNode.localEndpointStr() + queryPolicy + " query failed", t);
            future.fail(new RaftException("Internal failure", raftNode.getLeaderEndpoint(), t));
        }
    }

    private void handleLeaderLocalRead() {
        if (raftNode.demoteToFollowerIfMajorityHeartbeatTimeoutElapsed()) {
            future.fail(raftNode.newNotLeaderException());
            return;
        }

        handleAnyLocalRead();
    }

    private void handleAnyLocalRead() {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug(raftNode.localEndpointStr() + " Querying: " + operation + " with policy: " + queryPolicy + " in term: "
                                 + state.term());
        }

        raftNode.runQuery(operation, minCommitIndex, future);
    }

    private void handleLinearizableRead() {
        if (state.role() != LEADER) {
            future.fail(raftNode.newNotLeaderException());
            return;
        } else if (!raftNode.canQueryLinearizable()) {
            future.fail(raftNode.newCannotReplicateException());
            return;
        }

        long commitIndex = state.commitIndex();
        if (commitIndex < minCommitIndex) {
            future.fail(new LaggingCommitIndexException(state.commitIndex(), minCommitIndex, state.leader()));
            return;
        }

        QueryState queryState = state.leaderState().queryState();

        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug(raftNode.localEndpointStr() + " Adding query at commit index: " + commitIndex + ", query seq no: "
                                 + queryState.querySeqNo());
        }

        if (queryState.addQuery(commitIndex, operation, future)) {
            raftNode.broadcastAppendEntriesRequest();
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
            LOGGER.debug("{} Won't {} query {}, since Raft node is not started.", raftNode.localEndpointStr(), queryPolicy,
                         operation);
            future.fail(new IllegalStateException("Cannot query because Raft node not started"));
            return false;
        } else if (isTerminal(status)) {
            LOGGER.debug("{} Won't {} query {}, since Raft node is {}.", raftNode.localEndpointStr(), queryPolicy, operation,
                         status);
            future.fail(raftNode.newNotLeaderException());
            return false;
        }

        return true;
    }

}
