package com.hazelcast.raft.impl.task;

import com.hazelcast.raft.QueryPolicy;
import com.hazelcast.raft.exception.CannotReplicateException;
import com.hazelcast.raft.exception.NotLeaderException;
import com.hazelcast.raft.exception.RaftException;
import com.hazelcast.raft.impl.RaftNodeImpl;
import com.hazelcast.raft.impl.groupop.RaftGroupOp;
import com.hazelcast.raft.impl.state.QueryState;
import com.hazelcast.raft.impl.state.RaftState;
import com.hazelcast.raft.impl.util.InternallyCompletableFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.hazelcast.raft.RaftNodeStatus.STEPPED_DOWN;
import static com.hazelcast.raft.RaftNodeStatus.TERMINATED;
import static com.hazelcast.raft.RaftRole.LEADER;

/**
 * QueryTask is executed to query/read Raft state without appending log entry.
 * It's scheduled by {@link RaftNodeImpl#query(Object, QueryPolicy)}.
 *
 * @author mdogan
 * @author metanet
 * @see QueryPolicy
 */
public class QueryTask
        implements Runnable {

    private static final Logger LOGGER = LoggerFactory.getLogger(QueryTask.class);

    private final RaftNodeImpl raftNode;
    private final Object operation;
    private final QueryPolicy queryPolicy;
    private final InternallyCompletableFuture<Object> resultFuture;

    public QueryTask(RaftNodeImpl raftNode, Object operation, QueryPolicy policy,
                     InternallyCompletableFuture<Object> resultFuture) {
        this.raftNode = raftNode;
        this.operation = operation;
        this.queryPolicy = policy;
        this.resultFuture = resultFuture;
    }

    @Override
    public void run() {
        try {
            if (!verifyOperation()) {
                return;
            }

            if (!verifyRaftNodeStatus()) {
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
                    resultFuture
                            .internalCompleteExceptionally(new IllegalArgumentException("Invalid query policy: " + queryPolicy));
            }
        } catch (Throwable t) {
            LOGGER.error(raftNode.localEndpointName() + " -> " + queryPolicy + " query failed", t);
            resultFuture.internalCompleteExceptionally(new RaftException("Internal failure", raftNode.getLeaderEndpoint(), t));
        }
    }

    private void handleLeaderLocalRead() {
        RaftState state = raftNode.state();
        if (state.role() != LEADER) {
            resultFuture.internalCompleteExceptionally(new NotLeaderException(raftNode.getLocalEndpoint(), state.leader()));
            return;
        }

        handleAnyLocalRead();
    }

    private void handleAnyLocalRead() {
        RaftState state = raftNode.state();
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug(raftNode.localEndpointName() + " Querying: " + operation + " with policy: " + queryPolicy + " in term: "
                    + state.term());
        }

        raftNode.runQuery(operation, resultFuture);
    }

    private void handleLinearizableRead() {
        if (!raftNode.isLinearizableReadOptimizationEnabled()) {
            new ReplicateTask(raftNode, operation, resultFuture).run();
            return;
        }

        RaftState state = raftNode.state();
        if (state.role() != LEADER) {
            resultFuture.internalCompleteExceptionally(new NotLeaderException(raftNode.getLocalEndpoint(), state.leader()));
            return;
        }

        if (!raftNode.canQueryLinearizable()) {
            resultFuture.internalCompleteExceptionally(new CannotReplicateException(state.leader()));
            return;
        }

        long commitIndex = state.commitIndex();
        QueryState queryState = state.leaderState().queryState();

        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug(raftNode.localEndpointName() + " Adding query at commit index: " + commitIndex + ", query round: "
                    + queryState.queryRound());
        }

        if (queryState.addQuery(commitIndex, operation, resultFuture) == 1) {
            raftNode.broadcastAppendEntriesRequest();
        }
    }

    private boolean verifyOperation() {
        if (operation instanceof RaftGroupOp) {
            resultFuture.internalCompleteExceptionally(new IllegalArgumentException("cannot run query: " + operation));
            return false;
        }

        return true;
    }

    private boolean verifyRaftNodeStatus() {
        switch (raftNode.getStatus()) {
            case INITIAL:
                LOGGER.debug("{} Won't {} query {}, since Raft node is not started.", raftNode.localEndpointName(), queryPolicy,
                        operation);
                resultFuture
                        .internalCompleteExceptionally(new IllegalStateException("Cannot query because Raft node not started"));
                return false;
            case TERMINATED:
                LOGGER.debug("{} Won't {} query {}, since Raft node is terminated.", raftNode.localEndpointName(), queryPolicy,
                        operation);
                resultFuture.internalCompleteExceptionally(TERMINATED.createException(raftNode.getLocalEndpoint()));
                return false;
            case STEPPED_DOWN:
                LOGGER.debug("{} Won't {} query {}, since Raft node is stepped down.", raftNode.localEndpointName(), queryPolicy,
                        operation);
                resultFuture.internalCompleteExceptionally(STEPPED_DOWN.createException(raftNode.getLocalEndpoint()));
                return false;
        }

        return true;
    }

}
