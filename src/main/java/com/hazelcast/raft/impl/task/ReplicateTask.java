package com.hazelcast.raft.impl.task;

import com.hazelcast.raft.RaftNode;
import com.hazelcast.raft.exception.CannotReplicateException;
import com.hazelcast.raft.exception.NotLeaderException;
import com.hazelcast.raft.exception.RaftException;
import com.hazelcast.raft.impl.RaftNodeImpl;
import com.hazelcast.raft.impl.groupop.TerminateRaftGroupOp;
import com.hazelcast.raft.impl.groupop.UpdateRaftGroupMembersOp;
import com.hazelcast.raft.impl.log.LogEntry;
import com.hazelcast.raft.impl.log.RaftLog;
import com.hazelcast.raft.impl.state.RaftState;
import com.hazelcast.raft.impl.util.InternallyCompletableFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.hazelcast.raft.RaftNodeStatus.STEPPED_DOWN;
import static com.hazelcast.raft.RaftNodeStatus.TERMINATED;
import static com.hazelcast.raft.RaftNodeStatus.TERMINATING;
import static com.hazelcast.raft.RaftNodeStatus.UPDATING_GROUP_MEMBER_LIST;
import static com.hazelcast.raft.RaftRole.LEADER;

/**
 * ReplicateTask is executed to append an operation to Raft log
 * and replicate the new entry to followers. It's scheduled by
 * {@link RaftNode#replicate(Object)}
 * or by {@link MembershipChangeTask} for membership changes.
 * <p>
 * If this node is not the leader, future is immediately notified with
 * {@link NotLeaderException}.
 * <p>
 * If replication of the operation is not allowed at the moment
 * (see {@link RaftNodeImpl#canReplicateNewOperation(Object)}), the future is
 * immediately notified with {@link CannotReplicateException}.
 *
 * @author mdogan
 * @author metanet
 */
public class ReplicateTask
        implements Runnable {

    private static final Logger LOGGER = LoggerFactory.getLogger(ReplicateTask.class);

    private final RaftNodeImpl raftNode;
    private final Object operation;
    private final InternallyCompletableFuture<Object> resultFuture;

    public ReplicateTask(RaftNodeImpl raftNode, Object operation, InternallyCompletableFuture<Object> resultFuture) {
        this.raftNode = raftNode;
        this.operation = operation;
        this.resultFuture = resultFuture;
    }

    @Override
    public void run() {
        try {
            if (!verifyRaftNodeStatus()) {
                return;
            }

            RaftState state = raftNode.state();
            if (state.role() != LEADER) {
                resultFuture.internalCompleteExceptionally(new NotLeaderException(raftNode.getLocalEndpoint(), state.leader()));
                return;
            }

            if (!raftNode.canReplicateNewOperation(operation)) {
                resultFuture.internalCompleteExceptionally(new CannotReplicateException(raftNode.getLocalEndpoint()));
                return;
            }

            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug(raftNode.localEndpointName() + " Replicating: " + operation + " in term: " + state.term());
            }

            RaftLog log = state.log();

            if (!log.checkAvailableCapacity(1)) {
                resultFuture.internalCompleteExceptionally(new IllegalStateException("Not enough capacity in RaftLog!"));
                return;
            }

            long newEntryLogIndex = log.lastLogOrSnapshotIndex() + 1;
            raftNode.registerFuture(newEntryLogIndex, resultFuture);
            log.appendEntries(new LogEntry(state.term(), newEntryLogIndex, operation));

            preApplyGroupOp(newEntryLogIndex, operation);

            raftNode.broadcastAppendEntriesRequest();
        } catch (Throwable t) {
            LOGGER.error(raftNode.localEndpointName() + " " + operation + " could not be replicated to leader: " + raftNode
                    .getLocalEndpoint(), t);
            resultFuture.internalCompleteExceptionally(new RaftException("Internal failure", raftNode.getLeaderEndpoint(), t));
        }
    }

    private boolean verifyRaftNodeStatus() {
        switch (raftNode.getStatus()) {
            case INITIAL:
                LOGGER.debug("{} Won't run {}, since Raft node is not started.", raftNode.localEndpointName(), operation);
                resultFuture.internalCompleteExceptionally(
                        new IllegalStateException("Cannot replicate because Raft node not started"));
                return false;
            case TERMINATED:
                LOGGER.debug("{} Won't run {}, since Raft node is terminated.", raftNode.localEndpointName(), operation);
                resultFuture.internalCompleteExceptionally(TERMINATED.createException(raftNode.getLocalEndpoint()));
                return false;
            case STEPPED_DOWN:
                LOGGER.debug("{} Won't run {}, since Raft node is stepped down.", raftNode.localEndpointName(), operation);
                resultFuture.internalCompleteExceptionally(STEPPED_DOWN.createException(raftNode.getLocalEndpoint()));
                return false;
        }

        return true;
    }

    private void preApplyGroupOp(long logIndex, Object operation) {
        if (operation instanceof TerminateRaftGroupOp) {
            raftNode.setStatus(TERMINATING);
        } else if (operation instanceof UpdateRaftGroupMembersOp) {
            raftNode.setStatus(UPDATING_GROUP_MEMBER_LIST);
            raftNode.updateGroupMembers(logIndex, ((UpdateRaftGroupMembersOp) operation).getMembers());
        }
    }

}
