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

import io.microraft.RaftNode;
import io.microraft.RaftNodeStatus;
import io.microraft.exception.CannotReplicateException;
import io.microraft.exception.NotLeaderException;
import io.microraft.exception.RaftException;
import io.microraft.impl.RaftNodeImpl;
import io.microraft.impl.log.RaftLog;
import io.microraft.impl.state.RaftState;
import io.microraft.impl.util.OrderedFuture;
import io.microraft.model.groupop.TerminateRaftGroupOp;
import io.microraft.model.groupop.UpdateRaftGroupMembersOp;
import io.microraft.model.log.LogEntry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static io.microraft.RaftNodeStatus.INITIAL;
import static io.microraft.RaftNodeStatus.TERMINATING_RAFT_GROUP;
import static io.microraft.RaftNodeStatus.UPDATING_RAFT_GROUP_MEMBER_LIST;
import static io.microraft.RaftNodeStatus.isTerminal;
import static io.microraft.RaftRole.LEADER;

/**
 * Appends the given operation to the log of the given leader Raft node and
 * replicates the new entry to the followers.
 * <p>
 * Scheduled by {@link RaftNode#replicate(Object)},
 * or {@link MembershipChangeTask} for membership changes.
 * <p>
 * If the given Raft node is not the leader, the future is notified with
 * {@link NotLeaderException}.
 * <p>
 * If the given operation could not be appended to the Raft log at the moment,
 * (see {@link RaftNodeImpl#canReplicateNewOperation(Object)}), the future is
 * notified with {@link CannotReplicateException}.
 *
 * @author mdogan
 * @author metanet
 */
public final class ReplicateTask
        implements Runnable {

    private static final Logger LOGGER = LoggerFactory.getLogger(ReplicateTask.class);

    private final RaftNodeImpl raftNode;
    private final RaftState state;
    private final Object operation;
    private final OrderedFuture future;

    public ReplicateTask(RaftNodeImpl raftNode, Object operation, OrderedFuture future) {
        this.raftNode = raftNode;
        this.state = raftNode.state();
        this.operation = operation;
        this.future = future;
    }

    @Override
    public void run() {
        try {
            if (!verifyRaftNodeStatus()) {
                return;
            } else if (state.role() != LEADER) {
                future.fail(raftNode.newNotLeaderException());
                return;
            } else if (!raftNode.canReplicateNewOperation(operation)) {
                future.fail(raftNode.newCannotReplicateException());
                return;
            }

            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug(raftNode.localEndpointStr() + " Replicating: " + operation + " in term: " + state.term());
            }

            RaftLog log = state.log();

            if (!log.checkAvailableCapacity(1)) {
                future.fail(new IllegalStateException("Not enough capacity in RaftLog!"));
                return;
            }

            long newEntryLogIndex = log.lastLogOrSnapshotIndex() + 1;
            raftNode.registerFuture(newEntryLogIndex, future);
            LogEntry entry = raftNode.getModelFactory().createLogEntryBuilder().setTerm(state.term()).setIndex(newEntryLogIndex)
                                     .setOperation(operation).build();
            log.appendEntry(entry);

            preApplyGroupOp(newEntryLogIndex, operation);

            raftNode.broadcastAppendEntriesRequest();
        } catch (Throwable t) {
            LOGGER.error(raftNode.localEndpointStr() + " " + operation + " could not be replicated to leader: " + raftNode
                    .getLocalEndpoint(), t);
            future.fail(new RaftException("Internal failure", raftNode.getLeaderEndpoint(), t));
        }
    }

    private boolean verifyRaftNodeStatus() {
        RaftNodeStatus status = raftNode.getStatus();
        if (status == INITIAL) {
            LOGGER.debug("{} Won't run {}, since Raft node is not started.", raftNode.localEndpointStr(), operation);
            future.fail(raftNode.newCannotReplicateException());
            return false;
        } else if (isTerminal(status)) {
            LOGGER.debug("{} Won't run {}, since Raft node is {}.", raftNode.localEndpointStr(), operation, status);
            future.fail(raftNode.newNotLeaderException());
            return false;
        }

        return true;
    }

    private void preApplyGroupOp(long logIndex, Object operation) {
        if (operation instanceof TerminateRaftGroupOp) {
            raftNode.setStatus(TERMINATING_RAFT_GROUP);
        } else if (operation instanceof UpdateRaftGroupMembersOp) {
            raftNode.setStatus(UPDATING_RAFT_GROUP_MEMBER_LIST);
            raftNode.updateGroupMembers(logIndex, ((UpdateRaftGroupMembersOp) operation).getMembers());
        }
    }

}
