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

package io.microraft.impl.handler;

import static io.microraft.RaftNodeStatus.ACTIVE;
import static io.microraft.RaftNodeStatus.UPDATING_RAFT_GROUP_MEMBER_LIST;
import static io.microraft.RaftRole.FOLLOWER;
import static io.microraft.RaftRole.LEARNER;
import static java.lang.Math.min;
import static java.util.Collections.emptyList;
import static java.util.Objects.requireNonNull;

import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.List;
import java.util.Map.Entry;

import javax.annotation.Nonnull;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.microraft.RaftEndpoint;
import io.microraft.impl.RaftNodeImpl;
import io.microraft.impl.log.RaftLog;
import io.microraft.model.groupop.RaftGroupOp;
import io.microraft.model.groupop.UpdateRaftGroupMembersOp;
import io.microraft.model.log.LogEntry;
import io.microraft.model.message.AppendEntriesFailureResponse;
import io.microraft.model.message.AppendEntriesRequest;
import io.microraft.model.message.AppendEntriesSuccessResponse;
import io.microraft.model.message.RaftMessage;

/**
 * Handles an {@link AppendEntriesRequest} sent by the Raft group leader and
 * responds with either an {@link AppendEntriesSuccessResponse} or an
 * {@link AppendEntriesFailureResponse}.
 * <p>
 * See <i>5.3 Log replication</i> section of <i>In Search of an Understandable
 * Consensus Algorithm</i> paper by <i>Diego Ongaro</i> and <i>John
 * Ousterhout</i>.
 *
 * @see AppendEntriesRequest
 * @see AppendEntriesSuccessResponse
 * @see AppendEntriesFailureResponse
 */
public class AppendEntriesRequestHandler extends AbstractMessageHandler<AppendEntriesRequest> {

    private static final Logger LOGGER = LoggerFactory.getLogger(AppendEntriesRequestHandler.class);

    public AppendEntriesRequestHandler(RaftNodeImpl raftNode, AppendEntriesRequest request) {
        super(raftNode, request);
    }

    @Override
    @SuppressWarnings({"checkstyle:npathcomplexity", "checkstyle:cyclomaticcomplexity", "checkstyle:methodlength",
            "checkstyle:nestedifdepth"})
    // Justification: It is easier to follow the AppendEntriesRPC logic in a single
    // method
    protected void handle(@Nonnull AppendEntriesRequest request) {
        requireNonNull(request);

        LOGGER.debug("{} received {}.", localEndpointStr(), request);
        RaftEndpoint leader = request.getSender();

        // Reply false if term < currentTerm (§5.1)
        if (request.getTerm() < state.term()) {
            if (LOGGER.isDebugEnabled()) {
                LOGGER.warn(localEndpointStr() + " Stale " + request + " received in current term: " + state.term());
            }

            node.send(leader, createAppendEntriesFailureResponse(state.term(), 0, 0));
            return;
        }

        RaftLog log = state.log();

        // Transform into follower if a newer term is seen or another node wins the
        // election of the current term
        if (request.getTerm() > state.term() || (state.role() != FOLLOWER && state.role() != LEARNER)) {
            // If the request term is greater than the local term, update the local term and
            // convert to follower (§5.1)
            LOGGER.info("{} Moving to new term: {} and leader: {} from current term: {}.", localEndpointStr(),
                    request.getTerm(), leader.getId(), state.term());
            node.toFollower(request.getTerm());
        }

        if (!leader.equals(state.leader())) {
            LOGGER.info("{} Setting leader: {}", localEndpointStr(), leader.getId());
            node.leader(leader);
        }

        node.leaderHeartbeatReceived();

        if (!verifyLastLogEntry(request, log)) {
            RaftMessage response = createAppendEntriesFailureResponse(request.getTerm(),
                    request.getQuerySequenceNumber(), request.getFlowControlSequenceNumber());
            node.send(leader, response);
            return;
        }

        Entry<Long, List<LogEntry>> e = appendLogEntries(request, log);
        long lastLogIndex = e.getKey();
        List<LogEntry> newLogEntries = e.getValue();
        long oldCommitIndex = state.commitIndex();

        // Update the commit index
        if (request.getCommitIndex() > oldCommitIndex) {
            // If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of
            // last new entry)
            long newCommitIndex = min(request.getCommitIndex(), lastLogIndex);
            LOGGER.debug("{} Setting commit index: {}.", localEndpointStr(), newCommitIndex);

            state.commitIndex(newCommitIndex);
        }

        try {
            RaftMessage response = modelFactory.createAppendEntriesSuccessResponseBuilder()
                    .setGroupId(node.getGroupId()).setSender(localEndpoint()).setTerm(state.term())
                    .setLastLogIndex(lastLogIndex).setQuerySequenceNumber(request.getQuerySequenceNumber())
                    .setFlowControlSequenceNumber(request.getFlowControlSequenceNumber()).build();
            node.send(leader, response);
        } finally {
            boolean commitIndexAdvanced = (state.commitIndex() > oldCommitIndex);
            if (commitIndexAdvanced) {
                node.applyLogEntries();
            }

            if (newLogEntries.size() > 0) {
                prepareGroupOp(newLogEntries, state.commitIndex());
            }

            if (commitIndexAdvanced) {
                node.tryRunScheduledQueries();
            }
        }
    }

    private boolean verifyLastLogEntry(AppendEntriesRequest request, RaftLog log) {
        if (request.getPreviousLogIndex() > 0) {
            long lastLogIndex = log.lastLogOrSnapshotIndex();
            int lastLogTerm = log.lastLogOrSnapshotTerm();

            int prevLogTerm;
            if (request.getPreviousLogIndex() == lastLogIndex) {
                prevLogTerm = lastLogTerm;
            } else if (log.snapshotIndex() >= request.getPreviousLogIndex()) {
                prevLogTerm = log.snapshotEntry().getTerm();
            } else {
                // Reply false if log does not contain an entry at prevLogIndex whose term
                // matches prevLogTerm (§5.3)
                LogEntry prevEntry = log.getLogEntry(request.getPreviousLogIndex());
                if (prevEntry == null) {
                    if (LOGGER.isDebugEnabled()) {
                        LOGGER.warn(localEndpointStr() + " Failed to get previous log index for " + request + ", last"
                                + " log index: " + lastLogIndex);
                    }

                    return false;
                }

                prevLogTerm = prevEntry.getTerm();
            }

            if (request.getPreviousLogTerm() != prevLogTerm) {
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.warn(localEndpointStr() + " Previous log term of " + request + " is different than ours: "
                            + prevLogTerm);
                }

                return false;
            }
        }

        return true;
    }

    private Entry<Long, List<LogEntry>> appendLogEntries(AppendEntriesRequest request, RaftLog log) {
        int truncatedRequestEntryCount = 0;
        List<LogEntry> newLogEntries = emptyList();
        // Process any new entries
        if (request.getLogEntries().size() > 0) {
            // Delete any conflicting entries, skip any duplicates
            long lastLogIndex = log.lastLogOrSnapshotIndex();
            for (int i = 0, requestEntryCount = request.getLogEntries().size(); i < requestEntryCount; i++) {
                LogEntry requestEntry = request.getLogEntries().get(i);

                if (requestEntry.getIndex() > lastLogIndex) {
                    newLogEntries = request.getLogEntries().subList(i, requestEntryCount);
                    break;
                }

                LogEntry localEntry = log.getLogEntry(requestEntry.getIndex());

                assert localEntry != null : localEndpointStr() + " Entry not found on log index: "
                        + requestEntry.getIndex() + " for " + request;

                // If an existing entry conflicts with a new one (same index but different
                // terms),
                // delete the existing entry and all that follow it (§5.3)
                if (requestEntry.getTerm() != localEntry.getTerm()) {
                    List<LogEntry> truncatedEntries = log.truncateEntriesFrom(requestEntry.getIndex());
                    if (LOGGER.isDebugEnabled()) {
                        LOGGER.warn(localEndpointStr() + " Truncated " + truncatedEntries.size() + " entries from "
                                + "entry index: " + requestEntry.getIndex() + " => " + truncatedEntries);
                    } else {
                        LOGGER.warn("{} Truncated {} entries from entry index: {}", localEndpointStr(),
                                truncatedEntries.size(), requestEntry.getIndex());
                    }

                    state.invalidateFuturesFrom(requestEntry.getIndex(), node.newNotLeaderException());
                    // TODO(szymon): What happens if we achieve to persist the step down (see
                    //   `demoteToNonVotingMember`) but
                    //   fail to flush the log?
                    revertPreparedGroupOp(truncatedEntries);
                    newLogEntries = request.getLogEntries().subList(i, requestEntryCount);
                    log.flush();
                    break;
                }
            }

            if (newLogEntries.size() > 0) {
                if (log.availableCapacity() < newLogEntries.size()) {
                    if (LOGGER.isDebugEnabled()) {
                        LOGGER.warn(localEndpointStr() + " Truncating " + newLogEntries.size() + " entries to "
                                + log.availableCapacity() + " to fit into the available capacity of the Raft log");
                    }

                    truncatedRequestEntryCount = newLogEntries.size() - log.availableCapacity();
                    newLogEntries = newLogEntries.subList(0, log.availableCapacity());
                }

                // Append any new entries not already in the log
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug(
                            localEndpointStr() + " Appending " + newLogEntries.size() + " entries: " + newLogEntries);
                }

                log.appendEntries(newLogEntries);
                log.flush();
            }
        }

        // I cannot use log.lastLogOrSnapshotIndex() for lastLogIndex because my log may
        // contain
        // some pending entries from the previous leader and those entries will be
        // truncated soon
        // I can only send a response based on how many entries I have appended from
        // this append request
        long lastLogIndex = request.getPreviousLogIndex() + request.getLogEntries().size() - truncatedRequestEntryCount;

        return new SimpleImmutableEntry<>(lastLogIndex, newLogEntries);
    }

    private void prepareGroupOp(List<LogEntry> logEntries, long commitIndex) {
        // There can be at most one appended & not-committed group operation in the log
        logEntries.stream()
                .filter(logEntry -> logEntry.getIndex() > commitIndex && logEntry.getOperation() instanceof RaftGroupOp)
                .findFirst().ifPresent(logEntry -> {
                    Object operation = logEntry.getOperation();
                    assert (operation instanceof UpdateRaftGroupMembersOp)
                            : "Invalid Raft group operation: " + operation + " in " + node.getGroupId();
                    node.setStatus(UPDATING_RAFT_GROUP_MEMBER_LIST);
                    UpdateRaftGroupMembersOp groupOp = (UpdateRaftGroupMembersOp) operation;
                    node.updateGroupMembers(logEntry.getIndex(), groupOp.getMembers(), groupOp.getVotingMembers());
                });
    }

    private void revertPreparedGroupOp(List<LogEntry> logEntries) {
        // Reverting inflight (i.e., appended but not-yet-committed) Raft group
        // operations.
        // There can be at most 1 instance of such operation...
        logEntries.stream().filter(logEntry -> logEntry.getOperation() instanceof RaftGroupOp).findFirst()
                .ifPresent(logEntry -> {
                    node.setStatus(ACTIVE);
                    if (logEntry.getOperation() instanceof UpdateRaftGroupMembersOp) {
                        node.revertGroupMembers();
                    }
                });
    }

    private RaftMessage createAppendEntriesFailureResponse(int term, long queryRound, long sequenceNumber) {
        return modelFactory.createAppendEntriesFailureResponseBuilder().setGroupId(node.getGroupId())
                .setSender(localEndpoint()).setTerm(term).setExpectedNextIndex(message.getPreviousLogIndex() + 1)
                .setQuerySequenceNumber(queryRound).setFlowControlSequenceNumber(sequenceNumber).build();
    }

}
