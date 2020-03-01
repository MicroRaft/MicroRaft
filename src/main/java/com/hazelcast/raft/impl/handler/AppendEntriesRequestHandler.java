package com.hazelcast.raft.impl.handler;

import com.hazelcast.raft.RaftMsg;
import com.hazelcast.raft.impl.RaftNodeImpl;
import com.hazelcast.raft.impl.groupop.RaftGroupOp;
import com.hazelcast.raft.impl.groupop.TerminateRaftGroupOp;
import com.hazelcast.raft.impl.groupop.UpdateRaftGroupMembersOp;
import com.hazelcast.raft.impl.log.LogEntry;
import com.hazelcast.raft.impl.log.RaftLog;
import com.hazelcast.raft.impl.msg.AppendEntriesFailureResponse;
import com.hazelcast.raft.impl.msg.AppendEntriesRequest;
import com.hazelcast.raft.impl.msg.AppendEntriesSuccessResponse;
import com.hazelcast.raft.impl.task.RaftNodeStatusAwareTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;

import static com.hazelcast.raft.RaftNodeStatus.ACTIVE;
import static com.hazelcast.raft.RaftNodeStatus.TERMINATING;
import static com.hazelcast.raft.RaftNodeStatus.UPDATING_GROUP_MEMBER_LIST;
import static com.hazelcast.raft.RaftRole.FOLLOWER;
import static java.lang.Math.min;

/**
 * Handles an {@link AppendEntriesRequest} sent by the Raft group leader and
 * responds with either an {@link AppendEntriesSuccessResponse}
 * or an {@link AppendEntriesFailureResponse}.
 * <p>
 * See <i>5.3 Log replication</i> section of
 * <i>In Search of an Understandable Consensus Algorithm</i>
 * paper by <i>Diego Ongaro</i> and <i>John Ousterhout</i>.
 *
 * @author mdogan
 * @author metanet
 * @see AppendEntriesRequest
 * @see AppendEntriesSuccessResponse
 * @see AppendEntriesFailureResponse
 */
public class AppendEntriesRequestHandler
        extends RaftNodeStatusAwareTask
        implements Runnable {

    private static final Logger LOGGER = LoggerFactory.getLogger(AppendEntriesRequestHandler.class);

    private final AppendEntriesRequest request;

    public AppendEntriesRequestHandler(RaftNodeImpl raftNode, AppendEntriesRequest request) {
        super(raftNode);
        this.request = request;
    }

    @Override
    @SuppressWarnings({"checkstyle:npathcomplexity", "checkstyle:cyclomaticcomplexity", "checkstyle:methodlength", "checkstyle:nestedifdepth"})
    // Justification: It is easier to follow the AppendEntriesRPC logic in a single method
    protected void doRun() {
        LOGGER.debug("{} Received {}", localEndpointName(), request);

        // Reply false if term < currentTerm (§5.1)
        if (request.term() < state.term()) {
            if (LOGGER.isDebugEnabled()) {
                LOGGER.warn(localEndpointName() + " Stale " + request + " received in current term: " + state.term());
            }

            node.send(createFailureResponse(state.term()), request.leader());
            return;
        }

        RaftLog raftLog = state.log();

        // Transform into follower if a newer term is seen or another node wins the election of the current term
        if (request.term() > state.term() || state.role() != FOLLOWER) {
            // If the request or response contains term T > currentTerm: set currentTerm = T, convert to follower (§5.1)
            LOGGER.info("{} Demoting to FOLLOWER from current role: {}, term: {} to new term: {} and leader: {}",
                    localEndpointName(), state.role(), state.term(), request.term(), request.leader());
            node.toFollower(request.term());
        }

        if (!request.leader().equals(state.leader())) {
            LOGGER.info("{} Setting leader: {}", localEndpointName(), request.leader());
            node.leader(request.leader());
        }

        // Verify the last log entry
        if (request.prevLogIndex() > 0) {
            long lastLogIndex = raftLog.lastLogOrSnapshotIndex();
            int lastLogTerm = raftLog.lastLogOrSnapshotTerm();

            int prevLogTerm;
            if (request.prevLogIndex() == lastLogIndex) {
                prevLogTerm = lastLogTerm;
            } else {
                // Reply false if log does not contain an entry at prevLogIndex whose term matches prevLogTerm (§5.3)
                LogEntry prevLog = raftLog.getLogEntry(request.prevLogIndex());
                if (prevLog == null) {
                    if (LOGGER.isDebugEnabled()) {
                        LOGGER.warn(
                                localEndpointName() + " Failed to get previous log index for " + request + ", last log index: "
                                        + lastLogIndex);
                    }

                    node.send(createFailureResponse(request.term()), request.leader());
                    return;
                }
                prevLogTerm = prevLog.term();
            }

            if (request.prevLogTerm() != prevLogTerm) {
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.warn(
                            localEndpointName() + " Previous log term of " + request + " is different than ours: " + prevLogTerm);
                }

                node.send(createFailureResponse(request.term()), request.leader());
                return;
            }
        }

        int truncatedAppendRequestEntryCount = 0;
        LogEntry[] newEntries = null;
        // Process any new entries
        if (request.entryCount() > 0) {
            // Delete any conflicting entries, skip any duplicates
            long lastLogIndex = raftLog.lastLogOrSnapshotIndex();

            for (int i = 0; i < request.entryCount(); i++) {
                LogEntry reqEntry = request.entries()[i];

                if (reqEntry.index() > lastLogIndex) {
                    newEntries = Arrays.copyOfRange(request.entries(), i, request.entryCount());
                    break;
                }

                LogEntry localEntry = raftLog.getLogEntry(reqEntry.index());

                assert localEntry != null :
                        localEndpointName() + " Entry not found on log index: " + reqEntry.index() + " for " + request;

                // If an existing entry conflicts with a new one (same index but different terms),
                // delete the existing entry and all that follow it (§5.3)
                if (reqEntry.term() != localEntry.term()) {
                    List<LogEntry> truncatedEntries = raftLog.truncateEntriesFrom(reqEntry.index());
                    if (LOGGER.isDebugEnabled()) {
                        LOGGER.warn(localEndpointName() + " Truncated " + truncatedEntries.size() + " entries from entry index: "
                                + reqEntry.index() + " => " + truncatedEntries);
                    } else {
                        LOGGER.warn("{} Truncated {} entries from entry index: {}", localEndpointName(), truncatedEntries.size(),
                                reqEntry.index());
                    }

                    node.invalidateFuturesFrom(reqEntry.index());
                    revertPreAppliedGroupOp(truncatedEntries);
                    newEntries = Arrays.copyOfRange(request.entries(), i, request.entryCount());
                    break;
                }
            }

            if (newEntries != null && newEntries.length > 0) {
                if (raftLog.availableCapacity() < newEntries.length) {
                    if (LOGGER.isDebugEnabled()) {
                        LOGGER.warn(localEndpointName() + " Truncating " + newEntries.length + " entries to " + raftLog
                                .availableCapacity() + " to fit into the available capacity of the Raft log");
                    }

                    truncatedAppendRequestEntryCount = newEntries.length - raftLog.availableCapacity();
                    newEntries = Arrays.copyOf(newEntries, raftLog.availableCapacity());
                }

                // Append any new entries not already in the log
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug(
                            localEndpointName() + " Appending " + newEntries.length + " entries: " + Arrays.toString(newEntries));
                }

                raftLog.appendEntries(newEntries);
            }
        }

        // I cannot use raftLog.lastLogOrSnapshotIndex() for lastLogIndex because my log may contain
        // some uncommitted entries from the previous leader and those entries will be truncated soon
        // I can only send a response based on how many entries I have appended from this append request
        long lastLogIndex = request.prevLogIndex() + request.entryCount() - truncatedAppendRequestEntryCount;
        long oldCommitIndex = state.commitIndex();

        // Update the commit index
        if (request.leaderCommitIndex() > oldCommitIndex) {
            // If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
            long newCommitIndex = min(request.leaderCommitIndex(), lastLogIndex);
            LOGGER.debug("{} Setting commit index: {}", localEndpointName(), newCommitIndex);

            state.commitIndex(newCommitIndex);
        }

        node.updateLastAppendEntriesTimestamp();

        try {
            RaftMsg resp = new AppendEntriesSuccessResponse(localEndpoint(), state.term(), lastLogIndex, request.queryRound());
            node.send(resp, request.leader());
        } finally {
            if (state.commitIndex() > oldCommitIndex) {
                node.applyLogEntries();
            }
            if (newEntries != null) {
                preApplyGroupOp(newEntries, state.commitIndex());
            }
        }
    }

    private void preApplyGroupOp(LogEntry[] entries, long commitIndex) {
        // There can be at most one appended & not-committed group operation in the log
        Arrays.stream(entries).filter(entry -> entry.index() > commitIndex && entry.operation() instanceof RaftGroupOp)
              .findFirst().ifPresent(entry -> {
            Object operation = entry.operation();
            if (operation instanceof TerminateRaftGroupOp) {
                node.setStatus(TERMINATING);
            } else if (operation instanceof UpdateRaftGroupMembersOp) {
                node.setStatus(UPDATING_GROUP_MEMBER_LIST);
                node.updateGroupMembers(entry.index(), ((UpdateRaftGroupMembersOp) operation).getMembers());
            } else {
                assert false : "Invalid Raft group operation: " + operation + " in " + node.getGroupId();
            }
        });
    }

    private void revertPreAppliedGroupOp(List<LogEntry> entries) {
        // Reverting inflight (i.e., appended but not-yet-committed) Raft group operations.
        // There can be at most 1 such operation...
        entries.stream().filter(entry -> entry.operation() instanceof RaftGroupOp).findFirst().ifPresent(entry -> {
            node.setStatus(ACTIVE);
            if (entry.operation() instanceof UpdateRaftGroupMembersOp) {
                node.resetGroupMembers();
            }
        });
    }

    private AppendEntriesFailureResponse createFailureResponse(int term) {
        return new AppendEntriesFailureResponse(localEndpoint(), term, request.prevLogIndex() + 1);
    }
}
