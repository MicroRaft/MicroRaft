package com.hazelcast.raft.impl.task;

import com.hazelcast.raft.MembershipChangeMode;
import com.hazelcast.raft.RaftEndpoint;
import com.hazelcast.raft.RaftGroupMembers;
import com.hazelcast.raft.exception.MismatchingRaftGroupMembersCommitIndexException;
import com.hazelcast.raft.exception.NotLeaderException;
import com.hazelcast.raft.exception.RaftException;
import com.hazelcast.raft.impl.RaftNodeImpl;
import com.hazelcast.raft.impl.groupop.RaftGroupOp;
import com.hazelcast.raft.impl.groupop.UpdateRaftGroupMembersOp;
import com.hazelcast.raft.impl.state.RaftState;
import com.hazelcast.raft.impl.util.InternallyCompletableFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.LinkedHashSet;

import static com.hazelcast.raft.RaftNodeStatus.STEPPED_DOWN;
import static com.hazelcast.raft.RaftNodeStatus.TERMINATED;
import static com.hazelcast.raft.RaftRole.LEADER;

/**
 * MembershipChangeTask is executed to add/remove a member to the Raft group.
 * <p>
 * If membership change type is ADD but the member already exists in the group,
 * then future is notified with {@link IllegalStateException}.
 * <p>
 * If membership change type is REMOVE but the member doesn't exist
 * in the group, then future is notified with
 * {@link IllegalStateException}.
 * <p>
 * {@link UpdateRaftGroupMembersOp} Raft operation is created with members
 * according to the member parameter and membership change and it's replicated
 * via {@link ReplicateTask}.
 *
 * @author mdogan
 * @author metanet
 * @see MembershipChangeMode
 */
public class MembershipChangeTask
        implements Runnable {

    private static final Logger LOGGER = LoggerFactory.getLogger(MembershipChangeTask.class);

    private final RaftNodeImpl raftNode;
    private final long groupMembersCommitIndex;
    private final RaftEndpoint endpoint;
    private final MembershipChangeMode membershipChangeMode;
    private final InternallyCompletableFuture<Object> resultFuture;

    public MembershipChangeTask(RaftNodeImpl raftNode, InternallyCompletableFuture<Object> resultFuture, RaftEndpoint endpoint,
                                MembershipChangeMode membershipChangeMode, long groupMembersCommitIndex) {
        this.raftNode = raftNode;
        this.groupMembersCommitIndex = groupMembersCommitIndex;
        this.endpoint = endpoint;
        this.membershipChangeMode = membershipChangeMode;
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

            if (!isValidGroupMemberCommitIndex()) {
                return;
            }

            Collection<RaftEndpoint> members = new LinkedHashSet<>(state.members());
            boolean memberExists = members.contains(endpoint);

            if (membershipChangeMode == null) {
                resultFuture.internalCompleteExceptionally(new IllegalArgumentException("Unknown type: " + membershipChangeMode));
                return;
            }

            switch (membershipChangeMode) {
                case ADD:
                    if (memberExists) {
                        resultFuture.internalCompleteExceptionally(new IllegalArgumentException(
                                endpoint + " already exists in " + members + " of group " + raftNode.getGroupId()));
                        return;
                    }
                    members.add(endpoint);
                    break;

                case REMOVE:
                    if (!memberExists) {
                        resultFuture.internalCompleteExceptionally(new IllegalArgumentException(
                                endpoint + " does not exist in " + members + " of group " + raftNode.getGroupId()));
                        return;
                    }
                    members.remove(endpoint);
                    break;

                default:
                    resultFuture
                            .internalCompleteExceptionally(new IllegalArgumentException("Unknown type: " + membershipChangeMode));
                    return;
            }

            LOGGER.info("{} New group members after {} {} -> {}", raftNode.localEndpointName(), membershipChangeMode, endpoint,
                    members);
            RaftGroupOp operation = new UpdateRaftGroupMembersOp(members, endpoint, membershipChangeMode);
            new ReplicateTask(raftNode, operation, resultFuture).run();
        } catch (Throwable t) {
            LOGGER.error(raftNode.localEndpointName() + " " + this + " failed.", t);
            resultFuture.internalCompleteExceptionally(new RaftException("Internal failure", raftNode.getLeaderEndpoint(), t));
        }
    }

    private boolean verifyRaftNodeStatus() {
        switch (raftNode.getStatus()) {
            case INITIAL:
                LOGGER.error("{} Cannot {} {} with expected members commit index: {} since raft node is not started.",
                        raftNode.localEndpointName(), membershipChangeMode, endpoint, groupMembersCommitIndex);
                resultFuture.internalCompleteExceptionally(
                        new IllegalStateException("Cannot change group membership because Raft node not started"));
                return false;
            case TERMINATED:
                LOGGER.error("{} Cannot {} {} with expected members commit index: {} since raft node is terminated.",
                        raftNode.localEndpointName(), membershipChangeMode, endpoint, groupMembersCommitIndex);
                resultFuture.internalCompleteExceptionally(TERMINATED.createException(raftNode.getLocalEndpoint()));
                return false;
            case STEPPED_DOWN:
                LOGGER.error("{} Cannot {} {} with expected members commit index: {} since raft node is stepped down.",
                        raftNode.localEndpointName(), membershipChangeMode, endpoint, groupMembersCommitIndex);
                resultFuture.internalCompleteExceptionally(STEPPED_DOWN.createException(raftNode.getLocalEndpoint()));
                return false;
        }

        return true;
    }

    private boolean isValidGroupMemberCommitIndex() {
        RaftState state = raftNode.state();
        RaftGroupMembers groupMembers = state.committedGroupMembers();
        if (groupMembers.index() != groupMembersCommitIndex) {
            LOGGER.error(
                    "{} Cannot {} {} because expected members commit index: {} is different than group members commit index: {}",
                    raftNode.localEndpointName(), membershipChangeMode, endpoint, groupMembersCommitIndex, groupMembers.index());

            resultFuture.internalCompleteExceptionally(
                    new MismatchingRaftGroupMembersCommitIndexException(groupMembers.index(), groupMembers.members()));
            return false;
        }

        return true;
    }

    @Override
    public String toString() {
        return "MembershipChangeTask{" + "groupMembersCommitIndex=" + groupMembersCommitIndex + ", member=" + endpoint
                + ", membershipChangeMode=" + membershipChangeMode + '}';
    }
}
