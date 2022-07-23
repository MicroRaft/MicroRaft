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

import io.microraft.MembershipChangeMode;
import io.microraft.RaftEndpoint;
import io.microraft.RaftNodeStatus;
import io.microraft.exception.MismatchingRaftGroupMembersCommitIndexException;
import io.microraft.exception.RaftException;
import io.microraft.impl.RaftNodeImpl;
import io.microraft.impl.state.RaftGroupMembersState;
import io.microraft.impl.state.RaftState;
import io.microraft.impl.util.OrderedFuture;
import io.microraft.model.groupop.RaftGroupOp;
import io.microraft.model.groupop.UpdateRaftGroupMembersOp;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.LinkedHashSet;

import static io.microraft.RaftNodeStatus.INITIAL;
import static io.microraft.RaftNodeStatus.isTerminal;
import static io.microraft.RaftRole.LEADER;
import static io.microraft.RaftRole.LEARNER;
import static io.microraft.report.RaftGroupMembers.MAX_LEARNER_COUNT;
import static java.util.stream.Collectors.toList;

/**
 * Executed to add or remove a member to the Raft group.
 * <p>
 * If the membership change mode is {@link MembershipChangeMode#ADD_LEARNER} but the given member already exists in the
 * Raft group, or the membership change mode is {@link MembershipChangeMode#ADD_OR_PROMOTE_TO_FOLLOWER} but the given
 * member already exists in the Raft group as a voting member, then the future is notified with
 * {@link IllegalArgumentException}.
 * <p>
 * If the membership change mode is {@link MembershipChangeMode#REMOVE_MEMBER} but the member does not exist in the Raft
 * group, then the future is notified with {@link IllegalStateException}.
 * <p>
 * This task creates an instance of {@link UpdateRaftGroupMembersOp} that includes the requested membership change and
 * the new member list of the Raft group, and replicates this operation to the Raft group via passing it to
 * {@link ReplicateTask}.
 *
 * @see MembershipChangeMode
 */
public final class MembershipChangeTask implements Runnable {

    private static final Logger LOGGER = LoggerFactory.getLogger(MembershipChangeTask.class);

    private final RaftNodeImpl raftNode;
    private final RaftState state;
    private final long groupMembersCommitIndex;
    private final RaftEndpoint endpoint;
    private final MembershipChangeMode membershipChangeMode;
    private final OrderedFuture future;

    public MembershipChangeTask(RaftNodeImpl raftNode, OrderedFuture future, RaftEndpoint endpoint,
            MembershipChangeMode membershipChangeMode, long groupMembersCommitIndex) {
        this.raftNode = raftNode;
        this.state = raftNode.state();
        this.future = future;
        this.endpoint = endpoint;
        this.groupMembersCommitIndex = groupMembersCommitIndex;
        this.membershipChangeMode = membershipChangeMode;
    }

    @Override
    public void run() {
        try {
            if (!verifyRaftNodeStatus()) {
                return;
            } else if (state.role() != LEADER) {
                future.fail(raftNode.newNotLeaderException());
                return;
            } else if (!verifyGroupMembersCommitIndex()) {
                return;
            } else if (membershipChangeMode == null) {
                future.fail(new IllegalArgumentException("Null membership change mode!"));
                return;
            }

            RaftGroupMembersState effectiveMembers = state.effectiveGroupMembers();
            Collection<RaftEndpoint> members = new LinkedHashSet<>(effectiveMembers.getMembers());
            Collection<RaftEndpoint> votingMembers = new LinkedHashSet<>(effectiveMembers.getVotingMembers());

            switch (membershipChangeMode) {
            case ADD_LEARNER:
                if (members.contains(endpoint)) {
                    String msg = endpoint + " already exists in " + members + " of group " + raftNode.getGroupId();
                    future.fail(new IllegalArgumentException(msg));
                    return;
                } else if (members.size() - votingMembers.size() == MAX_LEARNER_COUNT) {
                    String msg = "Cannot add " + endpoint + " to group " + raftNode.getGroupId() + " as 3rd " + LEARNER;
                    future.fail(new IllegalArgumentException(msg));
                    return;
                } else if (state.initialMembers().isKnownMember(endpoint)) {
                    String msg = endpoint + " already exists in the initial member list: " + members + " of group "
                            + raftNode.getGroupId();
                    future.fail(new IllegalArgumentException(msg));
                    return;
                }

                members.add(endpoint);
                break;
            case ADD_OR_PROMOTE_TO_FOLLOWER:
                if (votingMembers.contains(endpoint)) {
                    String msg = endpoint + " is already a voting member in group " + raftNode.getGroupId();
                    future.fail(new IllegalArgumentException(msg));
                    return;
                }

                members.add(endpoint);
                votingMembers.add(endpoint);
                break;
            case REMOVE_MEMBER:
                if (!members.contains(endpoint)) {
                    String msg = endpoint + " does not exist in " + members + " of group " + raftNode.getGroupId();
                    future.fail(new IllegalArgumentException(msg));
                    return;
                } else if (votingMembers.size() == 1 && votingMembers.contains(state.localEndpoint())
                        && endpoint.equals(state.localEndpoint())) {
                    String msg = "Cannot remove " + endpoint + " from singleton group " + raftNode.getGroupId();
                    future.fail(new IllegalStateException(msg));
                    return;
                }

                members.remove(endpoint);
                votingMembers.remove(endpoint);
                break;
            default:
                future.fail(new IllegalArgumentException("Unknown membership change mode: " + membershipChangeMode));
                return;
            }

            LOGGER.info("{} New group members after {} of {}: {}, voting members: {}", raftNode.localEndpointStr(),
                    membershipChangeMode, endpoint.getId(), members.stream().map(RaftEndpoint::getId).collect(toList()),
                    votingMembers.stream().map(RaftEndpoint::getId).collect(toList()));

            RaftGroupOp operation = raftNode.getModelFactory().createUpdateRaftGroupMembersOpBuilder()
                    .setMembers(members).setVotingMembers(votingMembers).setEndpoint(endpoint)
                    .setMode(membershipChangeMode).build();
            new ReplicateTask(raftNode, operation, future).run();
        } catch (Throwable t) {
            LOGGER.error(raftNode.localEndpointStr() + " " + this + " failed.", t);
            future.fail(new RaftException("Internal failure", raftNode.getLeaderEndpoint(), t));
        }
    }

    private boolean verifyRaftNodeStatus() {
        RaftNodeStatus status = raftNode.getStatus();
        if (status == INITIAL) {
            LOGGER.error("{} Cannot {} {} with expected members commit index: {} since Raft node is not started.",
                    raftNode.localEndpointStr(), membershipChangeMode, endpoint.getId(), groupMembersCommitIndex);
            future.fail(new IllegalStateException("Cannot change group membership because Raft node not started"));
            return false;
        } else if (isTerminal(status)) {
            LOGGER.error("{} Cannot {} {} with expected members commit index: {} since Raft node is {}.",
                    raftNode.localEndpointStr(), membershipChangeMode, endpoint.getId(), groupMembersCommitIndex,
                    status);
            future.fail(raftNode.newNotLeaderException());
            return false;
        }

        return true;
    }

    private boolean verifyGroupMembersCommitIndex() {
        RaftGroupMembersState groupMembers = state.committedGroupMembers();
        if (groupMembers.getLogIndex() != groupMembersCommitIndex) {
            LOGGER.error(
                    "{} Cannot {} {} because expected members commit index: {} is different than group members commit"
                            + " index: {}",
                    raftNode.localEndpointStr(), membershipChangeMode, endpoint.getId(), groupMembersCommitIndex,
                    groupMembers.getLogIndex());
            Throwable t = new MismatchingRaftGroupMembersCommitIndexException(groupMembers.getLogIndex(),
                    groupMembers.getMembers());
            future.fail(t);
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
