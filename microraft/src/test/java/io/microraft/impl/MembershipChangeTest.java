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

package io.microraft.impl;

import io.microraft.Ordered;
import io.microraft.RaftConfig;
import io.microraft.RaftEndpoint;
import io.microraft.RaftNode;
import io.microraft.exception.CannotReplicateException;
import io.microraft.exception.IndeterminateStateException;
import io.microraft.exception.NotLeaderException;
import io.microraft.impl.local.LocalRaftGroup;
import io.microraft.impl.local.SimpleStateMachine;
import io.microraft.impl.state.RaftGroupMembersState;
import io.microraft.model.message.AppendEntriesFailureResponse;
import io.microraft.model.message.AppendEntriesRequest;
import io.microraft.model.message.AppendEntriesSuccessResponse;
import io.microraft.model.message.PreVoteRequest;
import io.microraft.model.message.VoteRequest;
import io.microraft.report.RaftGroupMembers;
import io.microraft.test.util.BaseTest;
import org.junit.After;
import org.junit.Test;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

import static io.microraft.MembershipChangeMode.ADD_LEARNER;
import static io.microraft.MembershipChangeMode.ADD_OR_PROMOTE_TO_FOLLOWER;
import static io.microraft.MembershipChangeMode.REMOVE_MEMBER;
import static io.microraft.RaftNodeStatus.ACTIVE;
import static io.microraft.RaftNodeStatus.TERMINATED;
import static io.microraft.RaftRole.FOLLOWER;
import static io.microraft.RaftRole.LEARNER;
import static io.microraft.impl.local.SimpleStateMachine.applyValue;
import static io.microraft.test.util.AssertionUtils.allTheTime;
import static io.microraft.test.util.AssertionUtils.eventually;
import static io.microraft.test.util.RaftTestUtils.TEST_RAFT_CONFIG;
import static io.microraft.test.util.RaftTestUtils.getCommitIndex;
import static io.microraft.test.util.RaftTestUtils.getCommittedGroupMembers;
import static io.microraft.test.util.RaftTestUtils.getEffectiveGroupMembers;
import static io.microraft.test.util.RaftTestUtils.getLastLogOrSnapshotEntry;
import static io.microraft.test.util.RaftTestUtils.getRole;
import static io.microraft.test.util.RaftTestUtils.getSnapshotEntry;
import static io.microraft.test.util.RaftTestUtils.getStatus;
import static io.microraft.test.util.RaftTestUtils.majority;
import static java.util.Arrays.asList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

public class MembershipChangeTest extends BaseTest {

    private LocalRaftGroup group;

    @After
    public void destroy() {
        if (group != null) {
            group.destroy();
        }
    }

    @Test(timeout = 300_000)
    public void when_newNodeJoinsAsFollower_then_itAppendsMissingEntries() {
        int initialMemberCount = 3;
        group = LocalRaftGroup.start(initialMemberCount);

        RaftNodeImpl leader = group.waitUntilLeaderElected();
        leader.replicate(applyValue("val")).join();

        RaftGroupMembersState initialMembers = leader.getInitialMembers();

        RaftNodeImpl newNode = group.createNewNode();

        Ordered<RaftGroupMembers> result = leader
                .changeMembership(newNode.getLocalEndpoint(), ADD_OR_PROMOTE_TO_FOLLOWER, 0).join();

        assertThat(result.getCommitIndex()).isEqualTo(getCommitIndex(leader));

        for (RaftGroupMembers groupMembers : asList(result.getResult(), leader.getCommittedMembers(),
                leader.getEffectiveMembers())) {
            assertThat(groupMembers.getMembers()).contains(newNode.getLocalEndpoint());
            assertThat(groupMembers.getVotingMembers()).contains(newNode.getLocalEndpoint());
            assertThat(groupMembers.getMajorityQuorumSize()).isEqualTo(1 + majority(initialMemberCount));
        }

        assertThat(leader.getInitialMembers().getMembers()).isEqualTo(initialMembers.getMembers());

        long commitIndex = getCommitIndex(leader);
        eventually(() -> assertThat(getCommitIndex(newNode)).isEqualTo(commitIndex));

        assertThat(getRole(newNode)).isEqualTo(FOLLOWER);

        RaftGroupMembersState effectiveGroupMembers = getEffectiveGroupMembers(leader);
        eventually(() -> {
            for (RaftNodeImpl node : group.getNodes()) {
                assertThat(getStatus(node)).isEqualTo(ACTIVE);
                assertThat(getEffectiveGroupMembers(node).getMembers()).isEqualTo(effectiveGroupMembers.getMembers());
                assertThat(getEffectiveGroupMembers(node).getLogIndex()).isEqualTo(effectiveGroupMembers.getLogIndex());
                assertThat(getCommittedGroupMembers(node).getMembers()).isEqualTo(effectiveGroupMembers.getMembers());
                assertThat(getCommittedGroupMembers(node).getLogIndex()).isEqualTo(effectiveGroupMembers.getLogIndex());
            }
        });

        SimpleStateMachine stateMachine = group.getStateMachine(newNode.getLocalEndpoint());
        assertThat(stateMachine.size()).isEqualTo(1);
        assertThat(stateMachine.valueSet()).contains("val");

        assertThat(newNode.getInitialMembers().getMembers()).isEqualTo(initialMembers.getMembers());
        assertThat(newNode.getCommittedMembers().getMembers()).isEqualTo(leader.getCommittedMembers().getMembers());
        assertThat(newNode.getCommittedMembers().getLogIndex()).isEqualTo(leader.getCommittedMembers().getLogIndex());
        assertThat(newNode.getCommittedMembers().getMajorityQuorumSize()).isEqualTo(1 + majority(initialMemberCount));
        assertThat(newNode.getEffectiveMembers().getMembers()).isEqualTo(leader.getEffectiveMembers().getMembers());
        assertThat(newNode.getEffectiveMembers().getLogIndex()).isEqualTo(leader.getEffectiveMembers().getLogIndex());
        assertThat(newNode.getEffectiveMembers().getMajorityQuorumSize()).isEqualTo(1 + majority(initialMemberCount));
    }

    @Test(timeout = 300_000)
    public void when_newNodeJoinsAsLearner_then_itAppendsMissingEntries() {
        int initialMemberCount = 3;
        group = LocalRaftGroup.start(initialMemberCount);

        RaftNodeImpl leader = group.waitUntilLeaderElected();
        leader.replicate(applyValue("val")).join();

        RaftGroupMembersState initialMembers = leader.getInitialMembers();

        RaftNodeImpl newNode = group.createNewNode();

        Ordered<RaftGroupMembers> result = leader.changeMembership(newNode.getLocalEndpoint(), ADD_LEARNER, 0).join();

        assertThat(result.getCommitIndex()).isEqualTo(getCommitIndex(leader));

        for (RaftGroupMembers groupMembers : asList(result.getResult(), leader.getCommittedMembers(),
                leader.getEffectiveMembers())) {
            assertThat(groupMembers.getMembers()).contains(newNode.getLocalEndpoint());
            assertThat(groupMembers.getVotingMembers()).doesNotContain(newNode.getLocalEndpoint());
            assertThat(groupMembers.getMajorityQuorumSize()).isEqualTo(majority(initialMemberCount));
        }

        assertThat(leader.getInitialMembers().getMembers()).isEqualTo(initialMembers.getMembers());

        long commitIndex = getCommitIndex(leader);
        eventually(() -> assertThat(getCommitIndex(newNode)).isEqualTo(commitIndex));

        assertThat(getRole(newNode)).isEqualTo(LEARNER);

        RaftGroupMembersState effectiveGroupMembers = getEffectiveGroupMembers(leader);
        eventually(() -> {
            for (RaftNodeImpl node : group.getNodes()) {
                assertThat(getStatus(node)).isEqualTo(ACTIVE);
                assertThat(getEffectiveGroupMembers(node).getMembers()).isEqualTo(effectiveGroupMembers.getMembers());
                assertThat(getEffectiveGroupMembers(node).getLogIndex()).isEqualTo(effectiveGroupMembers.getLogIndex());
                assertThat(getCommittedGroupMembers(node).getMembers()).isEqualTo(effectiveGroupMembers.getMembers());
                assertThat(getCommittedGroupMembers(node).getLogIndex()).isEqualTo(effectiveGroupMembers.getLogIndex());
            }
        });

        SimpleStateMachine stateMachine = group.getStateMachine(newNode.getLocalEndpoint());
        assertThat(stateMachine.size()).isEqualTo(1);
        assertThat(stateMachine.valueSet()).contains("val");

        assertThat(newNode.getInitialMembers().getMembers()).isEqualTo(initialMembers.getMembers());
        assertThat(newNode.getCommittedMembers().getMembers()).isEqualTo(leader.getCommittedMembers().getMembers());
        assertThat(newNode.getCommittedMembers().getLogIndex()).isEqualTo(leader.getCommittedMembers().getLogIndex());
        assertThat(newNode.getCommittedMembers().getMajorityQuorumSize()).isEqualTo(majority(initialMemberCount));
        assertThat(newNode.getEffectiveMembers().getMembers()).isEqualTo(leader.getEffectiveMembers().getMembers());
        assertThat(newNode.getEffectiveMembers().getLogIndex()).isEqualTo(leader.getEffectiveMembers().getLogIndex());
        assertThat(newNode.getEffectiveMembers().getMajorityQuorumSize()).isEqualTo(majority(initialMemberCount));
    }

    @Test(timeout = 300_000)
    public void when_thereIsSingleLearner_then_followerIsAdded() {
        int initialMemberCount = 3;
        group = LocalRaftGroup.start(initialMemberCount);

        RaftNodeImpl leader = group.waitUntilLeaderElected();
        leader.replicate(applyValue("val")).join();

        RaftGroupMembersState initialMembers = leader.getInitialMembers();

        RaftNodeImpl newNode1 = group.createNewNode();

        Ordered<RaftGroupMembers> result1 = leader.changeMembership(newNode1.getLocalEndpoint(), ADD_LEARNER, 0).join();

        assertThat(result1.getCommitIndex()).isEqualTo(getCommitIndex(leader));

        for (RaftGroupMembers groupMembers : asList(result1.getResult(), leader.getCommittedMembers(),
                leader.getEffectiveMembers())) {
            assertThat(groupMembers.getMembers()).contains(newNode1.getLocalEndpoint());
            assertThat(groupMembers.getVotingMembers()).doesNotContain(newNode1.getLocalEndpoint());
            assertThat(groupMembers.getMajorityQuorumSize()).isEqualTo(majority(initialMemberCount));
        }

        assertThat(result1.getResult().getMembers()).contains(newNode1.getLocalEndpoint());

        RaftNodeImpl newNode2 = group.createNewNode();

        Ordered<RaftGroupMembers> result2 = leader
                .changeMembership(newNode2.getLocalEndpoint(), ADD_OR_PROMOTE_TO_FOLLOWER, result1.getCommitIndex())
                .join();

        assertThat(result2.getCommitIndex()).isEqualTo(getCommitIndex(leader));

        for (RaftGroupMembers groupMembers : asList(result2.getResult(), leader.getCommittedMembers(),
                leader.getEffectiveMembers())) {
            assertThat(groupMembers.getMembers()).contains(newNode1.getLocalEndpoint());
            assertThat(groupMembers.getMembers()).contains(newNode2.getLocalEndpoint());
            assertThat(groupMembers.getVotingMembers()).doesNotContain(newNode1.getLocalEndpoint());
            assertThat(groupMembers.getVotingMembers()).contains(newNode2.getLocalEndpoint());
            assertThat(groupMembers.getMajorityQuorumSize()).isEqualTo(1 + majority(initialMemberCount));
        }

        long commitIndex = getCommitIndex(leader);
        eventually(() -> assertThat(getCommitIndex(newNode1)).isEqualTo(commitIndex));
        eventually(() -> assertThat(getCommitIndex(newNode2)).isEqualTo(commitIndex));

        assertThat(newNode1.getReport().join().getResult().getRole()).isEqualTo(LEARNER);
        assertThat(newNode2.getReport().join().getResult().getRole()).isEqualTo(FOLLOWER);

        RaftGroupMembersState effectiveGroupMembers = getEffectiveGroupMembers(leader);
        eventually(() -> {
            for (RaftNodeImpl node : group.getNodes()) {
                assertThat(getStatus(node)).isEqualTo(ACTIVE);
                assertThat(getEffectiveGroupMembers(node).getMembers()).isEqualTo(effectiveGroupMembers.getMembers());
                assertThat(getEffectiveGroupMembers(node).getLogIndex()).isEqualTo(effectiveGroupMembers.getLogIndex());
                assertThat(getCommittedGroupMembers(node).getMembers()).isEqualTo(effectiveGroupMembers.getMembers());
                assertThat(getCommittedGroupMembers(node).getLogIndex()).isEqualTo(effectiveGroupMembers.getLogIndex());
            }
        });

        for (RaftNodeImpl newNode : asList(newNode1, newNode2)) {
            SimpleStateMachine stateMachine = group.getStateMachine(newNode.getLocalEndpoint());
            assertThat(stateMachine.size()).isEqualTo(1);
            assertThat(stateMachine.valueSet()).contains("val");

            assertThat(newNode.getInitialMembers().getMembers()).isEqualTo(initialMembers.getMembers());
            assertThat(newNode.getCommittedMembers().getMembers()).isEqualTo(leader.getCommittedMembers().getMembers());
            assertThat(newNode.getCommittedMembers().getLogIndex())
                    .isEqualTo(leader.getCommittedMembers().getLogIndex());
            assertThat(newNode.getEffectiveMembers().getMembers()).isEqualTo(leader.getEffectiveMembers().getMembers());
            assertThat(newNode.getEffectiveMembers().getLogIndex())
                    .isEqualTo(leader.getEffectiveMembers().getLogIndex());
        }
    }

    @Test(timeout = 300_000)
    public void when_thereIsSingleLearner_then_secondLearnerIsAdded() {
        int initialMemberCount = 3;
        group = LocalRaftGroup.start(initialMemberCount);

        RaftNodeImpl leader = group.waitUntilLeaderElected();
        leader.replicate(applyValue("val")).join();

        RaftGroupMembersState initialMembers = leader.getInitialMembers();

        RaftNodeImpl newNode1 = group.createNewNode();

        Ordered<RaftGroupMembers> result1 = leader.changeMembership(newNode1.getLocalEndpoint(), ADD_LEARNER, 0).join();

        assertThat(result1.getCommitIndex()).isEqualTo(getCommitIndex(leader));

        for (RaftGroupMembers groupMembers : asList(result1.getResult(), leader.getCommittedMembers(),
                leader.getEffectiveMembers())) {
            assertThat(groupMembers.getMembers()).contains(newNode1.getLocalEndpoint());
            assertThat(groupMembers.getVotingMembers()).doesNotContain(newNode1.getLocalEndpoint());
            assertThat(groupMembers.getMajorityQuorumSize()).isEqualTo(majority(initialMemberCount));
        }

        assertThat(result1.getResult().getMembers()).contains(newNode1.getLocalEndpoint());

        RaftNodeImpl newNode2 = group.createNewNode();

        Ordered<RaftGroupMembers> result2 = leader
                .changeMembership(newNode2.getLocalEndpoint(), ADD_LEARNER, result1.getCommitIndex()).join();

        assertThat(result2.getCommitIndex()).isEqualTo(getCommitIndex(leader));

        for (RaftGroupMembers groupMembers : asList(result2.getResult(), leader.getCommittedMembers(),
                leader.getEffectiveMembers())) {
            assertThat(groupMembers.getMembers()).contains(newNode1.getLocalEndpoint());
            assertThat(groupMembers.getMembers()).contains(newNode2.getLocalEndpoint());
            assertThat(groupMembers.getVotingMembers()).doesNotContain(newNode1.getLocalEndpoint());
            assertThat(groupMembers.getVotingMembers()).doesNotContain(newNode2.getLocalEndpoint());
            assertThat(groupMembers.getMajorityQuorumSize()).isEqualTo(majority(initialMemberCount));
        }

        long commitIndex = getCommitIndex(leader);
        eventually(() -> assertThat(getCommitIndex(newNode1)).isEqualTo(commitIndex));
        eventually(() -> assertThat(getCommitIndex(newNode2)).isEqualTo(commitIndex));

        assertThat(newNode1.getReport().join().getResult().getRole()).isEqualTo(LEARNER);
        assertThat(newNode2.getReport().join().getResult().getRole()).isEqualTo(LEARNER);

        RaftGroupMembersState effectiveGroupMembers = getEffectiveGroupMembers(leader);
        eventually(() -> {
            for (RaftNodeImpl node : group.getNodes()) {
                assertThat(getStatus(node)).isEqualTo(ACTIVE);
                assertThat(getEffectiveGroupMembers(node).getMembers()).isEqualTo(effectiveGroupMembers.getMembers());
                assertThat(getEffectiveGroupMembers(node).getLogIndex()).isEqualTo(effectiveGroupMembers.getLogIndex());
                assertThat(getCommittedGroupMembers(node).getMembers()).isEqualTo(effectiveGroupMembers.getMembers());
                assertThat(getCommittedGroupMembers(node).getLogIndex()).isEqualTo(effectiveGroupMembers.getLogIndex());
            }
        });

        for (RaftNodeImpl newNode : asList(newNode1, newNode2)) {
            SimpleStateMachine stateMachine = group.getStateMachine(newNode.getLocalEndpoint());
            assertThat(stateMachine.size()).isEqualTo(1);
            assertThat(stateMachine.valueSet()).contains("val");

            assertThat(newNode.getInitialMembers().getMembers()).isEqualTo(initialMembers.getMembers());
            assertThat(newNode.getCommittedMembers().getMembers()).isEqualTo(leader.getCommittedMembers().getMembers());
            assertThat(newNode.getCommittedMembers().getLogIndex())
                    .isEqualTo(leader.getCommittedMembers().getLogIndex());
            assertThat(newNode.getEffectiveMembers().getMembers()).isEqualTo(leader.getEffectiveMembers().getMembers());
            assertThat(newNode.getEffectiveMembers().getLogIndex())
                    .isEqualTo(leader.getEffectiveMembers().getLogIndex());
        }
    }

    @Test(timeout = 300_000)
    public void when_thereAreTwoLearners_then_followerIsAdded() {
        int initialMemberCount = 3;
        group = LocalRaftGroup.start(initialMemberCount);

        RaftNodeImpl leader = group.waitUntilLeaderElected();
        leader.replicate(applyValue("val")).join();

        RaftNodeImpl newNode1 = group.createNewNode();

        Ordered<RaftGroupMembers> result1 = leader.changeMembership(newNode1.getLocalEndpoint(), ADD_LEARNER, 0).join();

        RaftNodeImpl newNode2 = group.createNewNode();

        Ordered<RaftGroupMembers> result2 = leader
                .changeMembership(newNode2.getLocalEndpoint(), ADD_LEARNER, result1.getCommitIndex()).join();

        RaftNodeImpl newNode3 = group.createNewNode();

        Ordered<RaftGroupMembers> result3 = leader
                .changeMembership(newNode3.getLocalEndpoint(), ADD_OR_PROMOTE_TO_FOLLOWER, result2.getCommitIndex())
                .join();

        assertThat(result3.getCommitIndex()).isEqualTo(getCommitIndex(leader));

        for (RaftGroupMembers groupMembers : asList(result3.getResult(), leader.getCommittedMembers(),
                leader.getEffectiveMembers())) {
            assertThat(groupMembers.getMembers()).contains(newNode1.getLocalEndpoint());
            assertThat(groupMembers.getMembers()).contains(newNode2.getLocalEndpoint());
            assertThat(groupMembers.getMembers()).contains(newNode3.getLocalEndpoint());
            assertThat(groupMembers.getVotingMembers()).doesNotContain(newNode1.getLocalEndpoint());
            assertThat(groupMembers.getVotingMembers()).doesNotContain(newNode2.getLocalEndpoint());
            assertThat(groupMembers.getVotingMembers()).contains(newNode3.getLocalEndpoint());
            assertThat(groupMembers.getMajorityQuorumSize()).isEqualTo(1 + majority(initialMemberCount));
        }

        long commitIndex = getCommitIndex(leader);
        eventually(() -> assertThat(getCommitIndex(newNode1)).isEqualTo(commitIndex));
        eventually(() -> assertThat(getCommitIndex(newNode2)).isEqualTo(commitIndex));
        eventually(() -> assertThat(getCommitIndex(newNode3)).isEqualTo(commitIndex));

        assertThat(newNode1.getReport().join().getResult().getRole()).isEqualTo(LEARNER);
        assertThat(newNode2.getReport().join().getResult().getRole()).isEqualTo(LEARNER);
        assertThat(newNode3.getReport().join().getResult().getRole()).isEqualTo(FOLLOWER);

        RaftGroupMembersState effectiveGroupMembers = getEffectiveGroupMembers(leader);
        eventually(() -> {
            for (RaftNodeImpl node : group.getNodes()) {
                assertThat(getStatus(node)).isEqualTo(ACTIVE);
                assertThat(getEffectiveGroupMembers(node).getMembers()).isEqualTo(effectiveGroupMembers.getMembers());
                assertThat(getEffectiveGroupMembers(node).getLogIndex()).isEqualTo(effectiveGroupMembers.getLogIndex());
                assertThat(getCommittedGroupMembers(node).getMembers()).isEqualTo(effectiveGroupMembers.getMembers());
                assertThat(getCommittedGroupMembers(node).getLogIndex()).isEqualTo(effectiveGroupMembers.getLogIndex());
            }
        });

        for (RaftNodeImpl newNode : asList(newNode1, newNode2, newNode3)) {
            SimpleStateMachine stateMachine = group.getStateMachine(newNode.getLocalEndpoint());
            assertThat(stateMachine.size()).isEqualTo(1);
            assertThat(stateMachine.valueSet()).contains("val");

            assertThat(newNode.getCommittedMembers().getMembers()).isEqualTo(leader.getCommittedMembers().getMembers());
            assertThat(newNode.getCommittedMembers().getLogIndex())
                    .isEqualTo(leader.getCommittedMembers().getLogIndex());
            assertThat(newNode.getEffectiveMembers().getMembers()).isEqualTo(leader.getEffectiveMembers().getMembers());
            assertThat(newNode.getEffectiveMembers().getLogIndex())
                    .isEqualTo(leader.getEffectiveMembers().getLogIndex());
        }
    }

    @Test(timeout = 300_000)
    public void when_thereAreTwoLearners_then_thirdLearnerCannotBeAdded() {
        int initialMemberCount = 3;
        group = LocalRaftGroup.start(initialMemberCount);

        RaftNodeImpl leader = group.waitUntilLeaderElected();
        leader.replicate(applyValue("val")).join();

        RaftNodeImpl newNode1 = group.createNewNode();

        Ordered<RaftGroupMembers> result1 = leader.changeMembership(newNode1.getLocalEndpoint(), ADD_LEARNER, 0).join();

        RaftNodeImpl newNode2 = group.createNewNode();

        Ordered<RaftGroupMembers> result2 = leader
                .changeMembership(newNode2.getLocalEndpoint(), ADD_LEARNER, result1.getCommitIndex()).join();

        RaftNodeImpl newNode3 = group.createNewNode();

        try {
            leader.changeMembership(newNode3.getLocalEndpoint(), ADD_LEARNER, result2.getCommitIndex()).join();
            fail();
        } catch (CompletionException e) {
            assertThat(e).hasCauseInstanceOf(IllegalArgumentException.class);
        }
    }

    @Test(timeout = 300_000)
    public void when_learnerIsPromoted_then_majorityIsUpdated() {
        int initialMemberCount = 3;
        group = LocalRaftGroup.start(initialMemberCount);

        RaftNodeImpl leader = group.waitUntilLeaderElected();
        leader.replicate(applyValue("val")).join();

        RaftNodeImpl newNode = group.createNewNode();

        Ordered<RaftGroupMembers> result1 = leader.changeMembership(newNode.getLocalEndpoint(), ADD_LEARNER, 0).join();

        Ordered<RaftGroupMembers> result2 = leader
                .changeMembership(newNode.getLocalEndpoint(), ADD_OR_PROMOTE_TO_FOLLOWER, result1.getCommitIndex())
                .join();

        int newMajority = 1 + majority(initialMemberCount);

        for (RaftGroupMembers groupMembers : asList(result2.getResult(), leader.getCommittedMembers(),
                leader.getEffectiveMembers())) {
            assertThat(groupMembers.getMembers()).contains(newNode.getLocalEndpoint());
            assertThat(groupMembers.getVotingMembers()).contains(newNode.getLocalEndpoint());
            assertThat(groupMembers.getMajorityQuorumSize()).isEqualTo(newMajority);
        }

        eventually(() -> assertThat(getCommitIndex(newNode)).isEqualTo(getCommitIndex(leader)));
        assertThat(getRole(newNode)).isEqualTo(FOLLOWER);
    }

    @Test(timeout = 300_000)
    public void when_learnerIsPromoted_then_newLearnerCanBeAdded() {
        int initialMemberCount = 3;
        group = LocalRaftGroup.start(initialMemberCount);

        RaftNodeImpl leader = group.waitUntilLeaderElected();
        leader.replicate(applyValue("val")).join();

        RaftNodeImpl newNode1 = group.createNewNode();

        Ordered<RaftGroupMembers> result1 = leader.changeMembership(newNode1.getLocalEndpoint(), ADD_LEARNER, 0).join();

        Ordered<RaftGroupMembers> result2 = leader
                .changeMembership(newNode1.getLocalEndpoint(), ADD_OR_PROMOTE_TO_FOLLOWER, result1.getCommitIndex())
                .join();

        int newMajority = 1 + majority(initialMemberCount);

        RaftNodeImpl newNode2 = group.createNewNode();

        Ordered<RaftGroupMembers> result3 = leader
                .changeMembership(newNode2.getLocalEndpoint(), ADD_LEARNER, result2.getCommitIndex()).join();

        for (RaftGroupMembers groupMembers : asList(result3.getResult(), leader.getCommittedMembers(),
                leader.getEffectiveMembers())) {
            assertThat(groupMembers.getMembers()).contains(newNode1.getLocalEndpoint());
            assertThat(groupMembers.getMembers()).contains(newNode2.getLocalEndpoint());
            assertThat(groupMembers.getVotingMembers()).contains(newNode1.getLocalEndpoint());
            assertThat(groupMembers.getVotingMembers()).doesNotContain(newNode2.getLocalEndpoint());
            assertThat(groupMembers.getMajorityQuorumSize()).isEqualTo(newMajority);
        }

        eventually(() -> assertThat(getCommitIndex(newNode2)).isEqualTo(getCommitIndex(leader)));
        assertThat(getRole(newNode2)).isEqualTo(LEARNER);
    }

    @Test(timeout = 300_000)
    public void when_secondLearnerIsPromoted_then_majorityDoesNotChange() {
        int initialMemberCount = 3;
        group = LocalRaftGroup.start(initialMemberCount);

        RaftNodeImpl leader = group.waitUntilLeaderElected();
        leader.replicate(applyValue("val")).join();

        RaftNodeImpl newNode1 = group.createNewNode();

        Ordered<RaftGroupMembers> result1 = leader.changeMembership(newNode1.getLocalEndpoint(), ADD_LEARNER, 0).join();

        RaftNodeImpl newNode2 = group.createNewNode();

        Ordered<RaftGroupMembers> result2 = leader
                .changeMembership(newNode2.getLocalEndpoint(), ADD_LEARNER, result1.getCommitIndex()).join();

        Ordered<RaftGroupMembers> result3 = leader
                .changeMembership(newNode1.getLocalEndpoint(), ADD_OR_PROMOTE_TO_FOLLOWER, result2.getCommitIndex())
                .join();

        Ordered<RaftGroupMembers> result4 = leader
                .changeMembership(newNode2.getLocalEndpoint(), ADD_OR_PROMOTE_TO_FOLLOWER, result3.getCommitIndex())
                .join();

        int newMajority = majority(initialMemberCount + 2);

        for (RaftGroupMembers groupMembers : asList(result4.getResult(), leader.getCommittedMembers(),
                leader.getEffectiveMembers())) {
            assertThat(groupMembers.getMembers()).contains(newNode1.getLocalEndpoint());
            assertThat(groupMembers.getVotingMembers()).contains(newNode1.getLocalEndpoint());
            assertThat(groupMembers.getMembers()).contains(newNode2.getLocalEndpoint());
            assertThat(groupMembers.getVotingMembers()).contains(newNode2.getLocalEndpoint());
            assertThat(groupMembers.getMajorityQuorumSize()).isEqualTo(newMajority);
        }

        eventually(() -> assertThat(getCommitIndex(newNode1)).isEqualTo(getCommitIndex(leader)));
        eventually(() -> assertThat(getCommitIndex(newNode2)).isEqualTo(getCommitIndex(leader)));
        assertThat(getRole(newNode1)).isEqualTo(FOLLOWER);
        assertThat(getRole(newNode2)).isEqualTo(FOLLOWER);
    }

    @Test(timeout = 300_000)
    public void when_followerIsPromoted_then_promotionFails() {
        int initialMemberCount = 3;
        group = LocalRaftGroup.start(initialMemberCount);

        RaftNodeImpl leader = group.waitUntilLeaderElected();

        try {
            leader.changeMembership(leader.getLocalEndpoint(), ADD_OR_PROMOTE_TO_FOLLOWER, 0).join();
            fail();
        } catch (CompletionException e) {
            assertThat(e).hasCauseInstanceOf(IllegalArgumentException.class);
        }
    }

    @Test(timeout = 300_000)
    public void when_leaderFailsBeforeCommittingPromotion_then_followersElectNewLeaderViaGettingVoteFromLearner() {
        int initialMemberCount = 3;
        group = LocalRaftGroup.newBuilder(initialMemberCount).enableNewTermOperation().setConfig(
                RaftConfig.newBuilder().setLeaderHeartbeatTimeoutSecs(3).setLeaderHeartbeatPeriodSecs(1).build())
                .start();

        RaftNodeImpl leader = group.waitUntilLeaderElected();

        List<RaftNodeImpl> followers = group.getNodesExcept(leader.getLocalEndpoint());

        RaftNodeImpl newNode = group.createNewNode();

        Ordered<RaftGroupMembers> result = leader.changeMembership(newNode.getLocalEndpoint(), ADD_LEARNER, 0).join();

        eventually(() -> {
            for (RaftNodeImpl node : group.getNodes()) {
                assertThat(getCommitIndex(node)).isEqualTo(getCommitIndex(leader));
            }
        });

        for (RaftNodeImpl follower : followers) {
            group.dropMessagesTo(follower.getLocalEndpoint(), leader.getLocalEndpoint(),
                    AppendEntriesSuccessResponse.class);
            group.dropMessagesTo(follower.getLocalEndpoint(), leader.getLocalEndpoint(),
                    AppendEntriesFailureResponse.class);
        }

        group.dropMessagesTo(leader.getLocalEndpoint(), newNode.getLocalEndpoint(), AppendEntriesRequest.class);

        leader.changeMembership(newNode.getLocalEndpoint(), ADD_OR_PROMOTE_TO_FOLLOWER, result.getCommitIndex());

        eventually(() -> {
            assertThat(leader.getEffectiveMembers().getVotingMembers()).contains(newNode.getLocalEndpoint());
            for (RaftNodeImpl follower : followers) {
                assertThat(follower.getEffectiveMembers().getVotingMembers()).contains(newNode.getLocalEndpoint());
            }
        });

        group.terminateNode(leader.getLocalEndpoint());

        group.waitUntilLeaderElected();

        eventually(() -> {
            for (RaftNodeImpl node : group.getNodes()) {
                assertThat(node.getEffectiveMembers().getLogIndex())
                        .isEqualTo(node.getCommittedMembers().getLogIndex());
                assertThat(node.getEffectiveMembers().getVotingMembers()).contains(newNode.getLocalEndpoint());
            }
        });

        assertThat(getRole(newNode)).isEqualTo(FOLLOWER);
    }

    @Test(timeout = 300_000)
    public void when_initialMemberIsRemoved_then_itCannotBeAddedAgain() {
        group = LocalRaftGroup.start(3);

        RaftNodeImpl leader = group.waitUntilLeaderElected();
        RaftNodeImpl leavingFollower = group.getAnyNodeExcept(leader.getLocalEndpoint());

        leader.replicate(applyValue("val")).join();

        Ordered<RaftGroupMembers> result = leader.changeMembership(leavingFollower.getLocalEndpoint(), REMOVE_MEMBER, 0)
                .join();

        group.terminateNode(leavingFollower.getLocalEndpoint());

        try {
            leader.changeMembership(leavingFollower.getLocalEndpoint(), ADD_LEARNER, result.getCommitIndex()).join();
            fail();
        } catch (CompletionException e) {
            assertThat(e).hasCauseInstanceOf(IllegalArgumentException.class);
        }
    }

    @Test(timeout = 300_000)
    public void when_followerLeaves_then_itIsRemovedFromTheGroupMembers() {
        group = LocalRaftGroup.start(3);

        RaftNodeImpl leader = group.waitUntilLeaderElected();
        List<RaftNodeImpl> followers = group.getNodesExcept(leader.getLocalEndpoint());
        RaftNodeImpl leavingFollower = followers.get(0);
        RaftNodeImpl stayingFollower = followers.get(1);

        leader.replicate(applyValue("val")).join();

        Ordered<RaftGroupMembers> result = leader.changeMembership(leavingFollower.getLocalEndpoint(), REMOVE_MEMBER, 0)
                .join();

        assertThat(result.getResult().getMembers()).doesNotContain(leavingFollower.getLocalEndpoint());

        eventually(() -> {
            for (RaftNodeImpl node : asList(leader, stayingFollower)) {
                assertThat(getEffectiveGroupMembers(node).isKnownMember(leavingFollower.getLocalEndpoint())).isFalse();
                assertThat(getCommittedGroupMembers(node).isKnownMember(leavingFollower.getLocalEndpoint())).isFalse();
            }
        });

        group.terminateNode(leavingFollower.getLocalEndpoint());
    }

    @Test(timeout = 300_000)
    public void when_newNodeJoinsAfterAnotherNodeLeaves_then_itAppendsMissingEntries() {
        group = LocalRaftGroup.start(3);

        RaftNodeImpl leader = group.waitUntilLeaderElected();
        leader.replicate(applyValue("val")).join();

        List<RaftNodeImpl> followers = group.getNodesExcept(leader.getLocalEndpoint());
        RaftNodeImpl leavingFollower = followers.get(0);
        RaftNodeImpl stayingFollower = followers.get(1);

        long newMembersCommitIndex = leader.changeMembership(leavingFollower.getLocalEndpoint(), REMOVE_MEMBER, 0)
                .join().getCommitIndex();

        RaftNodeImpl newNode = group.createNewNode();

        leader.changeMembership(newNode.getLocalEndpoint(), ADD_LEARNER, newMembersCommitIndex).join();

        long commitIndex = getCommitIndex(leader);
        eventually(() -> assertThat(getCommitIndex(newNode)).isEqualTo(commitIndex));

        RaftGroupMembersState effectiveGroupMembers = getEffectiveGroupMembers(leader);
        eventually(() -> {
            for (RaftNodeImpl node : asList(leader, stayingFollower, newNode)) {
                assertThat(getStatus(node)).isEqualTo(ACTIVE);
                assertThat(getEffectiveGroupMembers(node).getMembers()).isEqualTo(effectiveGroupMembers.getMembers());
                assertThat(getEffectiveGroupMembers(node).getLogIndex()).isEqualTo(effectiveGroupMembers.getLogIndex());
                assertThat(getCommittedGroupMembers(node).getMembers()).isEqualTo(effectiveGroupMembers.getMembers());
                assertThat(getCommittedGroupMembers(node).getLogIndex()).isEqualTo(effectiveGroupMembers.getLogIndex());
                assertThat(getEffectiveGroupMembers(node).isKnownMember(leavingFollower.getLocalEndpoint())).isFalse();
                assertThat(getCommittedGroupMembers(node).isKnownMember(leavingFollower.getLocalEndpoint())).isFalse();
            }
        });

        SimpleStateMachine stateMachine = group.getStateMachine(newNode.getLocalEndpoint());
        assertThat(stateMachine.size()).isEqualTo(1);
        assertThat(stateMachine.valueSet()).contains("val");
    }

    @Test(timeout = 300_000)
    public void when_newNodeJoinsAfterAnotherNodeLeavesAndSnapshotIsTaken_then_itAppendsMissingEntries() {
        int commitCountToTakeSnapshot = 10;
        RaftConfig config = RaftConfig.newBuilder().setCommitCountToTakeSnapshot(commitCountToTakeSnapshot).build();
        group = LocalRaftGroup.start(3, config);

        RaftNodeImpl leader = group.waitUntilLeaderElected();

        leader.replicate(applyValue("val")).join();

        List<RaftNodeImpl> followers = group.getNodesExcept(leader.getLocalEndpoint());
        RaftNodeImpl leavingFollower = followers.get(0);
        RaftNodeImpl stayingFollower = followers.get(1);

        long newMembersIndex = leader.changeMembership(leavingFollower.getLocalEndpoint(), REMOVE_MEMBER, 0).join()
                .getCommitIndex();

        for (int i = 0; i < commitCountToTakeSnapshot; i++) {
            leader.replicate(applyValue("val" + i)).join();
        }

        eventually(() -> assertThat(getSnapshotEntry(leader).getIndex()).isGreaterThan(0));

        RaftNodeImpl newNode = group.createNewNode();

        leader.changeMembership(newNode.getLocalEndpoint(), ADD_LEARNER, newMembersIndex).join();

        long commitIndex = getCommitIndex(leader);
        eventually(() -> assertThat(getCommitIndex(newNode)).isEqualTo(commitIndex));

        RaftGroupMembersState effectiveGroupMembers = getEffectiveGroupMembers(leader);
        eventually(() -> {
            for (RaftNodeImpl node : asList(leader, stayingFollower, newNode)) {
                assertThat(getStatus(node)).isEqualTo(ACTIVE);
                assertThat(getEffectiveGroupMembers(node).getMembers()).isEqualTo(effectiveGroupMembers.getMembers());
                assertThat(getEffectiveGroupMembers(node).getLogIndex()).isEqualTo(effectiveGroupMembers.getLogIndex());
                assertThat(getCommittedGroupMembers(node).getMembers()).isEqualTo(effectiveGroupMembers.getMembers());
                assertThat(getCommittedGroupMembers(node).getLogIndex()).isEqualTo(effectiveGroupMembers.getLogIndex());
                assertThat(getEffectiveGroupMembers(node).isKnownMember(leavingFollower.getLocalEndpoint())).isFalse();
                assertThat(getCommittedGroupMembers(node).isKnownMember(leavingFollower.getLocalEndpoint())).isFalse();
            }
        });

        SimpleStateMachine stateMachine = group.getStateMachine(newNode.getLocalEndpoint());
        assertThat(stateMachine.size()).isEqualTo(commitCountToTakeSnapshot + 1);
        assertThat(stateMachine.valueSet()).contains("val");
        for (int i = 0; i < commitCountToTakeSnapshot; i++) {
            assertThat(stateMachine.valueSet()).contains("val" + i);
        }
    }

    @Test(timeout = 300_000)
    public void when_leaderLeaves_then_itIsRemovedFromTheGroupMembers() {
        group = LocalRaftGroup.start(3);

        RaftNodeImpl leader = group.waitUntilLeaderElected();

        leader.replicate(applyValue("val")).join();
        leader.changeMembership(leader.getLocalEndpoint(), REMOVE_MEMBER, 0).join();

        assertThat(leader.getStatus()).isEqualTo(TERMINATED);

        eventually(() -> {
            for (RaftNodeImpl node : group.<RaftNodeImpl>getNodesExcept(leader.getLocalEndpoint())) {
                assertThat(getEffectiveGroupMembers(node).isKnownMember(leader.getLocalEndpoint())).isFalse();
                assertThat(getCommittedGroupMembers(node).isKnownMember(leader.getLocalEndpoint())).isFalse();
            }
        });
    }

    @Test(timeout = 300_000)
    public void when_leaderLeaves_then_itCannotVoteForCommitOfMemberChange() {
        RaftConfig config = RaftConfig.newBuilder().setLeaderHeartbeatPeriodSecs(1).build();
        group = LocalRaftGroup.start(3, config);

        RaftNodeImpl leader = group.waitUntilLeaderElected();
        RaftNodeImpl follower = group.getAnyNodeExcept(leader.getLocalEndpoint());

        group.dropMessagesTo(follower.getLocalEndpoint(), leader.getLocalEndpoint(),
                AppendEntriesSuccessResponse.class);
        leader.replicate(applyValue("val")).join();

        leader.changeMembership(leader.getLocalEndpoint(), REMOVE_MEMBER, 0);

        allTheTime(() -> assertThat(getCommitIndex(leader)).isEqualTo(1), 5);
    }

    @Test(timeout = 300_000)
    public void when_leaderLeaves_then_followersElectNewLeader() {
        group = LocalRaftGroup.start(3, TEST_RAFT_CONFIG);

        RaftNodeImpl leader = group.waitUntilLeaderElected();
        List<RaftNodeImpl> followers = group.getNodesExcept(leader.getLocalEndpoint());

        leader.replicate(applyValue("val")).join();
        leader.changeMembership(leader.getLocalEndpoint(), REMOVE_MEMBER, 0).join();

        eventually(() -> {
            for (RaftNodeImpl node : followers) {
                assertThat(getEffectiveGroupMembers(node).isKnownMember(leader.getLocalEndpoint())).isFalse();
                assertThat(getCommittedGroupMembers(node).isKnownMember(leader.getLocalEndpoint())).isFalse();
            }
        });

        group.terminateNode(leader.getLocalEndpoint());

        eventually(() -> {
            for (RaftNodeImpl node : followers) {
                RaftEndpoint newLeader = node.getLeaderEndpoint();
                assertThat(newLeader).isNotNull().isNotEqualTo(leader.getLocalEndpoint());
            }
        });
    }

    @Test(timeout = 300_000)
    public void when_membershipChangeRequestIsMadeWithInvalidType_then_membershipChangeFails() {
        group = LocalRaftGroup.start(3);

        RaftNodeImpl leader = group.waitUntilLeaderElected();
        leader.replicate(applyValue("val")).join();

        try {
            leader.changeMembership(leader.getLocalEndpoint(), null, 0);
            fail();
        } catch (NullPointerException ignored) {
        }
    }

    @Test(timeout = 300_000)
    public void when_nonExistingEndpointIsRemoved_then_membershipChangeFails() {
        group = LocalRaftGroup.start(3);

        RaftNodeImpl leader = group.waitUntilLeaderElected();
        RaftNodeImpl leavingFollower = group.getAnyNodeExcept(leader.getLocalEndpoint());

        leader.replicate(applyValue("val")).join();
        long newMembersIndex = leader.changeMembership(leavingFollower.getLocalEndpoint(), REMOVE_MEMBER, 0).join()
                .getCommitIndex();

        try {
            leader.changeMembership(leavingFollower.getLocalEndpoint(), REMOVE_MEMBER, newMembersIndex).join();
            fail();
        } catch (CompletionException e) {
            assertThat(e).hasCauseInstanceOf(IllegalArgumentException.class);
        }
    }

    @Test(timeout = 300_000)
    public void when_existingEndpointIsAdded_then_membershipChangeFails() {
        group = LocalRaftGroup.start(3);

        RaftNodeImpl leader = group.waitUntilLeaderElected();

        leader.replicate(applyValue("val")).join();

        try {
            leader.changeMembership(leader.getLocalEndpoint(), ADD_LEARNER, 0).join();
            fail();
        } catch (CompletionException e) {
            assertThat(e).hasCauseInstanceOf(IllegalArgumentException.class);
        }
    }

    @Test(timeout = 300_000)
    public void when_thereIsNoCommitInTheCurrentTerm_then_cannotMakeMemberChange() {
        // https://groups.google.com/forum/#!msg/raft-dev/t4xj6dJTP6E/d2D9LrWRza8J

        group = LocalRaftGroup.start(3);

        RaftNodeImpl leader = group.waitUntilLeaderElected();

        try {
            leader.changeMembership(leader.getLocalEndpoint(), REMOVE_MEMBER, 0).join();
            fail();
        } catch (CompletionException e) {
            assertThat(e).hasCauseInstanceOf(CannotReplicateException.class);
        }
    }

    @Test(timeout = 300_000)
    public void when_appendNopEntryOnLeaderElection_then_canMakeMemberChangeAfterNopEntryCommitted() {
        // https://groups.google.com/forum/#!msg/raft-dev/t4xj6dJTP6E/d2D9LrWRza8J

        group = LocalRaftGroup.newBuilder(3).enableNewTermOperation().start();

        RaftNodeImpl leader = group.waitUntilLeaderElected();

        eventually(() -> {
            // may fail until nop-entry is committed
            try {
                leader.changeMembership(leader.getLocalEndpoint(), REMOVE_MEMBER, 0).join();
            } catch (CompletionException e) {
                if (e.getCause() instanceof CannotReplicateException) {
                    fail();
                }

                throw e;
            }
        });
    }

    @Test(timeout = 300_000)
    public void when_newJoiningNodeFirstReceivesSnapshot_then_itInstallsSnapshot() {
        RaftConfig config = RaftConfig.newBuilder().setCommitCountToTakeSnapshot(5).build();
        group = LocalRaftGroup.start(3, config);

        RaftNodeImpl leader = group.waitUntilLeaderElected();
        for (int i = 0; i < 4; i++) {
            leader.replicate(applyValue("val" + i)).join();
        }

        RaftNodeImpl newNode = group.createNewNode();

        group.dropMessagesTo(leader.getLocalEndpoint(), newNode.getLocalEndpoint(), AppendEntriesRequest.class);

        leader.changeMembership(newNode.getLocalEndpoint(), ADD_LEARNER, 0).join();

        eventually(() -> assertThat(getSnapshotEntry(leader).getIndex()).isGreaterThan(0));

        group.resetAllRulesFrom(leader.getLocalEndpoint());

        eventually(() -> {
            assertThat(getCommitIndex(newNode)).isEqualTo(getCommitIndex(leader));
            assertThat(getEffectiveGroupMembers(newNode).getMembers())
                    .isEqualTo(getEffectiveGroupMembers(leader).getMembers());
            assertThat(getCommittedGroupMembers(newNode).getMembers())
                    .isEqualTo(getEffectiveGroupMembers(leader).getMembers());
            SimpleStateMachine stateMachine = group.getStateMachine(newNode.getLocalEndpoint());
            assertThat(stateMachine.size()).isEqualTo(4);
        });
    }

    @Test(timeout = 300_000)
    public void when_leaderFailsWhileLeavingRaftGroup_othersCommitTheMembershipChange() {
        group = LocalRaftGroup.start(3, TEST_RAFT_CONFIG);

        RaftNodeImpl leader = group.waitUntilLeaderElected();
        List<RaftNodeImpl> followers = group.getNodesExcept(leader.getLocalEndpoint());

        leader.replicate(applyValue("val")).join();

        for (RaftNodeImpl follower : followers) {
            group.dropMessagesTo(follower.getLocalEndpoint(), leader.getLocalEndpoint(),
                    AppendEntriesSuccessResponse.class);
            group.dropMessagesTo(follower.getLocalEndpoint(), leader.getLocalEndpoint(),
                    AppendEntriesFailureResponse.class);
        }

        leader.changeMembership(leader.getLocalEndpoint(), REMOVE_MEMBER, 0);

        eventually(() -> {
            for (RaftNodeImpl follower : followers) {
                assertThat(getLastLogOrSnapshotEntry(follower).getIndex()).isEqualTo(2);
            }
        });

        group.terminateNode(leader.getLocalEndpoint());

        eventually(() -> {
            for (RaftNodeImpl follower : followers) {
                RaftEndpoint newLeaderEndpoint = follower.getLeaderEndpoint();
                assertThat(newLeaderEndpoint).isNotNull().isNotEqualTo(leader.getLocalEndpoint());
            }
        });

        RaftNodeImpl newLeader = group.getNode(followers.get(0).getLeaderEndpoint());
        newLeader.replicate(applyValue("val2"));

        eventually(() -> {
            for (RaftNodeImpl follower : followers) {
                assertThat(getCommittedGroupMembers(follower).isKnownMember(leader.getLocalEndpoint())).isFalse();
            }
        });
    }

    @Test(timeout = 300_000)
    public void when_followerAppendsMultipleMembershipChangesAtOnce_then_itCommitsThemCorrectly() {
        RaftConfig config = RaftConfig.newBuilder().setLeaderHeartbeatPeriodSecs(1).build();
        group = LocalRaftGroup.start(5, config);

        RaftNodeImpl leader = group.waitUntilLeaderElected();
        List<RaftNodeImpl> followers = group.getNodesExcept(leader.getLocalEndpoint());

        leader.replicate(applyValue("val")).join();

        eventually(() -> {
            for (RaftNodeImpl follower : followers) {
                assertThat(getCommitIndex(follower)).isEqualTo(1);
            }
        });

        RaftNodeImpl slowFollower = followers.get(0);

        for (RaftNodeImpl follower : followers) {
            if (follower != slowFollower) {
                group.dropMessagesTo(follower.getLocalEndpoint(), follower.getLeaderEndpoint(),
                        AppendEntriesSuccessResponse.class);
                group.dropMessagesTo(follower.getLocalEndpoint(), follower.getLeaderEndpoint(),
                        AppendEntriesFailureResponse.class);
            }
        }

        RaftNodeImpl newNode1 = group.createNewNode();
        group.dropMessagesTo(leader.getLocalEndpoint(), newNode1.getLocalEndpoint(), AppendEntriesRequest.class);
        CompletableFuture<Ordered<RaftGroupMembers>> f1 = leader.changeMembership(newNode1.getLocalEndpoint(),
                ADD_LEARNER, 0);

        eventually(() -> {
            for (RaftNodeImpl follower : followers) {
                assertThat(getLastLogOrSnapshotEntry(follower).getIndex()).isEqualTo(2);
            }
        });

        group.dropMessagesTo(leader.getLocalEndpoint(), slowFollower.getLocalEndpoint(), AppendEntriesRequest.class);

        for (RaftNodeImpl follower : followers) {
            if (follower != slowFollower) {
                group.allowAllMessagesTo(follower.getLocalEndpoint(), leader.getLeaderEndpoint());
            }
        }

        long newMembersIndex = f1.join().getCommitIndex();
        eventually(() -> {
            for (RaftNodeImpl follower : followers) {
                if (follower != slowFollower) {
                    assertThat(getCommittedGroupMembers(follower).getMembers()).hasSize(6);
                } else {
                    assertThat(getCommittedGroupMembers(follower).getMembers()).hasSize(5);
                    assertThat(getEffectiveGroupMembers(follower).getMembers()).hasSize(6);
                }
            }
        });

        RaftNodeImpl newNode2 = group.createNewNode();
        leader.changeMembership(newNode2.getLocalEndpoint(), ADD_LEARNER, newMembersIndex).join();

        group.allowAllMessagesTo(leader.getLocalEndpoint(), slowFollower.getLocalEndpoint());
        group.allowAllMessagesTo(slowFollower.getLocalEndpoint(), leader.getLocalEndpoint());
        group.allowAllMessagesTo(leader.getLocalEndpoint(), newNode1.getLocalEndpoint());

        RaftGroupMembersState leaderCommittedGroupMembers = getCommittedGroupMembers(leader);
        eventually(() -> {
            assertThat(getCommittedGroupMembers(slowFollower).getLogIndex())
                    .isEqualTo(leaderCommittedGroupMembers.getLogIndex());
            assertThat(getCommittedGroupMembers(newNode1).getLogIndex())
                    .isEqualTo(leaderCommittedGroupMembers.getLogIndex());
            assertThat(getCommittedGroupMembers(newNode2).getLogIndex())
                    .isEqualTo(leaderCommittedGroupMembers.getLogIndex());
        });
    }

    @Test(timeout = 300_000)
    public void when_leaderIsSteppingDown_then_itDoesNotAcceptNewAppends() {
        group = LocalRaftGroup.newBuilder(3).enableNewTermOperation().start();

        RaftNodeImpl leader = group.waitUntilLeaderElected();
        List<RaftNodeImpl> followers = group.getNodesExcept(leader.getLocalEndpoint());
        group.dropMessagesTo(leader.getLocalEndpoint(), followers.get(0).getLocalEndpoint(),
                AppendEntriesRequest.class);
        group.dropMessagesTo(leader.getLocalEndpoint(), followers.get(1).getLocalEndpoint(),
                AppendEntriesRequest.class);

        CompletableFuture<Ordered<RaftGroupMembers>> f1 = leader.changeMembership(leader.getLocalEndpoint(),
                REMOVE_MEMBER, 0);
        CompletableFuture<Ordered<Object>> f2 = leader.replicate(applyValue("1"));

        assertThat(f1).isNotDone();
        eventually(() -> assertThat(f2).isDone());

        try {
            f2.join();
            fail();
        } catch (CompletionException e) {
            assertThat(e).hasCauseInstanceOf(CannotReplicateException.class);
        }
    }

    @Test(timeout = 300_000)
    public void when_replicatedMembershipChangeIsReverted_then_itCanBeCommittedOnSecondReplicate() {
        group = LocalRaftGroup.newBuilder(3).setConfig(TEST_RAFT_CONFIG).enableNewTermOperation().start();

        RaftNodeImpl leader = group.waitUntilLeaderElected();
        List<RaftNodeImpl> followers = group.getNodesExcept(leader.getLocalEndpoint());

        leader.replicate(applyValue("val1")).join();
        long oldLeaderCommitIndexBeforeMembershipChange = getCommitIndex(leader);

        eventually(() -> {
            for (RaftNodeImpl follower : followers) {
                assertThat(getCommitIndex(follower)).isEqualTo(oldLeaderCommitIndexBeforeMembershipChange);
            }
        });

        group.dropMessagesTo(leader.getLocalEndpoint(), followers.get(0).getLocalEndpoint(),
                AppendEntriesRequest.class);
        group.dropMessagesTo(leader.getLocalEndpoint(), followers.get(1).getLocalEndpoint(),
                AppendEntriesRequest.class);

        RaftNodeImpl newNode = group.createNewNode();

        leader.changeMembership(newNode.getLocalEndpoint(), ADD_LEARNER, 0);

        eventually(() -> {
            long leaderLastLogIndex = getLastLogOrSnapshotEntry(leader).getIndex();
            assertThat(leaderLastLogIndex).isGreaterThan(oldLeaderCommitIndexBeforeMembershipChange)
                    .isEqualTo(getLastLogOrSnapshotEntry(newNode).getIndex());
        });

        group.dropMessagesToAll(newNode.getLocalEndpoint(), PreVoteRequest.class);
        group.dropMessagesToAll(newNode.getLocalEndpoint(), VoteRequest.class);

        group.terminateNode(leader.getLocalEndpoint());

        eventually(() -> {
            RaftEndpoint l0 = followers.get(0).getLeaderEndpoint();
            RaftEndpoint l1 = followers.get(1).getLeaderEndpoint();
            assertThat(l0).isNotNull().isNotEqualTo(leader.getLocalEndpoint());
            assertThat(l1).isNotNull().isNotEqualTo(leader.getLocalEndpoint()).isEqualTo(l0);
        });

        RaftNodeImpl newLeader = group.getNode(followers.get(0).getLeaderEndpoint());
        newLeader.replicate(applyValue("val1")).join();
        newLeader.changeMembership(newNode.getLocalEndpoint(), ADD_LEARNER, 0).join();

        eventually(() -> {
            assertThat(getCommitIndex(newNode)).isEqualTo(getCommitIndex(newLeader));
            assertThat(getCommittedGroupMembers(newNode).getLogIndex())
                    .isEqualTo(getCommittedGroupMembers(newLeader).getLogIndex());
        });
    }

    @Test(timeout = 300_000)
    public void when_raftGroupIsExtendedToEvenNumberOfServers_then_logReplicationQuorumSizeCanBeDecreased() {
        group = LocalRaftGroup.newBuilder(3).setConfig(TEST_RAFT_CONFIG).enableNewTermOperation().start();

        RaftNodeImpl leader = group.waitUntilLeaderElected();
        List<RaftNodeImpl> followers = group.getNodesExcept(leader.getLocalEndpoint());
        RaftNodeImpl newNode = group.createNewNode();
        Ordered<RaftGroupMembers> result = leader.changeMembership(newNode.getLocalEndpoint(), ADD_LEARNER, 0).join();
        leader.changeMembership(newNode.getLocalEndpoint(), ADD_OR_PROMOTE_TO_FOLLOWER, result.getCommitIndex()).join();

        eventually(() -> assertThat(newNode.getLeaderEndpoint()).isEqualTo(leader.getLocalEndpoint()));

        for (RaftNodeImpl follower : followers) {
            group.dropMessagesTo(leader.getLocalEndpoint(), follower.getLocalEndpoint(), AppendEntriesRequest.class);
        }

        leader.replicate(applyValue("val")).join();
    }

    @Test(timeout = 300_000)
    public void when_raftGroupIsExtendingToEvenNumberOfServers_then_membershipChangeCannotBeCommittedWithoutMajority() {
        group = LocalRaftGroup.newBuilder(3).setConfig(TEST_RAFT_CONFIG).enableNewTermOperation().start();

        RaftNodeImpl leader = group.waitUntilLeaderElected();
        for (RaftNode follower : group.getNodesExcept(leader.getLocalEndpoint())) {
            group.dropMessagesTo(leader.getLocalEndpoint(), follower.getLocalEndpoint(), AppendEntriesRequest.class);
        }

        RaftNodeImpl newNode = group.createNewNode();
        try {
            leader.changeMembership(newNode.getLocalEndpoint(), ADD_LEARNER, 0).join();
            fail();
        } catch (CompletionException e) {
            assertThat(e).satisfiesAnyOf(e2 -> assertThat(e2).hasCauseInstanceOf(IndeterminateStateException.class),
                    e2 -> assertThat(e2).hasCauseInstanceOf(NotLeaderException.class));

        }
    }

    @Test(timeout = 300_000)
    public void when_thereIsSingleVotingMember_then_learnerCanBeRemoved() {
        group = LocalRaftGroup.newBuilder(2, 1).setConfig(TEST_RAFT_CONFIG).enableNewTermOperation().start();
        RaftNodeImpl leader = group.waitUntilLeaderElected();
        RaftNodeImpl learner = group.getAnyNodeExcept(leader.getLocalEndpoint());

        Ordered<RaftGroupMembers> result = leader.changeMembership(learner.getLocalEndpoint(), REMOVE_MEMBER, 0).join();

        assertThat(result.getResult().getMembers()).contains(leader.getLocalEndpoint());
        assertThat(result.getResult().getMembers()).doesNotContain(learner.getLocalEndpoint());
        assertThat(result.getResult().getVotingMembers()).contains(leader.getLocalEndpoint());
        assertThat(result.getResult().getVotingMembers()).doesNotContain(learner.getLocalEndpoint());
    }

    @Test(timeout = 300_000)
    public void when_thereIsSingleVotingMember_then_votingMemberCannotBeRemoved() {
        group = LocalRaftGroup.newBuilder(2, 1).setConfig(TEST_RAFT_CONFIG).enableNewTermOperation().start();
        RaftNodeImpl leader = group.waitUntilLeaderElected();

        try {
            leader.changeMembership(leader.getLocalEndpoint(), REMOVE_MEMBER, 0).join();
            fail();
        } catch (CompletionException e) {
            assertThat(e).hasCauseInstanceOf(IllegalStateException.class);
        }
    }

    @Test(timeout = 300_000)
    public void when_leaderCrashesWhileLeavingRaftGroup_then_remainingVotingMemberCommitsMembershipChange() {
        group = LocalRaftGroup.newBuilder(3, 2).setConfig(TEST_RAFT_CONFIG).enableNewTermOperation().start();
        RaftNodeImpl leader = group.waitUntilLeaderElected();
        RaftNodeImpl follower = null;
        for (RaftNodeImpl node : group.<RaftNodeImpl>getNodesExcept(leader.getLocalEndpoint())) {
            if (getRole(node) == FOLLOWER) {
                follower = node;
                break;
            }
        }
        assertNotNull(follower);

        group.dropMessagesTo(follower.getLocalEndpoint(), leader.getLocalEndpoint(),
                AppendEntriesSuccessResponse.class);

        leader.changeMembership(leader.getLocalEndpoint(), REMOVE_MEMBER, 0);

        RaftNodeImpl node = follower;
        eventually(() -> assertThat(getEffectiveGroupMembers(node).getLogIndex()).isGreaterThan(0));

        leader.terminate();

        eventually(() -> assertThat(node.getLeaderEndpoint()).isEqualTo(node.getLocalEndpoint()));

        assertThat(getCommittedGroupMembers(follower).getMembers()).doesNotContain(leader.getLocalEndpoint());
    }

}
