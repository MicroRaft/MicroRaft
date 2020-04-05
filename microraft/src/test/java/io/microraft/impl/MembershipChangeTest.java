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
import io.microraft.exception.CannotReplicateException;
import io.microraft.impl.local.LocalRaftGroup;
import io.microraft.impl.local.SimpleStateMachine;
import io.microraft.impl.state.RaftGroupMembersState;
import io.microraft.impl.util.BaseTest;
import io.microraft.model.message.AppendEntriesFailureResponse;
import io.microraft.model.message.AppendEntriesRequest;
import io.microraft.model.message.AppendEntriesSuccessResponse;
import io.microraft.model.message.PreVoteRequest;
import io.microraft.model.message.VoteRequest;
import io.microraft.report.RaftGroupMembers;
import org.junit.After;
import org.junit.Test;

import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import static io.microraft.MembershipChangeMode.ADD;
import static io.microraft.MembershipChangeMode.REMOVE;
import static io.microraft.RaftNodeStatus.ACTIVE;
import static io.microraft.RaftNodeStatus.TERMINATED;
import static io.microraft.impl.local.SimpleStateMachine.apply;
import static io.microraft.impl.util.AssertionUtils.allTheTime;
import static io.microraft.impl.util.AssertionUtils.eventually;
import static io.microraft.impl.util.RaftTestUtils.TEST_RAFT_CONFIG;
import static io.microraft.impl.util.RaftTestUtils.getCommitIndex;
import static io.microraft.impl.util.RaftTestUtils.getCommittedGroupMembers;
import static io.microraft.impl.util.RaftTestUtils.getEffectiveGroupMembers;
import static io.microraft.impl.util.RaftTestUtils.getLastLogOrSnapshotEntry;
import static io.microraft.impl.util.RaftTestUtils.getSnapshotEntry;
import static io.microraft.impl.util.RaftTestUtils.getStatus;
import static java.util.Arrays.asList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.fail;

/**
 * @author mdogan
 * @author metanet
 */
public class MembershipChangeTest
        extends BaseTest {

    private LocalRaftGroup group;

    @After
    public void destroy() {
        if (group != null) {
            group.destroy();
        }
    }

    @Test(timeout = 300_000)
    public void when_newRaftNodeJoins_then_itAppendsMissingEntries()
            throws Exception {
        group = LocalRaftGroup.start(3);

        RaftNodeImpl leader = group.waitUntilLeaderElected();
        leader.replicate(apply("val")).get();

        RaftGroupMembersState initialMembers = leader.getInitialMembers();

        RaftNodeImpl newRaftNode = group.createNewNode();

        Ordered<RaftGroupMembers> result = leader.changeMembership(newRaftNode.getLocalEndpoint(), ADD, 0).get();

        assertThat(result.getCommitIndex()).isEqualTo(getCommitIndex(leader));
        assertThat(result.getResult().getMembers()).contains(newRaftNode.getLocalEndpoint());
        assertThat(leader.getInitialMembers().getMembers()).isEqualTo(initialMembers.getMembers());
        assertThat(leader.getCommittedMembers().isKnownMember(newRaftNode.getLocalEndpoint())).isTrue();
        assertThat(leader.getEffectiveMembers().isKnownMember(newRaftNode.getLocalEndpoint())).isTrue();

        long commitIndex = getCommitIndex(leader);
        eventually(() -> assertThat(getCommitIndex(newRaftNode)).isEqualTo(commitIndex));

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

        SimpleStateMachine stateMachine = group.getStateMachine(newRaftNode.getLocalEndpoint());
        assertThat(stateMachine.size()).isEqualTo(1);
        assertThat(stateMachine.valueSet()).contains("val");

        assertThat(newRaftNode.getInitialMembers().getMembers()).isEqualTo(initialMembers.getMembers());
        assertThat(newRaftNode.getCommittedMembers().getMembers()).isEqualTo(leader.getCommittedMembers().getMembers());
        assertThat(newRaftNode.getCommittedMembers().getLogIndex()).isEqualTo(leader.getCommittedMembers().getLogIndex());
        assertThat(newRaftNode.getEffectiveMembers().getMembers()).isEqualTo(leader.getEffectiveMembers().getMembers());
        assertThat(newRaftNode.getEffectiveMembers().getLogIndex()).isEqualTo(leader.getEffectiveMembers().getLogIndex());
    }

    @Test(timeout = 300_000)
    public void when_followerLeaves_then_itIsRemovedFromTheGroupMembers()
            throws Exception {
        group = LocalRaftGroup.start(3);

        RaftNodeImpl leader = group.waitUntilLeaderElected();
        List<RaftNodeImpl> followers = group.getNodesExcept(leader.getLocalEndpoint());
        RaftNodeImpl leavingFollower = followers.get(0);
        RaftNodeImpl stayingFollower = followers.get(1);

        leader.replicate(apply("val")).get();

        Ordered<RaftGroupMembers> result = leader.changeMembership(leavingFollower.getLocalEndpoint(), REMOVE, 0).get();

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
    public void when_newRaftNodeJoinsAfterAnotherNodeLeaves_then_itAppendsMissingEntries()
            throws Exception {
        group = LocalRaftGroup.start(3);

        RaftNodeImpl leader = group.waitUntilLeaderElected();
        leader.replicate(apply("val")).get();

        List<RaftNodeImpl> followers = group.getNodesExcept(leader.getLocalEndpoint());
        RaftNodeImpl leavingFollower = followers.get(0);
        RaftNodeImpl stayingFollower = followers.get(1);

        long newMembersCommitIndex = leader.changeMembership(leavingFollower.getLocalEndpoint(), REMOVE, 0).get()
                                           .getCommitIndex();

        RaftNodeImpl newRaftNode = group.createNewNode();

        leader.changeMembership(newRaftNode.getLocalEndpoint(), ADD, newMembersCommitIndex).get();

        long commitIndex = getCommitIndex(leader);
        eventually(() -> assertThat(getCommitIndex(newRaftNode)).isEqualTo(commitIndex));

        RaftGroupMembersState effectiveGroupMembers = getEffectiveGroupMembers(leader);
        eventually(() -> {
            for (RaftNodeImpl node : asList(leader, stayingFollower, newRaftNode)) {
                assertThat(getStatus(node)).isEqualTo(ACTIVE);
                assertThat(getEffectiveGroupMembers(node).getMembers()).isEqualTo(effectiveGroupMembers.getMembers());
                assertThat(getEffectiveGroupMembers(node).getLogIndex()).isEqualTo(effectiveGroupMembers.getLogIndex());
                assertThat(getCommittedGroupMembers(node).getMembers()).isEqualTo(effectiveGroupMembers.getMembers());
                assertThat(getCommittedGroupMembers(node).getLogIndex()).isEqualTo(effectiveGroupMembers.getLogIndex());
                assertThat(getEffectiveGroupMembers(node).isKnownMember(leavingFollower.getLocalEndpoint())).isFalse();
                assertThat(getCommittedGroupMembers(node).isKnownMember(leavingFollower.getLocalEndpoint())).isFalse();
            }
        });

        SimpleStateMachine stateMachine = group.getStateMachine(newRaftNode.getLocalEndpoint());
        assertThat(stateMachine.size()).isEqualTo(1);
        assertThat(stateMachine.valueSet()).contains("val");
    }

    @Test(timeout = 300_000)
    public void when_newRaftNodeJoinsAfterAnotherNodeLeavesAndSnapshotIsTaken_then_itAppendsMissingEntries()
            throws Exception {
        int commitCountToTakeSnapshot = 10;
        RaftConfig config = RaftConfig.newBuilder().setCommitCountToTakeSnapshot(commitCountToTakeSnapshot).build();
        group = LocalRaftGroup.start(3, config);

        RaftNodeImpl leader = group.waitUntilLeaderElected();

        leader.replicate(apply("val")).get();

        List<RaftNodeImpl> followers = group.getNodesExcept(leader.getLocalEndpoint());
        RaftNodeImpl leavingFollower = followers.get(0);
        RaftNodeImpl stayingFollower = followers.get(1);

        long newMembersIndex = leader.changeMembership(leavingFollower.getLocalEndpoint(), REMOVE, 0).get().getCommitIndex();

        for (int i = 0; i < commitCountToTakeSnapshot; i++) {
            leader.replicate(apply("val" + i)).get();
        }

        eventually(() -> assertThat(getSnapshotEntry(leader).getIndex()).isGreaterThan(0));

        RaftNodeImpl newRaftNode = group.createNewNode();

        leader.changeMembership(newRaftNode.getLocalEndpoint(), ADD, newMembersIndex).get();

        long commitIndex = getCommitIndex(leader);
        eventually(() -> assertThat(getCommitIndex(newRaftNode)).isEqualTo(commitIndex));

        RaftGroupMembersState effectiveGroupMembers = getEffectiveGroupMembers(leader);
        eventually(() -> {
            for (RaftNodeImpl node : asList(leader, stayingFollower, newRaftNode)) {
                assertThat(getStatus(node)).isEqualTo(ACTIVE);
                assertThat(getEffectiveGroupMembers(node).getMembers()).isEqualTo(effectiveGroupMembers.getMembers());
                assertThat(getEffectiveGroupMembers(node).getLogIndex()).isEqualTo(effectiveGroupMembers.getLogIndex());
                assertThat(getCommittedGroupMembers(node).getMembers()).isEqualTo(effectiveGroupMembers.getMembers());
                assertThat(getCommittedGroupMembers(node).getLogIndex()).isEqualTo(effectiveGroupMembers.getLogIndex());
                assertThat(getEffectiveGroupMembers(node).isKnownMember(leavingFollower.getLocalEndpoint())).isFalse();
                assertThat(getCommittedGroupMembers(node).isKnownMember(leavingFollower.getLocalEndpoint())).isFalse();
            }
        });

        SimpleStateMachine stateMachine = group.getStateMachine(newRaftNode.getLocalEndpoint());
        assertThat(stateMachine.size()).isEqualTo(commitCountToTakeSnapshot + 1);
        assertThat(stateMachine.valueSet()).contains("val");
        for (int i = 0; i < commitCountToTakeSnapshot; i++) {
            assertThat(stateMachine.valueSet()).contains("val" + i);
        }
    }

    @Test(timeout = 300_000)
    public void when_leaderLeaves_then_itIsRemovedFromTheGroupMembers()
            throws Exception {
        group = LocalRaftGroup.start(3);

        RaftNodeImpl leader = group.waitUntilLeaderElected();

        leader.replicate(apply("val")).get();
        leader.changeMembership(leader.getLocalEndpoint(), REMOVE, 0).get();

        assertThat(leader.getStatus()).isEqualTo(TERMINATED);

        eventually(() -> {
            for (RaftNodeImpl node : group.<RaftNodeImpl>getNodesExcept(leader.getLocalEndpoint())) {
                assertThat(getEffectiveGroupMembers(node).isKnownMember(leader.getLocalEndpoint())).isFalse();
                assertThat(getCommittedGroupMembers(node).isKnownMember(leader.getLocalEndpoint())).isFalse();
            }
        });
    }

    @Test(timeout = 300_000)
    public void when_leaderLeaves_then_itCannotVoteForCommitOfMemberChange()
            throws Exception {
        RaftConfig config = RaftConfig.newBuilder().setLeaderHeartbeatPeriodSecs(1).build();
        group = LocalRaftGroup.start(3, config);

        RaftNodeImpl leader = group.waitUntilLeaderElected();
        RaftNodeImpl follower = group.getAnyFollower();

        group.dropMessagesTo(follower.getLocalEndpoint(), leader.getLocalEndpoint(), AppendEntriesSuccessResponse.class);
        leader.replicate(apply("val")).get();

        leader.changeMembership(leader.getLocalEndpoint(), REMOVE, 0);

        allTheTime(() -> assertThat(getCommitIndex(leader)).isEqualTo(1), 10);
    }

    @Test(timeout = 300_000)
    public void when_leaderLeaves_then_followersElectNewLeader()
            throws Exception {
        group = LocalRaftGroup.start(3, TEST_RAFT_CONFIG);

        RaftNodeImpl leader = group.waitUntilLeaderElected();
        List<RaftNodeImpl> followers = group.getNodesExcept(leader.getLocalEndpoint());

        leader.replicate(apply("val")).get();
        leader.changeMembership(leader.getLocalEndpoint(), REMOVE, 0).get();

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
    public void when_membershipChangeRequestIsMadeWithWrongType_then_membershipChangeFails()
            throws Exception {
        group = LocalRaftGroup.start(3);

        RaftNodeImpl leader = group.waitUntilLeaderElected();
        leader.replicate(apply("val")).get();

        try {
            leader.changeMembership(leader.getLocalEndpoint(), null, 0).get();
            fail();
        } catch (ExecutionException e) {
            assertThat(e).hasCauseInstanceOf(IllegalArgumentException.class);
        }
    }

    @Test(timeout = 300_000)
    public void when_nonExistingEndpointIsRemoved_then_membershipChangeFails()
            throws Exception {
        group = LocalRaftGroup.start(3);

        RaftNodeImpl leader = group.waitUntilLeaderElected();
        RaftNodeImpl leavingFollower = group.getAnyFollower();

        leader.replicate(apply("val")).get();
        long newMembersIndex = leader.changeMembership(leavingFollower.getLocalEndpoint(), REMOVE, 0).get().getCommitIndex();

        try {
            leader.changeMembership(leavingFollower.getLocalEndpoint(), REMOVE, newMembersIndex).get();
            fail();
        } catch (ExecutionException e) {
            assertThat(e).hasCauseInstanceOf(IllegalArgumentException.class);
        }
    }

    @Test(timeout = 300_000)
    public void when_existingEndpointIsAdded_then_membershipChangeFails()
            throws Exception {
        group = LocalRaftGroup.start(3);

        RaftNodeImpl leader = group.waitUntilLeaderElected();

        leader.replicate(apply("val")).get();

        try {
            leader.changeMembership(leader.getLocalEndpoint(), ADD, 0).get();
            fail();
        } catch (ExecutionException e) {
            assertThat(e).hasCauseInstanceOf(IllegalArgumentException.class);
        }
    }

    @Test(timeout = 300_000)
    public void when_thereIsNoCommitInTheCurrentTerm_then_cannotMakeMemberChange()
            throws InterruptedException {
        // https://groups.google.com/forum/#!msg/raft-dev/t4xj6dJTP6E/d2D9LrWRza8J

        group = LocalRaftGroup.start(3);

        RaftNodeImpl leader = group.waitUntilLeaderElected();

        try {
            leader.changeMembership(leader.getLocalEndpoint(), REMOVE, 0).get();
            fail();
        } catch (ExecutionException e) {
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
                leader.changeMembership(leader.getLocalEndpoint(), REMOVE, 0).get();
            } catch (ExecutionException e) {
                if (e.getCause() instanceof CannotReplicateException) {
                    fail();
                }

                throw e;
            }
        });
    }

    @Test(timeout = 300_000)
    public void when_newJoiningNodeFirstReceivesSnapshot_then_itInstallsSnapshot()
            throws Exception {
        RaftConfig config = RaftConfig.newBuilder().setCommitCountToTakeSnapshot(5).build();
        group = LocalRaftGroup.start(3, config);

        RaftNodeImpl leader = group.waitUntilLeaderElected();
        for (int i = 0; i < 4; i++) {
            leader.replicate(apply("val" + i)).get();
        }

        RaftNodeImpl newRaftNode = group.createNewNode();

        group.dropMessagesTo(leader.getLocalEndpoint(), newRaftNode.getLocalEndpoint(), AppendEntriesRequest.class);

        leader.changeMembership(newRaftNode.getLocalEndpoint(), ADD, 0).get();

        eventually(() -> assertThat(getSnapshotEntry(leader).getIndex()).isGreaterThan(0));

        group.resetAllRulesFrom(leader.getLocalEndpoint());

        eventually(() -> {
            assertThat(getCommitIndex(newRaftNode)).isEqualTo(getCommitIndex(leader));
            assertThat(getEffectiveGroupMembers(newRaftNode).getMembers())
                    .isEqualTo(getEffectiveGroupMembers(leader).getMembers());
            assertThat(getCommittedGroupMembers(newRaftNode).getMembers())
                    .isEqualTo(getEffectiveGroupMembers(leader).getMembers());
            SimpleStateMachine stateMachine = group.getStateMachine(newRaftNode.getLocalEndpoint());
            assertThat(stateMachine.size()).isEqualTo(4);
        });
    }

    @Test(timeout = 300_000)
    public void when_leaderFailsWhileLeavingRaftGroup_othersCommitTheMembershipChange()
            throws Exception {
        group = LocalRaftGroup.start(3, TEST_RAFT_CONFIG);

        RaftNodeImpl leader = group.waitUntilLeaderElected();
        List<RaftNodeImpl> followers = group.getNodesExcept(leader.getLocalEndpoint());

        leader.replicate(apply("val")).get();

        for (RaftNodeImpl follower : followers) {
            group.dropMessagesTo(follower.getLocalEndpoint(), leader.getLocalEndpoint(), AppendEntriesSuccessResponse.class);
            group.dropMessagesTo(follower.getLocalEndpoint(), leader.getLocalEndpoint(), AppendEntriesFailureResponse.class);
        }

        leader.changeMembership(leader.getLocalEndpoint(), REMOVE, 0);

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
        newLeader.replicate(apply("val2"));

        eventually(() -> {
            for (RaftNodeImpl follower : followers) {
                assertThat(getCommittedGroupMembers(follower).isKnownMember(leader.getLocalEndpoint())).isFalse();
            }
        });
    }

    @Test(timeout = 300_000)
    public void when_followerAppendsMultipleMembershipChangesAtOnce_then_itCommitsThemCorrectly()
            throws Exception {
        RaftConfig config = RaftConfig.newBuilder().setLeaderHeartbeatPeriodSecs(1).build();
        group = LocalRaftGroup.start(5, config);

        RaftNodeImpl leader = group.waitUntilLeaderElected();
        List<RaftNodeImpl> followers = group.getNodesExcept(leader.getLocalEndpoint());

        leader.replicate(apply("val")).get();

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

        RaftNodeImpl newRaftNode1 = group.createNewNode();
        group.dropMessagesTo(leader.getLocalEndpoint(), newRaftNode1.getLocalEndpoint(), AppendEntriesRequest.class);
        Future<Ordered<RaftGroupMembers>> f1 = leader.changeMembership(newRaftNode1.getLocalEndpoint(), ADD, 0);

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

        long newMembersIndex = f1.get().getCommitIndex();
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

        RaftNodeImpl newRaftNode2 = group.createNewNode();
        leader.changeMembership(newRaftNode2.getLocalEndpoint(), ADD, newMembersIndex).get();

        group.allowAllMessagesTo(leader.getLocalEndpoint(), slowFollower.getLocalEndpoint());
        group.allowAllMessagesTo(slowFollower.getLocalEndpoint(), leader.getLocalEndpoint());
        group.allowAllMessagesTo(leader.getLocalEndpoint(), newRaftNode1.getLocalEndpoint());

        RaftGroupMembersState leaderCommittedGroupMembers = getCommittedGroupMembers(leader);
        eventually(() -> {
            assertThat(getCommittedGroupMembers(slowFollower).getLogIndex()).isEqualTo(leaderCommittedGroupMembers.getLogIndex());
            assertThat(getCommittedGroupMembers(newRaftNode1).getLogIndex()).isEqualTo(leaderCommittedGroupMembers.getLogIndex());
            assertThat(getCommittedGroupMembers(newRaftNode2).getLogIndex()).isEqualTo(leaderCommittedGroupMembers.getLogIndex());
        });
    }

    @Test(timeout = 300_000)
    public void when_leaderIsSteppingDown_then_itDoesNotAcceptNewAppends()
            throws InterruptedException {
        group = LocalRaftGroup.newBuilder(3).enableNewTermOperation().start();

        RaftNodeImpl leader = group.waitUntilLeaderElected();
        List<RaftNodeImpl> followers = group.getNodesExcept(leader.getLocalEndpoint());
        group.dropMessagesTo(leader.getLocalEndpoint(), followers.get(0).getLocalEndpoint(), AppendEntriesRequest.class);
        group.dropMessagesTo(leader.getLocalEndpoint(), followers.get(1).getLocalEndpoint(), AppendEntriesRequest.class);

        Future<Ordered<RaftGroupMembers>> f1 = leader.changeMembership(leader.getLocalEndpoint(), REMOVE, 0);
        Future<Ordered<Object>> f2 = leader.replicate(apply("1"));

        assertThat(f1).isNotDone();
        eventually(() -> assertThat(f2).isDone());

        try {
            f2.get();
            fail();
        } catch (ExecutionException e) {
            assertThat(e).hasCauseInstanceOf(CannotReplicateException.class);
        }
    }

    @Test(timeout = 300_000)
    public void when_replicatedMembershipChangeIsReverted_then_itCanBeCommittedOnSecondReplicate()
            throws Exception {
        group = LocalRaftGroup.newBuilder(3).setConfig(TEST_RAFT_CONFIG).enableNewTermOperation().start();

        RaftNodeImpl leader = group.waitUntilLeaderElected();
        List<RaftNodeImpl> followers = group.getNodesExcept(leader.getLocalEndpoint());

        leader.replicate(apply("val1")).get();
        long oldLeaderCommitIndexBeforeMembershipChange = getCommitIndex(leader);

        eventually(() -> {
            for (RaftNodeImpl follower : followers) {
                assertThat(getCommitIndex(follower)).isEqualTo(oldLeaderCommitIndexBeforeMembershipChange);
            }
        });

        group.dropMessagesTo(leader.getLocalEndpoint(), followers.get(0).getLocalEndpoint(), AppendEntriesRequest.class);
        group.dropMessagesTo(leader.getLocalEndpoint(), followers.get(1).getLocalEndpoint(), AppendEntriesRequest.class);

        RaftNodeImpl newRaftNode = group.createNewNode();

        leader.changeMembership(newRaftNode.getLocalEndpoint(), ADD, 0);

        eventually(() -> {
            long leaderLastLogIndex = getLastLogOrSnapshotEntry(leader).getIndex();
            assertThat(leaderLastLogIndex).isGreaterThan(oldLeaderCommitIndexBeforeMembershipChange)
                                          .isEqualTo(getLastLogOrSnapshotEntry(newRaftNode).getIndex());
        });

        group.dropMessagesToAll(newRaftNode.getLocalEndpoint(), PreVoteRequest.class);
        group.dropMessagesToAll(newRaftNode.getLocalEndpoint(), VoteRequest.class);

        group.terminateNode(leader.getLocalEndpoint());

        eventually(() -> {
            RaftEndpoint l0 = followers.get(0).getLeaderEndpoint();
            RaftEndpoint l1 = followers.get(1).getLeaderEndpoint();
            assertThat(l0).isNotNull().isNotEqualTo(leader.getLocalEndpoint());
            assertThat(l1).isNotNull().isNotEqualTo(leader.getLocalEndpoint()).isEqualTo(l0);
        });

        RaftNodeImpl newLeader = group.getNode(followers.get(0).getLeaderEndpoint());
        newLeader.replicate(apply("val1")).get();
        newLeader.changeMembership(newRaftNode.getLocalEndpoint(), ADD, 0).get();

        eventually(() -> {
            assertThat(getCommitIndex(newRaftNode)).isEqualTo(getCommitIndex(newLeader));
            assertThat(getCommittedGroupMembers(newRaftNode).getLogIndex())
                    .isEqualTo(getCommittedGroupMembers(newLeader).getLogIndex());
        });
    }

}
