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

import io.microraft.MembershipChangeMode;
import io.microraft.Ordered;
import io.microraft.RaftConfig;
import io.microraft.RaftEndpoint;
import io.microraft.RaftRole;
import io.microraft.impl.local.InMemoryRaftStore;
import io.microraft.impl.local.LocalRaftGroup;
import io.microraft.impl.local.SimpleStateMachine;
import io.microraft.model.log.LogEntry;
import io.microraft.model.log.SnapshotEntry;
import io.microraft.model.message.AppendEntriesRequest;
import io.microraft.model.message.PreVoteRequest;
import io.microraft.persistence.RestoredRaftState;
import io.microraft.report.RaftGroupMembers;
import io.microraft.test.util.BaseTest;
import org.junit.After;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static io.microraft.MembershipChangeMode.ADD_OR_PROMOTE_TO_FOLLOWER;
import static io.microraft.MembershipChangeMode.REMOVE_MEMBER;
import static io.microraft.RaftRole.FOLLOWER;
import static io.microraft.RaftRole.LEARNER;
import static io.microraft.impl.local.LocalRaftGroup.IN_MEMORY_RAFT_STATE_STORE_FACTORY;
import static io.microraft.impl.local.SimpleStateMachine.applyValue;
import static io.microraft.test.util.AssertionUtils.eventually;
import static io.microraft.test.util.RaftTestUtils.TEST_RAFT_CONFIG;
import static io.microraft.test.util.RaftTestUtils.getCommitIndex;
import static io.microraft.test.util.RaftTestUtils.getCommittedGroupMembers;
import static io.microraft.test.util.RaftTestUtils.getEffectiveGroupMembers;
import static io.microraft.test.util.RaftTestUtils.getLastApplied;
import static io.microraft.test.util.RaftTestUtils.getRaftStore;
import static io.microraft.test.util.RaftTestUtils.getRestoredState;
import static io.microraft.test.util.RaftTestUtils.getRole;
import static io.microraft.test.util.RaftTestUtils.getSnapshotEntry;
import static io.microraft.test.util.RaftTestUtils.getTerm;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class PersistenceTest
        extends BaseTest {

    private LocalRaftGroup group;

    @After
    public void destroy() {
        if (group != null) {
            group.destroy();
        }
    }

    @Test(timeout = 300_000)
    public void testTermAndVoteArePersisted() {
        group = LocalRaftGroup.newBuilder(3).setConfig(TEST_RAFT_CONFIG).setRaftStoreFactory(IN_MEMORY_RAFT_STATE_STORE_FACTORY)
                              .start();

        RaftNodeImpl leader = group.waitUntilLeaderElected();
        List<RaftNodeImpl> followers = group.getNodesExcept(leader.getLocalEndpoint());
        Set<RaftEndpoint> endpoints = new HashSet<>();
        for (RaftNodeImpl node : group.getNodes()) {
            endpoints.add(node.getLocalEndpoint());
        }

        int term1 = getTerm(leader);
        eventually(() -> {
            for (RaftNodeImpl node : group.getNodes()) {
                RestoredRaftState restoredState = getRestoredState(node);
                assertThat(restoredState.getLocalEndpoint()).isEqualTo(node.getLocalEndpoint());
                assertThat(restoredState.getTerm()).isEqualTo(term1);
                assertThat(restoredState.getInitialGroupMembers().getMembers()).isEqualTo(endpoints);
            }
        });

        group.terminateNode(leader.getLocalEndpoint());

        eventually(() -> {
            for (RaftNodeImpl node : followers) {
                RaftEndpoint l = node.getLeaderEndpoint();
                assertNotNull(l);
                assertThat(l).isNotEqualTo(leader.getLeaderEndpoint());
            }
        });

        RaftNodeImpl newLeader = group.waitUntilLeaderElected();
        int term2 = getTerm(newLeader);

        eventually(() -> {
            for (RaftNodeImpl node : followers) {
                RestoredRaftState restoredState = getRestoredState(node);
                assertThat(restoredState.getTerm()).isEqualTo(term2);
                assertThat(restoredState.getVotedMember()).isEqualTo(newLeader.getLeaderEndpoint());
            }
        });
    }

    @Test(timeout = 300_000) public void testCommittedEntriesArePersisted() {
        group = LocalRaftGroup.newBuilder(3).setRaftStoreFactory(IN_MEMORY_RAFT_STATE_STORE_FACTORY).start();

        RaftNodeImpl leader = group.waitUntilLeaderElected();
        int count = 10;
        for (int i = 0; i < count; i++) {
            leader.replicate(applyValue("val" + i)).join();
        }

        eventually(() -> {
            for (RaftNodeImpl node : group.getNodes()) {
                RestoredRaftState restoredState = getRestoredState(node);
                List<LogEntry> entries = restoredState.getLogEntries();
                assertThat(entries).hasSize(count);
                for (int i = 0; i < count; i++) {
                    assertThat(entries.get(i).getIndex()).isEqualTo(i + 1);
                }
            }
        });
    }

    @Test(timeout = 300_000)
    public void testUncommittedEntriesArePersisted() {
        group = LocalRaftGroup.newBuilder(3).setRaftStoreFactory(IN_MEMORY_RAFT_STATE_STORE_FACTORY).start();

        RaftNodeImpl leader = group.waitUntilLeaderElected();
        List<RaftNodeImpl> followers = group.getNodesExcept(leader.getLocalEndpoint());
        RaftNodeImpl responsiveFollower = followers.get(0);

        for (int i = 1; i < followers.size(); i++) {
            group.dropMessagesTo(leader.getLocalEndpoint(), followers.get(i).getLocalEndpoint(), AppendEntriesRequest.class);
        }

        int count = 10;
        for (int i = 0; i < count; i++) {
            leader.replicate(applyValue("val" + i));
        }

        eventually(() -> {
            for (RaftNodeImpl node : Arrays.asList(leader, responsiveFollower)) {
                RestoredRaftState restoredState = getRestoredState(node);
                List<LogEntry> entries = restoredState.getLogEntries();
                assertThat(entries).hasSize(count);
                for (int i = 0; i < count; i++) {
                    assertThat(entries.get(i).getIndex()).isEqualTo(i + 1);
                }
            }
        });
    }

    @Test(timeout = 300_000) public void testSnapshotIsPersisted() {
        int commitCountToTakeSnapshot = 50;
        RaftConfig config = RaftConfig.newBuilder().setCommitCountToTakeSnapshot(commitCountToTakeSnapshot).build();
        group = LocalRaftGroup.newBuilder(3).setConfig(config).setRaftStoreFactory(IN_MEMORY_RAFT_STATE_STORE_FACTORY).start();

        RaftNodeImpl leader = group.waitUntilLeaderElected();
        for (int i = 0; i < commitCountToTakeSnapshot; i++) {
            leader.replicate(applyValue("val" + i)).join();
        }

        eventually(() -> {
            assertThat(getSnapshotEntry(leader).getIndex()).isEqualTo(commitCountToTakeSnapshot);

            for (RaftNodeImpl node : group.getNodes()) {
                RestoredRaftState restoredState = getRestoredState(node);
                SnapshotEntry snapshot = restoredState.getSnapshotEntry();
                assertNotNull(snapshot);
                assertThat(snapshot.getIndex()).isEqualTo(commitCountToTakeSnapshot);
                assertNotNull(snapshot.getOperation());
            }
        });
    }

    @Test(timeout = 300_000) public void when_leaderAppendEntriesInMinoritySplit_then_itTruncatesEntriesOnStore() {
        group = LocalRaftGroup.newBuilder(3)
                              .setConfig(TEST_RAFT_CONFIG)
                              .setRaftStoreFactory(IN_MEMORY_RAFT_STATE_STORE_FACTORY)
                              .start();

        RaftNodeImpl leader = group.waitUntilLeaderElected();

        leader.replicate(applyValue("val1")).join();

        eventually(() -> {
            for (RaftNodeImpl node : group.getNodes()) {
                assertThat(getCommitIndex(node)).isEqualTo(1);
            }
        });

        List<RaftNodeImpl> followers = group.getNodesExcept(leader.getLocalEndpoint());
        group.splitMembers(leader.getLocalEndpoint());

        for (int i = 0; i < 10; i++) {
            leader.replicate(applyValue("isolated" + i));
        }

        eventually(() -> {
            for (RaftNodeImpl node : followers) {
                RaftEndpoint leaderEndpoint = node.getLeaderEndpoint();
                assertNotNull(leaderEndpoint);
                assertThat(leaderEndpoint).isNotEqualTo(leader.getLocalEndpoint());
            }
        });

        eventually(() -> {
            RestoredRaftState restoredState = getRestoredState(leader);
            assertThat(restoredState.getLogEntries()).hasSize(11);
        });

        RaftNodeImpl newLeader = group.getNode(followers.get(0).getLeaderEndpoint());
        for (int i = 0; i < 10; i++) {
            newLeader.replicate(applyValue("valNew" + i)).join();
        }

        eventually(() -> {
            for (RaftNodeImpl node : followers) {
                assertThat(getCommitIndex(node)).isEqualTo(11);
            }
        });

        group.merge();

        RaftNodeImpl finalLeader = group.waitUntilLeaderElected();

        assertThat(finalLeader.getLocalEndpoint()).isNotEqualTo(leader.getLocalEndpoint());

        eventually(() -> {
            RestoredRaftState state = getRestoredState(leader);
            assertThat(state.getLogEntries()).hasSize(11);
        });
    }

    @Test(timeout = 300_000) public void when_leaderIsRestarted_then_itBecomesFollowerAndRestoresItsRaftState() {
        group = LocalRaftGroup.newBuilder(3)
                              .setConfig(TEST_RAFT_CONFIG)
                              .enableNewTermOperation()
                              .setRaftStoreFactory(IN_MEMORY_RAFT_STATE_STORE_FACTORY)
                              .start();

        RaftNodeImpl leader = group.waitUntilLeaderElected();
        int count = 10;
        for (int i = 0; i < count; i++) {
            leader.replicate(applyValue("val" + i)).join();
        }

        RaftEndpoint terminatedEndpoint = leader.getLocalEndpoint();
        group.terminateNode(terminatedEndpoint);

        InMemoryRaftStore stateStore = getRaftStore(leader);
        RestoredRaftState terminatedState = stateStore.toRestoredRaftState();

        RaftNodeImpl newLeader = group.waitUntilLeaderElected();
        RaftNodeImpl restartedNode = group.restoreNode(terminatedState, stateStore);

        assertThat(getCommittedGroupMembers(restartedNode).getMembersList()).isEqualTo(
                getCommittedGroupMembers(newLeader).getMembersList());
        assertThat(getEffectiveGroupMembers(restartedNode).getMembersList()).isEqualTo(
                getEffectiveGroupMembers(newLeader).getMembersList());

        eventually(() -> {
            assertThat(restartedNode.getLeaderEndpoint()).isEqualTo(newLeader.getLocalEndpoint());
            assertThat(getTerm(restartedNode)).isEqualTo(getTerm(newLeader));
            assertThat(getCommitIndex(restartedNode)).isEqualTo(getCommitIndex(newLeader));
            assertThat(getLastApplied(restartedNode)).isEqualTo(getLastApplied(newLeader));
            SimpleStateMachine stateMachine = group.getStateMachine(restartedNode.getLocalEndpoint());
            List<Object> values = stateMachine.valueList();
            assertNotNull(values);
            assertThat(values).hasSize(count);
            for (int i = 0; i < count; i++) {
                assertThat(values.get(i)).isEqualTo("val" + i);
            }
        });
    }

    @Test(timeout = 300_000) public void when_leaderIsRestarted_then_itRestoresItsRaftStateAndBecomesLeader() {
        group = LocalRaftGroup.newBuilder(3)
                              .enableNewTermOperation()
                              .setRaftStoreFactory(IN_MEMORY_RAFT_STATE_STORE_FACTORY)
                              .start();

        RaftNodeImpl leader = group.waitUntilLeaderElected();
        int count = 10;
        for (int i = 0; i < count; i++) {
            leader.replicate(applyValue("val" + i)).join();
        }

        int term = getTerm(leader);
        long commitIndex = getCommitIndex(leader);

        RaftEndpoint terminatedEndpoint = leader.getLocalEndpoint();
        InMemoryRaftStore stateStore = getRaftStore(leader);
        RestoredRaftState terminatedState = stateStore.toRestoredRaftState();

        // Block voting between followers
        // to avoid a leader election before leader restarts.
        blockVotingBetweenFollowers();

        group.terminateNode(terminatedEndpoint);
        RaftNodeImpl restartedNode = group.restoreNode(terminatedState, stateStore);

        RaftNodeImpl newLeader = group.waitUntilLeaderElected();
        assertThat(restartedNode).isSameAs(newLeader);

        eventually(() -> {
            assertThat(getTerm(restartedNode)).isGreaterThan(term);
            assertThat(getCommitIndex(restartedNode)).isEqualTo(commitIndex + 1);
            SimpleStateMachine stateMachine = group.getStateMachine(restartedNode.getLocalEndpoint());
            List<Object> values = stateMachine.valueList();
            assertNotNull(values);
            assertThat(values).hasSize(count);
            for (int i = 0; i < count; i++) {
                assertThat(values.get(i)).isEqualTo("val" + i);
            }
        });
    }

    private void blockVotingBetweenFollowers() {
        for (RaftNodeImpl follower : group.<RaftNodeImpl>getNodesExcept(group.getLeaderEndpoint())) {
            group.dropMessagesToAll(follower.getLocalEndpoint(), PreVoteRequest.class);
        }
    }

    @Test(timeout = 300_000) public void when_followerIsRestarted_then_itRestoresItsRaftState() {
        group = LocalRaftGroup.newBuilder(3).setRaftStoreFactory(IN_MEMORY_RAFT_STATE_STORE_FACTORY).start();

        RaftNodeImpl leader = group.waitUntilLeaderElected();
        RaftNodeImpl terminatedFollower = group.getAnyNodeExcept(leader.getLocalEndpoint());
        int count = 10;
        for (int i = 0; i < count; i++) {
            leader.replicate(applyValue("val" + i)).join();
        }

        eventually(() -> assertEquals(getCommitIndex(leader), getCommitIndex(terminatedFollower)));

        RaftEndpoint terminatedEndpoint = terminatedFollower.getLocalEndpoint();
        group.terminateNode(terminatedEndpoint);

        InMemoryRaftStore stateStore = getRaftStore(terminatedFollower);
        RestoredRaftState terminatedState = stateStore.toRestoredRaftState();

        leader.replicate(applyValue("val" + count)).join();

        RaftNodeImpl restartedNode = group.restoreNode(terminatedState, stateStore);

        assertThat(getCommittedGroupMembers(restartedNode).getMembersList()).isEqualTo(
                getCommittedGroupMembers(leader).getMembersList());
        assertThat(getEffectiveGroupMembers(restartedNode).getMembersList()).isEqualTo(
                getEffectiveGroupMembers(leader).getMembersList());

        eventually(() -> {
            assertThat(restartedNode.getLeaderEndpoint()).isEqualTo(leader.getLocalEndpoint());
            assertThat(getTerm(restartedNode)).isEqualTo(getTerm(leader));
            assertThat(getCommitIndex(restartedNode)).isEqualTo(getCommitIndex(leader));
            assertThat(getLastApplied(restartedNode)).isEqualTo(getLastApplied(leader));
            SimpleStateMachine stateMachine = group.getStateMachine(restartedNode.getLocalEndpoint());
            List<Object> values = stateMachine.valueList();
            assertNotNull(values);
            assertThat(values).hasSize(count + 1);
            for (int i = 0; i <= count; i++) {
                assertThat(values.get(i)).isEqualTo("val" + i);
            }
        });
    }

    @Test(timeout = 300_000) public void when_learnerIsRestarted_then_itRestoresItsRaftState() {
        group = LocalRaftGroup.newBuilder(3, 2).setRaftStoreFactory(IN_MEMORY_RAFT_STATE_STORE_FACTORY).start();

        RaftNodeImpl leader = group.waitUntilLeaderElected();
        RaftNodeImpl terminatedLearner = null;
        for (RaftNodeImpl node : group.<RaftNodeImpl>getNodesExcept(leader.getLocalEndpoint())) {
            if (getRole(node) == LEARNER) {
                terminatedLearner = node;
                break;
            }
        }
        assertNotNull(terminatedLearner);

        int count = 10;
        for (int i = 0; i < count; i++) {
            leader.replicate(applyValue("val" + i)).join();
        }

        RaftNodeImpl terminated = terminatedLearner;
        eventually(() -> assertEquals(getCommitIndex(leader), getCommitIndex(terminated)));

        RaftEndpoint terminatedEndpoint = terminatedLearner.getLocalEndpoint();
        group.terminateNode(terminatedEndpoint);

        InMemoryRaftStore stateStore = getRaftStore(terminatedLearner);
        RestoredRaftState terminatedState = stateStore.toRestoredRaftState();

        leader.replicate(applyValue("val" + count)).join();

        RaftNodeImpl restartedNode = group.restoreNode(terminatedState, stateStore);

        assertThat(getRole(restartedNode)).isEqualTo(LEARNER);
        assertThat(getCommittedGroupMembers(restartedNode).getMembersList()).isEqualTo(
                getCommittedGroupMembers(leader).getMembersList());
        assertThat(getEffectiveGroupMembers(restartedNode).getMembersList()).isEqualTo(
                getEffectiveGroupMembers(leader).getMembersList());

        eventually(() -> {
            assertThat(restartedNode.getLeaderEndpoint()).isEqualTo(leader.getLocalEndpoint());
            assertThat(getTerm(restartedNode)).isEqualTo(getTerm(leader));
            assertThat(getCommitIndex(restartedNode)).isEqualTo(getCommitIndex(leader));
            assertThat(getLastApplied(restartedNode)).isEqualTo(getLastApplied(leader));
            SimpleStateMachine stateMachine = group.getStateMachine(restartedNode.getLocalEndpoint());
            List<Object> values = stateMachine.valueList();
            assertNotNull(values);
            assertThat(values).hasSize(count + 1);
            for (int i = 0; i <= count; i++) {
                assertThat(values.get(i)).isEqualTo("val" + i);
            }
        });
    }

    @Test(timeout = 300_000) public void when_leaderIsRestarted_then_itBecomesFollowerAndRestoresItsRaftStateWithSnapshot() {
        int commitCountToTakeSnapshot = 50;
        RaftConfig config = RaftConfig.newBuilder()
                                      .setLeaderElectionTimeoutMillis(2000)
                                      .setLeaderHeartbeatPeriodSecs(1)
                                      .setLeaderHeartbeatTimeoutSecs(5)
                                      .setCommitCountToTakeSnapshot(commitCountToTakeSnapshot)
                                      .build();
        group = LocalRaftGroup.newBuilder(3)
                              .setConfig(config)
                              .enableNewTermOperation()
                              .setRaftStoreFactory(IN_MEMORY_RAFT_STATE_STORE_FACTORY)
                              .start();

        RaftNodeImpl leader = group.waitUntilLeaderElected();

        for (int i = 0; i <= commitCountToTakeSnapshot; i++) {
            leader.replicate(applyValue("val" + i)).join();
        }

        assertThat(getSnapshotEntry(leader).getIndex()).isGreaterThan(0);

        RaftEndpoint terminatedEndpoint = leader.getLocalEndpoint();
        group.terminateNode(terminatedEndpoint);

        InMemoryRaftStore stateStore = getRaftStore(leader);
        RestoredRaftState terminatedState = stateStore.toRestoredRaftState();

        RaftNodeImpl newLeader = group.waitUntilLeaderElected();
        RaftNodeImpl restartedNode = group.restoreNode(terminatedState, stateStore);

        assertThat(getCommittedGroupMembers(restartedNode).getMembersList()).isEqualTo(
                getCommittedGroupMembers(newLeader).getMembersList());
        assertThat(getEffectiveGroupMembers(restartedNode).getMembersList()).isEqualTo(
                getEffectiveGroupMembers(newLeader).getMembersList());

        eventually(() -> {
            assertThat(restartedNode.getLeaderEndpoint()).isEqualTo(newLeader.getLocalEndpoint());
            assertThat(getTerm(restartedNode)).isEqualTo(getTerm(newLeader));
            assertThat(getCommitIndex(restartedNode)).isEqualTo(getCommitIndex(newLeader));
            assertThat(getLastApplied(restartedNode)).isEqualTo(getLastApplied(newLeader));
            SimpleStateMachine stateMachine = group.getStateMachine(restartedNode.getLocalEndpoint());
            List<Object> values = stateMachine.valueList();
            assertNotNull(values);
            assertThat(values).hasSize(commitCountToTakeSnapshot + 1);
            for (int i = 0; i <= commitCountToTakeSnapshot; i++) {
                assertThat(values.get(i)).isEqualTo("val" + i);
            }
        });
    }

    @Test(timeout = 300_000) public void when_followerIsRestarted_then_itRestoresItsRaftStateWithSnapshot() {
        int commitCountToTakeSnapshot = 50;
        RaftConfig config = RaftConfig.newBuilder().setCommitCountToTakeSnapshot(commitCountToTakeSnapshot).build();
        group = LocalRaftGroup.newBuilder(3)
                              .setConfig(config)
                              .enableNewTermOperation()
                              .setRaftStoreFactory(IN_MEMORY_RAFT_STATE_STORE_FACTORY)
                              .start();

        RaftNodeImpl leader = group.waitUntilLeaderElected();

        for (int i = 0; i <= commitCountToTakeSnapshot; i++) {
            leader.replicate(applyValue("val" + i)).join();
        }

        eventually(() -> {
            for (RaftNodeImpl node : group.getNodes()) {
                assertThat(getSnapshotEntry(node).getIndex()).isGreaterThan(0);
            }
        });

        RaftNodeImpl terminatedFollower = group.getAnyNodeExcept(leader.getLocalEndpoint());
        RaftEndpoint terminatedEndpoint = terminatedFollower.getLocalEndpoint();
        group.terminateNode(terminatedEndpoint);

        InMemoryRaftStore stateStore = getRaftStore(terminatedFollower);
        RestoredRaftState terminatedState = stateStore.toRestoredRaftState();

        leader.replicate(applyValue("val" + (commitCountToTakeSnapshot + 1))).join();

        RaftNodeImpl restartedNode = group.restoreNode(terminatedState, stateStore);

        assertThat(getCommittedGroupMembers(restartedNode).getMembersList()).isEqualTo(
                getCommittedGroupMembers(leader).getMembersList());
        assertThat(getEffectiveGroupMembers(restartedNode).getMembersList()).isEqualTo(
                getEffectiveGroupMembers(leader).getMembersList());

        eventually(() -> {
            assertThat(restartedNode.getLeaderEndpoint()).isEqualTo(leader.getLocalEndpoint());
            assertThat(getTerm(restartedNode)).isEqualTo(getTerm(leader));
            assertThat(getCommitIndex(restartedNode)).isEqualTo(getCommitIndex(leader));
            assertThat(getLastApplied(restartedNode)).isEqualTo(getLastApplied(leader));
            SimpleStateMachine stateMachine = group.getStateMachine(restartedNode.getLocalEndpoint());
            List<Object> values = stateMachine.valueList();
            assertNotNull(values);
            assertThat(values).hasSize(commitCountToTakeSnapshot + 2);
            for (int i = 0; i <= commitCountToTakeSnapshot + 1; i++) {
                assertThat(values.get(i)).isEqualTo("val" + i);
            }
        });
    }

    @Test(timeout = 300_000) public void when_learnerIsRestarted_then_itRestoresItsRaftStateWithSnapshot() {
        int commitCountToTakeSnapshot = 50;
        RaftConfig config = RaftConfig.newBuilder().setCommitCountToTakeSnapshot(commitCountToTakeSnapshot).build();
        group = LocalRaftGroup.newBuilder(3, 2)
                              .setConfig(config)
                              .enableNewTermOperation()
                              .setRaftStoreFactory(IN_MEMORY_RAFT_STATE_STORE_FACTORY)
                              .start();

        RaftNodeImpl leader = group.waitUntilLeaderElected();

        for (int i = 0; i <= commitCountToTakeSnapshot; i++) {
            leader.replicate(applyValue("val" + i)).join();
        }

        eventually(() -> {
            for (RaftNodeImpl node : group.getNodes()) {
                assertThat(getSnapshotEntry(node).getIndex()).isGreaterThan(0);
            }
        });

        RaftNodeImpl terminatedLearner = null;
        for (RaftNodeImpl node : group.<RaftNodeImpl>getNodesExcept(leader.getLocalEndpoint())) {
            if (getRole(node) == LEARNER) {
                terminatedLearner = node;
                break;
            }
        }
        assertNotNull(terminatedLearner);

        RaftEndpoint terminatedEndpoint = terminatedLearner.getLocalEndpoint();
        group.terminateNode(terminatedEndpoint);

        InMemoryRaftStore stateStore = getRaftStore(terminatedLearner);
        RestoredRaftState terminatedState = stateStore.toRestoredRaftState();

        leader.replicate(applyValue("val" + (commitCountToTakeSnapshot + 1))).join();

        RaftNodeImpl restartedNode = group.restoreNode(terminatedState, stateStore);

        assertThat(getCommittedGroupMembers(restartedNode).getMembersList()).isEqualTo(
                getCommittedGroupMembers(leader).getMembersList());
        assertThat(getEffectiveGroupMembers(restartedNode).getMembersList()).isEqualTo(
                getEffectiveGroupMembers(leader).getMembersList());
        assertThat(getRole(restartedNode)).isEqualTo(LEARNER);

        eventually(() -> {
            assertThat(restartedNode.getLeaderEndpoint()).isEqualTo(leader.getLocalEndpoint());
            assertThat(getTerm(restartedNode)).isEqualTo(getTerm(leader));
            assertThat(getCommitIndex(restartedNode)).isEqualTo(getCommitIndex(leader));
            assertThat(getLastApplied(restartedNode)).isEqualTo(getLastApplied(leader));
            SimpleStateMachine stateMachine = group.getStateMachine(restartedNode.getLocalEndpoint());
            List<Object> values = stateMachine.valueList();
            assertNotNull(values);
            assertThat(values).hasSize(commitCountToTakeSnapshot + 2);
            for (int i = 0; i <= commitCountToTakeSnapshot + 1; i++) {
                assertThat(values.get(i)).isEqualTo("val" + i);
            }
        });
    }

    @Test(timeout = 300_000) public void when_leaderIsRestarted_then_itRestoresItsRaftStateWithSnapshotAndBecomesLeader() {
        int commitCountToTakeSnapshot = 50;
        RaftConfig config = RaftConfig.newBuilder().setCommitCountToTakeSnapshot(commitCountToTakeSnapshot).build();
        group = LocalRaftGroup.newBuilder(3)
                              .setConfig(config)
                              .enableNewTermOperation()
                              .setRaftStoreFactory(IN_MEMORY_RAFT_STATE_STORE_FACTORY)
                              .start();

        RaftNodeImpl leader = group.waitUntilLeaderElected();

        for (int i = 0; i <= commitCountToTakeSnapshot; i++) {
            leader.replicate(applyValue("val" + i)).join();
        }

        assertThat(getSnapshotEntry(leader).getIndex()).isGreaterThan(0);
        int term = getTerm(leader);
        long commitIndex = getCommitIndex(leader);

        RaftEndpoint terminatedEndpoint = leader.getLocalEndpoint();
        InMemoryRaftStore stateStore = getRaftStore(leader);
        RestoredRaftState terminatedState = stateStore.toRestoredRaftState();

        // Block voting between followers
        // to avoid a leader election before leader restarts.
        blockVotingBetweenFollowers();

        group.terminateNode(terminatedEndpoint);
        RaftNodeImpl restartedNode = group.restoreNode(terminatedState, stateStore);

        RaftNodeImpl newLeader = group.waitUntilLeaderElected();
        assertThat(newLeader).isSameAs(restartedNode);

        eventually(() -> {
            assertThat(getTerm(restartedNode)).isGreaterThan(term);
            assertThat(getCommitIndex(restartedNode)).isEqualTo(commitIndex + 1);
            SimpleStateMachine stateMachine = group.getStateMachine(restartedNode.getLocalEndpoint());
            List<Object> values = stateMachine.valueList();
            assertNotNull(values);
            assertThat(values).hasSize(commitCountToTakeSnapshot + 1);
            for (int i = 0; i <= commitCountToTakeSnapshot; i++) {
                assertThat(values.get(i)).isEqualTo("val" + i);
            }
        });
    }

    @Test(timeout = 300_000) public void when_leaderIsRestarted_then_itBecomesLeaderAndAppliesPreviouslyCommittedMemberList() {
        group = LocalRaftGroup.newBuilder(3)
                              .enableNewTermOperation()
                              .setRaftStoreFactory(IN_MEMORY_RAFT_STATE_STORE_FACTORY)
                              .start();

        RaftNodeImpl leader = group.waitUntilLeaderElected();
        List<RaftNodeImpl> followers = group.getNodesExcept(leader.getLocalEndpoint());
        RaftNodeImpl removedFollower = followers.get(0);
        RaftNodeImpl runningFollower = followers.get(1);

        group.terminateNode(removedFollower.getLocalEndpoint());
        leader.replicate(applyValue("val")).join();
        leader.changeMembership(removedFollower.getLocalEndpoint(), REMOVE_MEMBER, 0).join();

        RaftEndpoint terminatedEndpoint = leader.getLocalEndpoint();
        InMemoryRaftStore stateStore = getRaftStore(leader);
        RestoredRaftState terminatedState = stateStore.toRestoredRaftState();

        // Block voting between followers
        // to avoid a leader election before leader restarts.
        blockVotingBetweenFollowers();

        group.terminateNode(terminatedEndpoint);
        RaftNodeImpl restartedNode = group.restoreNode(terminatedState, stateStore);

        RaftNodeImpl newLeader = group.waitUntilLeaderElected();
        assertThat(newLeader).isSameAs(restartedNode);

        eventually(() -> {
            assertThat(getCommitIndex(restartedNode)).isEqualTo(getCommitIndex(runningFollower));
            assertThat(getCommittedGroupMembers(restartedNode).getMembersList()).isEqualTo(
                    getCommittedGroupMembers(runningFollower).getMembersList());
            assertThat(getEffectiveGroupMembers(restartedNode).getMembersList()).isEqualTo(
                    getEffectiveGroupMembers(runningFollower).getMembersList());
        });
    }

    @Test(timeout = 300_000) public void when_followerIsRestarted_then_itAppliesPreviouslyCommittedMemberList() {
        RaftConfig config = RaftConfig.newBuilder().setLeaderHeartbeatPeriodSecs(1).setLeaderHeartbeatTimeoutSecs(30).build();
        group = LocalRaftGroup.newBuilder(3)
                              .setConfig(config)
                              .enableNewTermOperation()
                              .setRaftStoreFactory(IN_MEMORY_RAFT_STATE_STORE_FACTORY)
                              .start();

        RaftNodeImpl leader = group.waitUntilLeaderElected();
        List<RaftNodeImpl> followers = group.getNodesExcept(leader.getLocalEndpoint());
        RaftNodeImpl removedFollower = followers.get(0);
        RaftNodeImpl terminatedFollower = followers.get(1);

        group.terminateNode(removedFollower.getLocalEndpoint());
        leader.replicate(applyValue("val")).join();
        leader.changeMembership(removedFollower.getLocalEndpoint(), REMOVE_MEMBER, 0).join();

        RaftEndpoint terminatedEndpoint = terminatedFollower.getLocalEndpoint();
        group.terminateNode(terminatedEndpoint);

        InMemoryRaftStore stateStore = getRaftStore(terminatedFollower);
        RestoredRaftState terminatedState = stateStore.toRestoredRaftState();

        RaftNodeImpl restartedNode = group.restoreNode(terminatedState, stateStore);

        eventually(() -> {
            assertThat(getCommitIndex(restartedNode)).isEqualTo(getCommitIndex(leader));
            assertThat(getLastApplied(restartedNode)).isEqualTo(getLastApplied(leader));
            assertThat(getCommittedGroupMembers(restartedNode).getMembersList()).isEqualTo(
                    getCommittedGroupMembers(leader).getMembersList());
            assertThat(getEffectiveGroupMembers(restartedNode).getMembersList()).isEqualTo(
                    getEffectiveGroupMembers(leader).getMembersList());
        });
    }

    @Test(timeout = 300_000) public void when_learnerIsRestarted_then_itAppliesItsPromotion() {
        RaftConfig config = RaftConfig.newBuilder().setLeaderHeartbeatPeriodSecs(1).setLeaderHeartbeatTimeoutSecs(30).build();
        group = LocalRaftGroup.newBuilder(3, 2)
                              .setConfig(config)
                              .enableNewTermOperation()
                              .setRaftStoreFactory(IN_MEMORY_RAFT_STATE_STORE_FACTORY)
                              .start();

        RaftNodeImpl leader = group.waitUntilLeaderElected();

        RaftNodeImpl terminatedLearner = null;
        for (RaftNodeImpl node : group.<RaftNodeImpl>getNodesExcept(leader.getLocalEndpoint())) {
            if (getRole(node) == LEARNER) {
                terminatedLearner = node;
                break;
            }
        }
        assertNotNull(terminatedLearner);

        leader.replicate(applyValue("val")).join();
        leader.changeMembership(terminatedLearner.getLocalEndpoint(), ADD_OR_PROMOTE_TO_FOLLOWER, 0).join();

        group.terminateNode(terminatedLearner.getLocalEndpoint());

        InMemoryRaftStore stateStore = getRaftStore(terminatedLearner);
        RestoredRaftState terminatedState = stateStore.toRestoredRaftState();

        RaftNodeImpl restartedNode = group.restoreNode(terminatedState, stateStore);

        eventually(() -> {
            assertThat(getRole(restartedNode)).isEqualTo(FOLLOWER);
            assertThat(getCommitIndex(restartedNode)).isEqualTo(getCommitIndex(leader));
            assertThat(getLastApplied(restartedNode)).isEqualTo(getLastApplied(leader));
            assertThat(getCommittedGroupMembers(restartedNode).getMembersList()).isEqualTo(
                    getCommittedGroupMembers(leader).getMembersList());
            assertThat(getEffectiveGroupMembers(restartedNode).getMembersList()).isEqualTo(
                    getEffectiveGroupMembers(leader).getMembersList());
        });
    }

    @Test(timeout = 300_000)
    public void when_leaderIsRestarted_then_itBecomesLeaderAndAppliesPreviouslyCommittedMemberListViaSnapshot() {
        int commitCountToTakeSnapshot = 50;
        RaftConfig config = RaftConfig.newBuilder().setCommitCountToTakeSnapshot(commitCountToTakeSnapshot).build();
        group = LocalRaftGroup.newBuilder(3)
                              .setConfig(config)
                              .enableNewTermOperation()
                              .setRaftStoreFactory(IN_MEMORY_RAFT_STATE_STORE_FACTORY)
                              .start();

        RaftNodeImpl leader = group.waitUntilLeaderElected();
        List<RaftNodeImpl> followers = group.getNodesExcept(leader.getLocalEndpoint());
        RaftNodeImpl removedFollower = followers.get(0);
        RaftNodeImpl runningFollower = followers.get(1);

        group.terminateNode(removedFollower.getLocalEndpoint());
        leader.replicate(applyValue("val")).join();
        leader.changeMembership(removedFollower.getLocalEndpoint(), REMOVE_MEMBER, 0).join();

        while (getSnapshotEntry(leader).getIndex() == 0) {
            leader.replicate(applyValue("val")).join();
        }

        RaftEndpoint terminatedEndpoint = leader.getLocalEndpoint();
        InMemoryRaftStore stateStore = getRaftStore(leader);
        RestoredRaftState terminatedState = stateStore.toRestoredRaftState();

        // Block voting between followers
        // to avoid a leader election before leader restarts.
        blockVotingBetweenFollowers();

        group.terminateNode(terminatedEndpoint);
        RaftNodeImpl restartedNode = group.restoreNode(terminatedState, stateStore);

        RaftNodeImpl newLeader = group.waitUntilLeaderElected();
        assertThat(newLeader).isSameAs(restartedNode);

        eventually(() -> {
            assertThat(getCommitIndex(restartedNode)).isEqualTo(getCommitIndex(runningFollower));
            assertThat(getCommittedGroupMembers(restartedNode).getMembersList()).isEqualTo(
                    getCommittedGroupMembers(runningFollower).getMembersList());
            assertThat(getEffectiveGroupMembers(restartedNode).getMembersList()).isEqualTo(
                    getEffectiveGroupMembers(runningFollower).getMembersList());
        });
    }

    @Test(timeout = 300_000) public void when_followerIsRestarted_then_itAppliesPreviouslyCommittedMemberListViaSnapshot() {
        int commitCountToTakeSnapshot = 50;
        RaftConfig config = RaftConfig.newBuilder()
                                      .setCommitCountToTakeSnapshot(commitCountToTakeSnapshot)
                                      .setLeaderHeartbeatPeriodSecs(1)
                                      .setLeaderHeartbeatTimeoutSecs(30)
                                      .build();
        group = LocalRaftGroup.newBuilder(3)
                              .setConfig(config)
                              .enableNewTermOperation()
                              .setRaftStoreFactory(IN_MEMORY_RAFT_STATE_STORE_FACTORY)
                              .start();

        RaftNodeImpl leader = group.waitUntilLeaderElected();
        List<RaftNodeImpl> followers = group.getNodesExcept(leader.getLocalEndpoint());
        RaftNodeImpl removedFollower = followers.get(0);
        RaftNodeImpl terminatedFollower = followers.get(1);

        group.terminateNode(removedFollower.getLocalEndpoint());
        leader.replicate(applyValue("val")).join();
        leader.changeMembership(removedFollower.getLocalEndpoint(), REMOVE_MEMBER, 0).join();

        while (getSnapshotEntry(terminatedFollower).getIndex() == 0) {
            leader.replicate(applyValue("val")).join();
        }

        RaftEndpoint terminatedEndpoint = terminatedFollower.getLocalEndpoint();
        group.terminateNode(terminatedEndpoint);

        InMemoryRaftStore stateStore = getRaftStore(terminatedFollower);
        RestoredRaftState terminatedState = stateStore.toRestoredRaftState();

        RaftNodeImpl restartedNode = group.restoreNode(terminatedState, stateStore);

        eventually(() -> {
            assertThat(getCommitIndex(restartedNode)).isEqualTo(getCommitIndex(leader));
            assertThat(getLastApplied(restartedNode)).isEqualTo(getLastApplied(leader));
            assertThat(getCommittedGroupMembers(restartedNode).getMembersList()).isEqualTo(
                    getCommittedGroupMembers(leader).getMembersList());
            assertThat(getEffectiveGroupMembers(restartedNode).getMembersList()).isEqualTo(
                    getEffectiveGroupMembers(leader).getMembersList());
        });
    }

    @Test(timeout = 300_000) public void when_learnerIsRestarted_then_itAppliesItsPromotionViaSnapshot() {
        int commitCountToTakeSnapshot = 50;
        RaftConfig config = RaftConfig.newBuilder()
                                      .setCommitCountToTakeSnapshot(commitCountToTakeSnapshot)
                                      .setLeaderHeartbeatPeriodSecs(1)
                                      .setLeaderHeartbeatTimeoutSecs(30)
                                      .build();
        group = LocalRaftGroup.newBuilder(3, 2)
                              .setConfig(config)
                              .enableNewTermOperation()
                              .setRaftStoreFactory(IN_MEMORY_RAFT_STATE_STORE_FACTORY)
                              .start();

        RaftNodeImpl leader = group.waitUntilLeaderElected();

        RaftNodeImpl terminatedLearner = null;
        for (RaftNodeImpl node : group.<RaftNodeImpl>getNodesExcept(leader.getLocalEndpoint())) {
            if (getRole(node) == LEARNER) {
                terminatedLearner = node;
                break;
            }
        }
        assertNotNull(terminatedLearner);

        leader.replicate(applyValue("val")).join();
        leader.changeMembership(terminatedLearner.getLocalEndpoint(), ADD_OR_PROMOTE_TO_FOLLOWER, 0).join();

        while (getSnapshotEntry(terminatedLearner).getIndex() == 0) {
            leader.replicate(applyValue("val")).join();
        }

        group.terminateNode(terminatedLearner.getLocalEndpoint());

        InMemoryRaftStore stateStore = getRaftStore(terminatedLearner);
        RestoredRaftState terminatedState = stateStore.toRestoredRaftState();

        RaftNodeImpl restartedNode = group.restoreNode(terminatedState, stateStore);

        eventually(() -> {
            assertThat(getRole(restartedNode)).isEqualTo(FOLLOWER);
            assertThat(getCommitIndex(restartedNode)).isEqualTo(getCommitIndex(leader));
            assertThat(getLastApplied(restartedNode)).isEqualTo(getLastApplied(leader));
            assertThat(getCommittedGroupMembers(restartedNode).getMembersList()).isEqualTo(
                    getCommittedGroupMembers(leader).getMembersList());
            assertThat(getEffectiveGroupMembers(restartedNode).getMembersList()).isEqualTo(
                    getEffectiveGroupMembers(leader).getMembersList());
        });
    }

    @Test(timeout = 300_000) public void when_learnerIsRestarted_then_itRestartsAsNonVotingMember() {
        group = LocalRaftGroup.newBuilder(3)
                              .enableNewTermOperation()
                              .setRaftStoreFactory(IN_MEMORY_RAFT_STATE_STORE_FACTORY)
                              .start();

        RaftNodeImpl leader = group.waitUntilLeaderElected();

        RaftNodeImpl newNode = group.createNewNode();

        leader.changeMembership(newNode.getLocalEndpoint(), MembershipChangeMode.ADD_LEARNER, 0).join();

        eventually(() -> assertThat(getCommitIndex(newNode)).isEqualTo(getCommitIndex(leader)));

        group.terminateNode(newNode.getLocalEndpoint());

        InMemoryRaftStore stateStore = getRaftStore(newNode);
        RestoredRaftState terminatedState = stateStore.toRestoredRaftState();

        RaftNodeImpl restartedNode = group.restoreNode(terminatedState, stateStore);

        eventually(() -> assertThat(getCommitIndex(restartedNode)).isEqualTo(getCommitIndex(leader)));
        assertThat(getRole(restartedNode)).isEqualTo(LEARNER);
    }

    @Test(timeout = 300_000) public void when_promotedMemberIsRestarted_then_itRestartsAsVotingMember() {
        group = LocalRaftGroup.newBuilder(3)
                              .enableNewTermOperation()
                              .setRaftStoreFactory(IN_MEMORY_RAFT_STATE_STORE_FACTORY)
                              .start();

        RaftNodeImpl leader = group.waitUntilLeaderElected();

        RaftNodeImpl newNode = group.createNewNode();

        Ordered<RaftGroupMembers> membershipChangeResult = leader.changeMembership(newNode.getLocalEndpoint(),
                                                                                   MembershipChangeMode.ADD_LEARNER, 0).join();
        leader.changeMembership(newNode.getLocalEndpoint(), ADD_OR_PROMOTE_TO_FOLLOWER, membershipChangeResult.getCommitIndex())
              .join();

        eventually(() -> assertThat(getCommitIndex(newNode)).isEqualTo(getCommitIndex(leader)));

        group.terminateNode(newNode.getLocalEndpoint());

        InMemoryRaftStore stateStore = getRaftStore(newNode);
        RestoredRaftState terminatedState = stateStore.toRestoredRaftState();

        RaftNodeImpl restartedNode = group.restoreNode(terminatedState, stateStore);

        eventually(() -> assertThat(getCommitIndex(restartedNode)).isEqualTo(getCommitIndex(leader)));
        assertThat(getRole(restartedNode)).isEqualTo(RaftRole.FOLLOWER);
    }

    @Test(timeout = 300_000) public void when_promotingMemberIsRestarted_then_itRestartsAsVotingMember() {
        group = LocalRaftGroup.newBuilder(3)
                              .enableNewTermOperation()
                              .setRaftStoreFactory(IN_MEMORY_RAFT_STATE_STORE_FACTORY)
                              .start();

        RaftNodeImpl leader = group.waitUntilLeaderElected();
        List<RaftNodeImpl> followers = group.getNodesExcept(leader.getLocalEndpoint());

        RaftNodeImpl newNode = group.createNewNode();
        Ordered<RaftGroupMembers> membershipChangeResult = leader.changeMembership(newNode.getLocalEndpoint(),
                                                                                   MembershipChangeMode.ADD_LEARNER, 0).join();

        for (RaftNodeImpl follower : followers) {
            group.dropMessagesTo(leader.getLocalEndpoint(), follower.getLocalEndpoint(), AppendEntriesRequest.class);
        }

        leader.changeMembership(newNode.getLocalEndpoint(), ADD_OR_PROMOTE_TO_FOLLOWER, membershipChangeResult.getCommitIndex());

        eventually(() -> assertThat(getRole(newNode)).isEqualTo(RaftRole.FOLLOWER));

        group.terminateNode(newNode.getLocalEndpoint());

        InMemoryRaftStore stateStore = getRaftStore(newNode);
        RestoredRaftState terminatedState = stateStore.toRestoredRaftState();

        RaftNodeImpl restartedNode = group.restoreNode(terminatedState, stateStore);

        eventually(() -> assertThat(getCommitIndex(restartedNode)).isEqualTo(getCommitIndex(leader)));
        assertThat(getRole(restartedNode)).isEqualTo(RaftRole.FOLLOWER);
    }

    // TODO [basri] add snapshot chunk truncation tests

}
