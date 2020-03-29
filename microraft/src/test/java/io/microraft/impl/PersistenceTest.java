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

import io.microraft.RaftConfig;
import io.microraft.RaftEndpoint;
import io.microraft.impl.local.InMemoryRaftStore;
import io.microraft.impl.local.LocalRaftEndpoint;
import io.microraft.impl.local.LocalRaftGroup;
import io.microraft.impl.local.SimpleStateMachine;
import io.microraft.impl.util.BaseTest;
import io.microraft.model.log.LogEntry;
import io.microraft.model.log.SnapshotEntry;
import io.microraft.model.message.AppendEntriesRequest;
import io.microraft.model.message.PreVoteRequest;
import io.microraft.persistence.RaftStore;
import io.microraft.persistence.RestoredRaftState;
import org.junit.After;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.BiFunction;

import static io.microraft.MembershipChangeMode.REMOVE;
import static io.microraft.RaftConfig.DEFAULT_RAFT_CONFIG;
import static io.microraft.impl.local.SimpleStateMachine.apply;
import static io.microraft.impl.util.AssertionUtils.eventually;
import static io.microraft.impl.util.RaftTestUtils.TEST_RAFT_CONFIG;
import static io.microraft.impl.util.RaftTestUtils.getCommitIndex;
import static io.microraft.impl.util.RaftTestUtils.getCommittedGroupMembers;
import static io.microraft.impl.util.RaftTestUtils.getEffectiveGroupMembers;
import static io.microraft.impl.util.RaftTestUtils.getLastApplied;
import static io.microraft.impl.util.RaftTestUtils.getRaftStore;
import static io.microraft.impl.util.RaftTestUtils.getRestoredState;
import static io.microraft.impl.util.RaftTestUtils.getSnapshotEntry;
import static io.microraft.impl.util.RaftTestUtils.getTerm;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

public class PersistenceTest
        extends BaseTest {

    private static final BiFunction<RaftEndpoint, RaftConfig, RaftStore> RAFT_STATE_STORE_FACTORY = (endpoint, config) -> {
        int maxUncommittedEntryCount = config.getMaxUncommittedLogEntryCount();
        int commitIndexAdvanceCountToTakeSnapshot = config.getCommitCountToTakeSnapshot();
        int maxNumberOfLogsToKeepAfterSnapshot = (int) (commitIndexAdvanceCountToTakeSnapshot * 0.5);
        int logCapacity = commitIndexAdvanceCountToTakeSnapshot + maxUncommittedEntryCount + maxNumberOfLogsToKeepAfterSnapshot;
        return new InMemoryRaftStore(logCapacity);
    };

    private LocalRaftGroup group;

    @After
    public void destroy() {
        if (group != null) {
            group.destroy();
        }
    }

    @Test
    public void testTermAndVoteArePersisted() {
        group = new LocalRaftGroup(3, DEFAULT_RAFT_CONFIG, false, LocalRaftEndpoint::newEndpoint, RAFT_STATE_STORE_FACTORY, null);
        group.start();

        RaftNodeImpl leader = group.waitUntilLeaderElected();
        RaftNodeImpl[] followers = group.getNodesExcept(leader.getLocalEndpoint());
        Set<RaftEndpoint> endpoints = new HashSet<>();
        for (RaftNodeImpl node : group.getNodes()) {
            endpoints.add(node.getLocalEndpoint());
        }

        int term1 = getTerm(leader);
        eventually(() -> {
            for (RaftNodeImpl node : group.getNodes()) {
                RestoredRaftState restoredState = getRestoredState(node);
                assertEquals(node.getLocalEndpoint(), restoredState.getLocalEndpoint());
                assertEquals(term1, restoredState.getTerm());
                assertEquals(endpoints, restoredState.getInitialMembers());
            }
        });

        group.terminateNode(leader.getLocalEndpoint());

        eventually(() -> {
            for (RaftNodeImpl node : followers) {
                RaftEndpoint l = node.getLeaderEndpoint();
                assertNotNull(l);
                assertNotEquals(leader.getLeaderEndpoint(), l);
            }
        });

        RaftNodeImpl newLeader = group.waitUntilLeaderElected();
        int term2 = getTerm(newLeader);

        eventually(() -> {
            for (RaftNodeImpl node : followers) {
                RestoredRaftState restoredState = getRestoredState(node);
                assertEquals(term2, restoredState.getTerm());
                assertEquals(newLeader.getLeaderEndpoint(), restoredState.getVotedEndpoint());
            }
        });
    }

    @Test
    public void testCommittedEntriesArePersisted()
            throws Exception {
        group = new LocalRaftGroup(3, DEFAULT_RAFT_CONFIG, false, LocalRaftEndpoint::newEndpoint, RAFT_STATE_STORE_FACTORY, null);
        group.start();

        RaftNodeImpl leader = group.waitUntilLeaderElected();
        int count = 10;
        for (int i = 0; i < count; i++) {
            leader.replicate(apply("val" + i)).get();
        }

        eventually(() -> {
            for (RaftNodeImpl node : group.getNodes()) {
                RestoredRaftState restoredState = getRestoredState(node);
                List<LogEntry> entries = restoredState.getLogEntries();
                assertEquals(count, entries.size());
                for (int i = 0; i < count; i++) {
                    assertEquals(i + 1, entries.get(i).getIndex());
                }
            }
        });
    }

    @Test
    public void testUncommittedEntriesArePersisted() {
        group = new LocalRaftGroup(3, DEFAULT_RAFT_CONFIG, false, LocalRaftEndpoint::newEndpoint, RAFT_STATE_STORE_FACTORY, null);
        group.start();

        RaftNodeImpl leader = group.waitUntilLeaderElected();
        RaftNodeImpl[] followers = group.getNodesExcept(leader.getLocalEndpoint());
        RaftNodeImpl responsiveFollower = followers[0];

        for (int i = 1; i < followers.length; i++) {
            group.dropMessagesToMember(leader.getLocalEndpoint(), followers[i].getLocalEndpoint(), AppendEntriesRequest.class);
        }

        int count = 10;
        for (int i = 0; i < count; i++) {
            leader.replicate(apply("val" + i));
        }

        eventually(() -> {
            for (RaftNodeImpl node : Arrays.asList(leader, responsiveFollower)) {
                RestoredRaftState restoredState = getRestoredState(node);
                List<LogEntry> entries = restoredState.getLogEntries();
                assertEquals(count, entries.size());
                for (int i = 0; i < count; i++) {
                    assertEquals(i + 1, entries.get(i).getIndex());
                }
            }
        });
    }

    @Test
    public void testSnapshotIsPersisted()
            throws Exception {
        int committedEntryCountToTakeSnapshot = 50;
        RaftConfig config = RaftConfig.newBuilder().setCommitCountToTakeSnapshot(committedEntryCountToTakeSnapshot).build();
        group = new LocalRaftGroup(3, config, false, LocalRaftEndpoint::newEndpoint, RAFT_STATE_STORE_FACTORY, null);
        group.start();

        RaftNodeImpl leader = group.waitUntilLeaderElected();
        for (int i = 0; i < committedEntryCountToTakeSnapshot; i++) {
            leader.replicate(apply("val" + i)).get();
        }

        eventually(() -> {
            assertEquals(committedEntryCountToTakeSnapshot, getSnapshotEntry(leader).getIndex());

            for (RaftNodeImpl node : group.getNodes()) {
                RestoredRaftState restoredState = getRestoredState(node);
                SnapshotEntry snapshot = restoredState.getSnapshotEntry();
                assertNotNull(snapshot);
                assertEquals(committedEntryCountToTakeSnapshot, snapshot.getIndex());
                assertNotNull(snapshot.getOperation());
            }
        });
    }

    @Test
    public void when_leaderAppendEntriesInMinoritySplit_then_itTruncatesEntriesOnStore()
            throws Exception {
        group = new LocalRaftGroup(3, TEST_RAFT_CONFIG, false, LocalRaftEndpoint::newEndpoint, RAFT_STATE_STORE_FACTORY, null);
        group.start();

        RaftNodeImpl leader = group.waitUntilLeaderElected();

        leader.replicate(apply("val1")).get();

        eventually(() -> {
            for (RaftNodeImpl raftNode : group.getNodes()) {
                assertEquals(1, getCommitIndex(raftNode));
            }
        });

        RaftNodeImpl[] followers = group.getNodesExcept(leader.getLocalEndpoint());
        group.split(group.getIndexOfRunning(leader.getLocalEndpoint()));

        for (int i = 0; i < 10; i++) {
            leader.replicate(apply("isolated" + i));
        }

        eventually(() -> {
            for (RaftNodeImpl raftNode : followers) {
                RaftEndpoint leaderEndpoint = raftNode.getLeaderEndpoint();
                assertNotNull(leaderEndpoint);
                assertNotEquals(leader.getLocalEndpoint(), leaderEndpoint);
            }
        });

        eventually(() -> {
            RestoredRaftState restoredState = getRestoredState(leader);
            assertEquals(11, restoredState.getLogEntries().size());
        });

        RaftNodeImpl newLeader = group.getNode(followers[0].getLeaderEndpoint());
        for (int i = 0; i < 10; i++) {
            newLeader.replicate(apply("valNew" + i)).get();
        }

        eventually(() -> {
            for (RaftNodeImpl raftNode : followers) {
                assertEquals(11, getCommitIndex(raftNode));
            }
        });

        group.merge();

        RaftNodeImpl finalLeader = group.waitUntilLeaderElected();

        assertNotEquals(leader.getLocalEndpoint(), finalLeader.getLocalEndpoint());

        eventually(() -> {
            RestoredRaftState state = getRestoredState(leader);
            assertEquals(11, state.getLogEntries().size());
        });
    }

    @Test
    public void when_leaderIsRestarted_then_itBecomesFollowerAndRestoresItsRaftState()
            throws Exception {
        group = new LocalRaftGroup(3, TEST_RAFT_CONFIG, true, LocalRaftEndpoint::newEndpoint, RAFT_STATE_STORE_FACTORY, null);
        group.start();

        RaftNodeImpl leader = group.waitUntilLeaderElected();
        int count = 10;
        for (int i = 0; i < count; i++) {
            leader.replicate(apply("val" + i)).get();
        }

        RaftEndpoint terminatedEndpoint = leader.getLocalEndpoint();
        InMemoryRaftStore stateStore = getRaftStore(leader);
        RestoredRaftState terminatedState = stateStore.toRestoredRaftState();

        group.terminateNode(terminatedEndpoint);
        RaftNodeImpl newLeader = group.waitUntilLeaderElected();
        RaftNodeImpl restartedNode = group.createNewRaftNode(terminatedState, stateStore);

        assertEquals(new ArrayList<>(getCommittedGroupMembers(newLeader).getMembers()),
                new ArrayList<>(getCommittedGroupMembers(restartedNode).getMembers()));
        assertEquals(new ArrayList<>(getEffectiveGroupMembers(newLeader).getMembers()),
                new ArrayList<>(getEffectiveGroupMembers(restartedNode).getMembers()));

        eventually(() -> {
            assertEquals(newLeader.getLocalEndpoint(), restartedNode.getLeaderEndpoint());
            assertEquals(getTerm(newLeader), getTerm(restartedNode));
            assertEquals(getCommitIndex(newLeader), getCommitIndex(restartedNode));
            assertEquals(getLastApplied(newLeader), getLastApplied(restartedNode));
            SimpleStateMachine stateMachine = group.getRuntime(restartedNode.getLocalEndpoint()).getStateMachine();
            Object[] values = stateMachine.valuesArray();
            assertNotNull(values);
            assertEquals(count, values.length);
            for (int i = 0; i < count; i++) {
                assertEquals("val" + i, values[i]);
            }
        });
    }

    @Test
    public void when_leaderIsRestarted_then_itRestoresItsRaftStateAndBecomesLeader()
            throws Exception {
        group = new LocalRaftGroup(3, DEFAULT_RAFT_CONFIG, true, LocalRaftEndpoint::newEndpoint, RAFT_STATE_STORE_FACTORY, null);
        group.start();

        RaftNodeImpl leader = group.waitUntilLeaderElected();
        int count = 10;
        for (int i = 0; i < count; i++) {
            leader.replicate(apply("val" + i)).get();
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
        RaftNodeImpl restartedNode = group.createNewRaftNode(terminatedState, stateStore);

        RaftNodeImpl newLeader = group.waitUntilLeaderElected();
        assertSame(newLeader, restartedNode);

        eventually(() -> {
            assertTrue(getTerm(restartedNode) > term);
            assertEquals(commitIndex + 1, getCommitIndex(restartedNode));
            SimpleStateMachine stateMachine = group.getRuntime(restartedNode.getLocalEndpoint()).getStateMachine();
            Object[] values = stateMachine.valuesArray();
            assertNotNull(values);
            assertEquals(count, values.length);
            for (int i = 0; i < count; i++) {
                assertEquals("val" + i, values[i]);
            }
        });
    }

    private void blockVotingBetweenFollowers() {
        RaftEndpoint[] endpoints = group.getFollowerEndpoints();
        for (RaftEndpoint endpoint : endpoints) {
            if (group.isRunning(endpoint)) {
                group.dropMessagesToAll(endpoint, PreVoteRequest.class);
            }
        }
    }

    @Test
    public void when_followerIsRestarted_then_itRestoresItsRaftState()
            throws Exception {
        group = new LocalRaftGroup(3, DEFAULT_RAFT_CONFIG, false, LocalRaftEndpoint::newEndpoint, RAFT_STATE_STORE_FACTORY, null);
        group.start();

        RaftNodeImpl leader = group.waitUntilLeaderElected();
        RaftNodeImpl terminatedFollower = group.getAnyFollowerNode();
        int count = 10;
        for (int i = 0; i < count; i++) {
            leader.replicate(apply("val" + i)).get();
        }

        eventually(() -> assertEquals(getCommitIndex(leader), getCommitIndex(terminatedFollower)));

        RaftEndpoint terminatedEndpoint = terminatedFollower.getLocalEndpoint();
        InMemoryRaftStore stateStore = getRaftStore(terminatedFollower);
        RestoredRaftState terminatedState = stateStore.toRestoredRaftState();

        group.terminateNode(terminatedEndpoint);
        leader.replicate(apply("val" + count)).get();

        RaftNodeImpl restartedNode = group.createNewRaftNode(terminatedState, stateStore);

        assertEquals(new ArrayList<>(getCommittedGroupMembers(leader).getMembers()),
                new ArrayList<>(getCommittedGroupMembers(restartedNode).getMembers()));
        assertEquals(new ArrayList<>(getEffectiveGroupMembers(leader).getMembers()),
                new ArrayList<>(getEffectiveGroupMembers(restartedNode).getMembers()));

        eventually(() -> {
            assertEquals(leader.getLocalEndpoint(), restartedNode.getLeaderEndpoint());
            assertEquals(getTerm(leader), getTerm(restartedNode));
            assertEquals(getCommitIndex(leader), getCommitIndex(restartedNode));
            assertEquals(getLastApplied(leader), getLastApplied(restartedNode));
            SimpleStateMachine stateMachine = group.getRuntime(restartedNode.getLocalEndpoint()).getStateMachine();
            Object[] values = stateMachine.valuesArray();
            assertNotNull(values);
            assertEquals(count + 1, values.length);
            for (int i = 0; i <= count; i++) {
                assertEquals("val" + i, values[i]);
            }
        });
    }

    @Test
    public void when_leaderIsRestarted_then_itBecomesFollowerAndRestoresItsRaftStateWithSnapshot()
            throws Exception {
        int commitCountToTakeSnapshot = 50;
        RaftConfig config = RaftConfig.newBuilder().setLeaderElectionTimeoutMillis(2000).setLeaderHeartbeatPeriodSecs(1)
                                      .setLeaderHeartbeatTimeoutSecs(5).setCommitCountToTakeSnapshot(commitCountToTakeSnapshot)
                                      .build();
        group = new LocalRaftGroup(3, config, true, LocalRaftEndpoint::newEndpoint, RAFT_STATE_STORE_FACTORY, null);
        group.start();

        RaftNodeImpl leader = group.waitUntilLeaderElected();

        for (int i = 0; i <= commitCountToTakeSnapshot; i++) {
            leader.replicate(apply("val" + i)).get();
        }

        assertTrue(getSnapshotEntry(leader).getIndex() > 0);

        RaftEndpoint terminatedEndpoint = leader.getLocalEndpoint();
        InMemoryRaftStore stateStore = getRaftStore(leader);
        RestoredRaftState terminatedState = stateStore.toRestoredRaftState();

        group.terminateNode(terminatedEndpoint);
        RaftNodeImpl newLeader = group.waitUntilLeaderElected();
        RaftNodeImpl restartedNode = group.createNewRaftNode(terminatedState, stateStore);

        assertEquals(new ArrayList<>(getCommittedGroupMembers(newLeader).getMembers()),
                new ArrayList<>(getCommittedGroupMembers(restartedNode).getMembers()));
        assertEquals(new ArrayList<>(getEffectiveGroupMembers(newLeader).getMembers()),
                new ArrayList<>(getEffectiveGroupMembers(restartedNode).getMembers()));

        eventually(() -> {
            assertEquals(newLeader.getLocalEndpoint(), restartedNode.getLeaderEndpoint());
            assertEquals(getTerm(newLeader), getTerm(restartedNode));
            assertEquals(getCommitIndex(newLeader), getCommitIndex(restartedNode));
            assertEquals(getLastApplied(newLeader), getLastApplied(restartedNode));
            SimpleStateMachine stateMachine = group.getRuntime(restartedNode.getLocalEndpoint()).getStateMachine();
            Object[] values = stateMachine.valuesArray();
            assertNotNull(values);
            assertEquals(commitCountToTakeSnapshot + 1, values.length);
            for (int i = 0; i <= commitCountToTakeSnapshot; i++) {
                assertEquals("val" + i, values[i]);
            }
        });
    }

    @Test
    public void when_followerIsRestarted_then_itRestoresItsRaftStateWithSnapshot()
            throws Exception {
        int commitCountToTakeSnapshot = 50;
        RaftConfig config = RaftConfig.newBuilder().setCommitCountToTakeSnapshot(commitCountToTakeSnapshot).build();
        group = new LocalRaftGroup(3, config, true, LocalRaftEndpoint::newEndpoint, RAFT_STATE_STORE_FACTORY, null);
        group.start();

        RaftNodeImpl leader = group.waitUntilLeaderElected();

        for (int i = 0; i <= commitCountToTakeSnapshot; i++) {
            leader.replicate(apply("val" + i)).get();
        }

        eventually(() -> {
            for (RaftNodeImpl node : group.getNodes()) {
                assertTrue(getSnapshotEntry(node).getIndex() > 0);
            }
        });

        RaftNodeImpl terminatedFollower = group.getAnyFollowerNode();
        RaftEndpoint terminatedEndpoint = terminatedFollower.getLocalEndpoint();
        InMemoryRaftStore stateStore = getRaftStore(terminatedFollower);
        RestoredRaftState terminatedState = stateStore.toRestoredRaftState();

        group.terminateNode(terminatedEndpoint);

        leader.replicate(apply("val" + (commitCountToTakeSnapshot + 1))).get();

        RaftNodeImpl restartedNode = group.createNewRaftNode(terminatedState, stateStore);

        assertEquals(new ArrayList<>(getCommittedGroupMembers(leader).getMembers()),
                new ArrayList<>(getCommittedGroupMembers(restartedNode).getMembers()));
        assertEquals(new ArrayList<>(getEffectiveGroupMembers(leader).getMembers()),
                new ArrayList<>(getEffectiveGroupMembers(restartedNode).getMembers()));

        eventually(() -> {
            assertEquals(leader.getLocalEndpoint(), restartedNode.getLeaderEndpoint());
            assertEquals(getTerm(leader), getTerm(restartedNode));
            assertEquals(getCommitIndex(leader), getCommitIndex(restartedNode));
            assertEquals(getLastApplied(leader), getLastApplied(restartedNode));
            SimpleStateMachine stateMachine = group.getRuntime(restartedNode.getLocalEndpoint()).getStateMachine();
            Object[] values = stateMachine.valuesArray();
            assertNotNull(values);
            assertEquals(commitCountToTakeSnapshot + 2, values.length);
            for (int i = 0; i <= commitCountToTakeSnapshot + 1; i++) {
                assertEquals("val" + i, values[i]);
            }
        });
    }

    @Test
    public void when_leaderIsRestarted_then_itRestoresItsRaftStateWithSnapshotAndBecomesLeader()
            throws Exception {
        int commitCountToTakeSnapshot = 50;
        RaftConfig config = RaftConfig.newBuilder().setCommitCountToTakeSnapshot(commitCountToTakeSnapshot).build();
        group = new LocalRaftGroup(3, config, true, LocalRaftEndpoint::newEndpoint, RAFT_STATE_STORE_FACTORY, null);
        group.start();

        RaftNodeImpl leader = group.waitUntilLeaderElected();

        for (int i = 0; i <= commitCountToTakeSnapshot; i++) {
            leader.replicate(apply("val" + i)).get();
        }

        assertTrue(getSnapshotEntry(leader).getIndex() > 0);
        int term = getTerm(leader);
        long commitIndex = getCommitIndex(leader);

        RaftEndpoint terminatedEndpoint = leader.getLocalEndpoint();
        InMemoryRaftStore stateStore = getRaftStore(leader);
        RestoredRaftState terminatedState = stateStore.toRestoredRaftState();

        // Block voting between followers
        // to avoid a leader election before leader restarts.
        blockVotingBetweenFollowers();

        group.terminateNode(terminatedEndpoint);
        RaftNodeImpl restartedNode = group.createNewRaftNode(terminatedState, stateStore);

        RaftNodeImpl newLeader = group.waitUntilLeaderElected();
        assertSame(restartedNode, newLeader);

        eventually(() -> {
            assertTrue(getTerm(restartedNode) > term);
            assertEquals(commitIndex + 1, getCommitIndex(restartedNode));
            SimpleStateMachine stateMachine = group.getRuntime(restartedNode.getLocalEndpoint()).getStateMachine();
            Object[] values = stateMachine.valuesArray();
            assertNotNull(values);
            assertEquals(commitCountToTakeSnapshot + 1, values.length);
            for (int i = 0; i <= commitCountToTakeSnapshot; i++) {
                assertEquals("val" + i, values[i]);
            }
        });
    }

    @Test
    public void when_leaderIsRestarted_then_itBecomesLeaderAndAppliesPreviouslyCommittedMemberList()
            throws Exception {
        group = new LocalRaftGroup(3, DEFAULT_RAFT_CONFIG, true, LocalRaftEndpoint::newEndpoint, RAFT_STATE_STORE_FACTORY, null);
        group.start();

        RaftNodeImpl leader = group.waitUntilLeaderElected();
        RaftNodeImpl[] followers = group.getNodesExcept(leader.getLocalEndpoint());
        RaftNodeImpl removedFollower = followers[0];
        RaftNodeImpl runningFollower = followers[1];

        group.terminateNode(removedFollower.getLocalEndpoint());
        leader.replicate(apply("val")).get();
        leader.changeMembership(removedFollower.getLocalEndpoint(), REMOVE, 0).get();

        RaftEndpoint terminatedEndpoint = leader.getLocalEndpoint();
        InMemoryRaftStore stateStore = getRaftStore(leader);
        RestoredRaftState terminatedState = stateStore.toRestoredRaftState();

        // Block voting between followers
        // to avoid a leader election before leader restarts.
        blockVotingBetweenFollowers();

        group.terminateNode(terminatedEndpoint);
        RaftNodeImpl restartedNode = group.createNewRaftNode(terminatedState, stateStore);

        RaftNodeImpl newLeader = group.waitUntilLeaderElected();
        assertSame(restartedNode, newLeader);

        eventually(() -> {
            assertEquals(getCommitIndex(runningFollower), getCommitIndex(restartedNode));
            assertEquals(new ArrayList<>(getCommittedGroupMembers(runningFollower).getMembers()),
                    new ArrayList<>(getCommittedGroupMembers(restartedNode).getMembers()));
            assertEquals(new ArrayList<>(getEffectiveGroupMembers(runningFollower).getMembers()),
                    new ArrayList<>(getEffectiveGroupMembers(restartedNode).getMembers()));
        });
    }

    @Test
    public void when_followerIsRestarted_then_itAppliesPreviouslyCommittedMemberList()
            throws Exception {
        RaftConfig config = RaftConfig.newBuilder().setLeaderHeartbeatPeriodSecs(30).setLeaderHeartbeatTimeoutSecs(30).build();
        group = new LocalRaftGroup(3, config, true, LocalRaftEndpoint::newEndpoint, RAFT_STATE_STORE_FACTORY, null);
        group.start();

        RaftNodeImpl leader = group.waitUntilLeaderElected();
        RaftNodeImpl[] followers = group.getNodesExcept(leader.getLocalEndpoint());
        RaftNodeImpl removedFollower = followers[0];
        RaftNodeImpl terminatedFollower = followers[1];

        group.terminateNode(removedFollower.getLocalEndpoint());
        leader.replicate(apply("val")).get();
        leader.changeMembership(removedFollower.getLocalEndpoint(), REMOVE, 0).get();

        RaftEndpoint terminatedEndpoint = terminatedFollower.getLocalEndpoint();
        InMemoryRaftStore stateStore = getRaftStore(terminatedFollower);
        RestoredRaftState terminatedState = stateStore.toRestoredRaftState();

        group.terminateNode(terminatedEndpoint);
        RaftNodeImpl restartedNode = group.createNewRaftNode(terminatedState, stateStore);

        eventually(() -> {
            assertEquals(getCommitIndex(leader), getCommitIndex(restartedNode));
            assertEquals(getLastApplied(leader), getLastApplied(restartedNode));
            assertEquals(new ArrayList<>(getCommittedGroupMembers(leader).getMembers()),
                    new ArrayList<>(getCommittedGroupMembers(restartedNode).getMembers()));
            assertEquals(new ArrayList<>(getEffectiveGroupMembers(leader).getMembers()),
                    new ArrayList<>(getEffectiveGroupMembers(restartedNode).getMembers()));
        });
    }

    @Test
    public void when_leaderIsRestarted_then_itBecomesLeaderAndAppliesPreviouslyCommittedMemberListViaSnapshot()
            throws Exception {
        int commitCountToTakeSnapshot = 50;
        RaftConfig config = RaftConfig.newBuilder().setCommitCountToTakeSnapshot(commitCountToTakeSnapshot).build();
        group = new LocalRaftGroup(3, config, true, LocalRaftEndpoint::newEndpoint, RAFT_STATE_STORE_FACTORY, null);
        group.start();

        RaftNodeImpl leader = group.waitUntilLeaderElected();
        RaftNodeImpl[] followers = group.getNodesExcept(leader.getLocalEndpoint());
        RaftNodeImpl removedFollower = followers[0];
        RaftNodeImpl runningFollower = followers[1];

        group.terminateNode(removedFollower.getLocalEndpoint());
        leader.replicate(apply("val")).get();
        leader.changeMembership(removedFollower.getLocalEndpoint(), REMOVE, 0).get();

        while (getSnapshotEntry(leader).getIndex() == 0) {
            leader.replicate(apply("val")).get();
        }

        RaftEndpoint terminatedEndpoint = leader.getLocalEndpoint();
        InMemoryRaftStore stateStore = getRaftStore(leader);
        RestoredRaftState terminatedState = stateStore.toRestoredRaftState();

        // Block voting between followers
        // to avoid a leader election before leader restarts.
        blockVotingBetweenFollowers();

        group.terminateNode(terminatedEndpoint);
        RaftNodeImpl restartedNode = group.createNewRaftNode(terminatedState, stateStore);

        RaftNodeImpl newLeader = group.waitUntilLeaderElected();
        assertSame(restartedNode, newLeader);

        eventually(() -> {
            assertEquals(getCommitIndex(runningFollower), getCommitIndex(restartedNode));
            assertEquals(new ArrayList<>(getCommittedGroupMembers(runningFollower).getMembers()),
                    new ArrayList<>(getCommittedGroupMembers(restartedNode).getMembers()));
            assertEquals(new ArrayList<>(getEffectiveGroupMembers(runningFollower).getMembers()),
                    new ArrayList<>(getEffectiveGroupMembers(restartedNode).getMembers()));
        });
    }

    @Test
    public void when_followerIsRestarted_then_itAppliesPreviouslyCommittedMemberListViaSnapshot()
            throws Exception {
        int commitCountToTakeSnapshot = 50;
        RaftConfig config = RaftConfig.newBuilder().setCommitCountToTakeSnapshot(commitCountToTakeSnapshot)
                                      .setLeaderHeartbeatPeriodSecs(30).setLeaderHeartbeatTimeoutSecs(30).build();
        group = new LocalRaftGroup(3, config, true, LocalRaftEndpoint::newEndpoint, RAFT_STATE_STORE_FACTORY, null);
        group.start();

        RaftNodeImpl leader = group.waitUntilLeaderElected();
        RaftNodeImpl[] followers = group.getNodesExcept(leader.getLocalEndpoint());
        RaftNodeImpl removedFollower = followers[0];
        RaftNodeImpl terminatedFollower = followers[1];

        group.terminateNode(removedFollower.getLocalEndpoint());
        leader.replicate(apply("val")).get();
        leader.changeMembership(removedFollower.getLocalEndpoint(), REMOVE, 0).get();

        while (getSnapshotEntry(terminatedFollower).getIndex() == 0) {
            leader.replicate(apply("val")).get();
        }

        RaftEndpoint terminatedEndpoint = terminatedFollower.getLocalEndpoint();
        InMemoryRaftStore stateStore = getRaftStore(terminatedFollower);
        RestoredRaftState terminatedState = stateStore.toRestoredRaftState();

        group.terminateNode(terminatedEndpoint);
        RaftNodeImpl restartedNode = group.createNewRaftNode(terminatedState, stateStore);

        eventually(() -> {
            assertEquals(getCommitIndex(leader), getCommitIndex(restartedNode));
            assertEquals(getLastApplied(leader), getLastApplied(restartedNode));
            assertEquals(new ArrayList<>(getCommittedGroupMembers(leader).getMembers()),
                    new ArrayList<>(getCommittedGroupMembers(restartedNode).getMembers()));
            assertEquals(new ArrayList<>(getEffectiveGroupMembers(leader).getMembers()),
                    new ArrayList<>(getEffectiveGroupMembers(restartedNode).getMembers()));
        });
    }

}
