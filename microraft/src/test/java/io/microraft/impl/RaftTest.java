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
import io.microraft.RaftRole;
import io.microraft.exception.CannotReplicateException;
import io.microraft.exception.IndeterminateStateException;
import io.microraft.exception.NotLeaderException;
import io.microraft.impl.local.LocalRaftGroup;
import io.microraft.impl.local.SimpleStateMachine;
import io.microraft.model.message.AppendEntriesRequest;
import io.microraft.model.message.AppendEntriesSuccessResponse;
import io.microraft.model.message.VoteRequest;
import io.microraft.test.util.BaseTest;
import org.junit.After;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeoutException;

import static io.microraft.impl.local.SimpleStateMachine.apply;
import static io.microraft.test.util.AssertionUtils.allTheTime;
import static io.microraft.test.util.AssertionUtils.eventually;
import static io.microraft.test.util.RaftTestUtils.TEST_RAFT_CONFIG;
import static io.microraft.test.util.RaftTestUtils.getCommitIndex;
import static io.microraft.test.util.RaftTestUtils.getLastLogOrSnapshotEntry;
import static io.microraft.test.util.RaftTestUtils.getRole;
import static io.microraft.test.util.RaftTestUtils.getTerm;
import static io.microraft.test.util.RaftTestUtils.getVotedEndpoint;
import static io.microraft.test.util.RaftTestUtils.majority;
import static io.microraft.test.util.RaftTestUtils.minority;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * @author mdogan
 * @author metanet
 */
public class RaftTest
        extends BaseTest {

    private LocalRaftGroup group;

    @After
    public void destroy() {
        if (group != null) {
            group.destroy();
        }
    }

    @Test(timeout = 300_000)
    public void when_2NodeRaftGroupIsStarted_then_leaderIsElected() {
        testLeaderElection(2);
    }

    private void testLeaderElection(int nodeCount) {
        group = LocalRaftGroup.start(nodeCount);
        group.waitUntilLeaderElected();

        RaftEndpoint leaderEndpoint = group.getLeaderEndpoint();
        assertThat(leaderEndpoint).isNotNull();

        RaftNodeImpl leaderNode = group.getLeaderNode();
        assertThat(leaderNode).isNotNull();

        eventually(() -> {
            for (RaftNodeImpl node : group.getNodes()) {
                assertThat(node.getLeaderEndpoint()).isEqualTo(leaderEndpoint);
            }
        });
    }

    @Test(timeout = 300_000)
    public void when_3NodeRaftGroupIsStarted_then_leaderIsElected() {
        testLeaderElection(3);
    }

    @Test(timeout = 300_000)
    public void when_2NodeRaftGroupIsStarted_then_singleEntryCommitted()
            throws Exception {
        testSingleCommitEntry(2);
    }

    private void testSingleCommitEntry(int nodeCount)
            throws Exception {
        group = LocalRaftGroup.start(nodeCount);
        group.waitUntilLeaderElected();

        Object val = "val";
        Ordered<Object> result = group.getLeaderNode().replicate(apply(val)).get();
        assertThat(result.getResult()).isEqualTo(val);
        assertThat(result.getCommitIndex()).isEqualTo(1);

        int expectedCommitIndex = 1;
        eventually(() -> {
            for (RaftNodeImpl node : group.getNodes()) {
                assertThat(getCommitIndex(node)).isEqualTo(expectedCommitIndex);
                SimpleStateMachine stateMachine = group.getStateMachine(node.getLocalEndpoint());
                Object actual = stateMachine.get(expectedCommitIndex);
                assertThat(actual).isEqualTo(val);
            }
        });
    }

    @Test(timeout = 300_000)
    public void when_3NodeRaftGroupIsStarted_then_singleEntryCommitted()
            throws Exception {
        testSingleCommitEntry(3);
    }

    @Test(timeout = 300_000)
    public void when_followerAttemptsToReplicate_then_itFails()
            throws Exception {
        group = LocalRaftGroup.start(3);
        RaftNodeImpl leader = group.waitUntilLeaderElected();
        RaftNodeImpl follower = group.getAnyFollower();

        try {
            follower.replicate(apply("val")).get();
        } catch (ExecutionException e) {
            assertThat(e).hasCauseInstanceOf(NotLeaderException.class);
        }

        for (RaftNodeImpl node : group.getNodes()) {
            SimpleStateMachine stateMachine = group.getStateMachine(node.getLocalEndpoint());
            assertThat(stateMachine.size()).isEqualTo(0);
        }
    }

    @Test(timeout = 300_000)
    public void when_2NodeRaftGroupIsStarted_then_leaderCannotCommitWithOnlyLocalAppend()
            throws Exception {
        testNoCommitWhenOnlyLeaderAppends(2);
    }

    private void testNoCommitWhenOnlyLeaderAppends(int nodeCount)
            throws Exception {
        group = LocalRaftGroup.start(nodeCount);
        RaftNodeImpl leader = group.waitUntilLeaderElected();

        group.dropMessagesToAll(leader.getLocalEndpoint(), AppendEntriesRequest.class);

        try {
            leader.replicate(apply("val")).get(5, SECONDS);
            fail();
        } catch (TimeoutException ignored) {
        }

        for (RaftNodeImpl node : group.getNodes()) {
            assertThat(getCommitIndex(node)).isEqualTo(0);
            SimpleStateMachine stateMachine = group.getStateMachine(node.getLocalEndpoint());
            assertThat(stateMachine.size()).isEqualTo(0);
        }
    }

    @Test(timeout = 300_000)
    public void when_3NodeRaftGroupIsStarted_then_leaderCannotCommitWithOnlyLocalAppend()
            throws Exception {
        testNoCommitWhenOnlyLeaderAppends(3);
    }

    @Test(timeout = 300_000)
    public void when_leaderAppendsToMinority_then_itCannotCommit()
            throws Exception {
        group = LocalRaftGroup.start(5);
        RaftNodeImpl leader = group.waitUntilLeaderElected();
        List<RaftNodeImpl> followers = group.getNodesExcept(leader.getLocalEndpoint());

        for (int i = 1; i < followers.size(); i++) {
            group.dropMessagesTo(leader.getLocalEndpoint(), followers.get(i).getLocalEndpoint(), AppendEntriesRequest.class);
        }

        Future<Ordered<Object>> f = leader.replicate(apply("val"));

        eventually(() -> {
            assertThat(getLastLogOrSnapshotEntry(leader).getIndex()).isEqualTo(1);
            assertThat(getLastLogOrSnapshotEntry(followers.get(0)).getIndex()).isEqualTo(1);
        });

        try {
            f.get(5, SECONDS);
            fail();
        } catch (TimeoutException ignored) {
        }

        for (RaftNodeImpl node : group.getNodes()) {
            assertThat(getCommitIndex(node)).isEqualTo(0);
            SimpleStateMachine stateMachine = group.getStateMachine(node.getLocalEndpoint());
            assertThat(stateMachine.size()).isEqualTo(0);
        }
    }

    @Test(timeout = 300_000)
    public void when_4NodeRaftGroupIsStarted_then_leaderReplicateEntriesSequentially()
            throws Exception {
        testReplicateEntriesSequentially(4);
    }

    private void testReplicateEntriesSequentially(int nodeCount)
            throws Exception {
        int entryCount = 100;
        RaftConfig config = RaftConfig.newBuilder().setCommitCountToTakeSnapshot(entryCount + 2).build();
        group = LocalRaftGroup.start(nodeCount, config);
        RaftNodeImpl leader = group.waitUntilLeaderElected();

        for (int i = 0; i < entryCount; i++) {
            Object val = "val" + i;
            Future<Ordered<Object>> future = leader.replicate(apply(val));
            Ordered<Object> result = future.get();
            assertThat(result.getCommitIndex()).isEqualTo(i + 1);
        }

        eventually(() -> {
            for (RaftNodeImpl node : group.getNodes()) {
                assertThat(getCommitIndex(node)).isEqualTo(entryCount);
                SimpleStateMachine stateMachine = group.getStateMachine(node.getLocalEndpoint());
                assertThat(stateMachine.size()).isEqualTo(100);
                for (int i = 0; i < entryCount; i++) {
                    int commitIndex = i + 1;
                    Object val = "val" + i;
                    assertThat(stateMachine.get(commitIndex)).isEqualTo(val);
                }
            }
        });
    }

    @Test(timeout = 300_000)
    public void when_5NodeRaftGroupIsStarted_then_leaderReplicateEntriesSequentially()
            throws Exception {
        testReplicateEntriesSequentially(5);
    }

    @Test(timeout = 300_000)
    public void when_4NodeRaftGroupIsStarted_then_leaderReplicatesEntriesConcurrently()
            throws Exception {
        testReplicateEntriesConcurrently(4);
    }

    private void testReplicateEntriesConcurrently(int nodeCount)
            throws Exception {
        int entryCount = 100;
        RaftConfig config = RaftConfig.newBuilder().setCommitCountToTakeSnapshot(entryCount + 2).build();
        group = LocalRaftGroup.start(nodeCount, config);
        RaftNodeImpl leader = group.waitUntilLeaderElected();

        List<Future<Ordered<Object>>> futures = new ArrayList<>(entryCount);
        for (int i = 0; i < entryCount; i++) {
            Object val = "val" + i;
            futures.add(leader.replicate(apply(val)));
        }

        Set<Long> commitIndices = new HashSet<>();
        for (Future<Ordered<Object>> f : futures) {
            long commitIndex = f.get().getCommitIndex();
            assertTrue(commitIndices.add(commitIndex));
        }

        eventually(() -> {
            for (RaftNodeImpl node : group.getNodes()) {
                assertThat(getCommitIndex(node)).isEqualTo(entryCount);
                SimpleStateMachine stateMachine = group.getStateMachine(node.getLocalEndpoint());
                assertThat(stateMachine.size()).isEqualTo(100);
                Set<Object> values = stateMachine.valueSet();
                for (int i = 0; i < entryCount; i++) {
                    Object val = "val" + i;
                    assertThat(values.contains(val)).isTrue();
                }
            }
        });
    }

    @Test(timeout = 300_000)
    public void when_5NodeRaftGroupIsStarted_then_leaderReplicatesEntriesConcurrently()
            throws Exception {
        testReplicateEntriesConcurrently(5);
    }

    @Test(timeout = 300_000)
    public void when_4NodeRaftGroupIsStarted_then_entriesAreSubmittedInParallel()
            throws Exception {
        testReplicateEntriesInParallel(4);
    }

    private void testReplicateEntriesInParallel(int nodeCount)
            throws Exception {
        int threadCount = 10;
        int opsPerThread = 10;
        RaftConfig config = RaftConfig.newBuilder().setCommitCountToTakeSnapshot(threadCount * opsPerThread + 2).build();
        group = LocalRaftGroup.start(nodeCount, config);
        RaftNodeImpl leader = group.waitUntilLeaderElected();

        Thread[] threads = new Thread[threadCount];
        for (int i = 0; i < threadCount; i++) {
            int start = i * opsPerThread;
            threads[i] = new Thread(() -> {
                List<Future<Ordered<Object>>> futures = new ArrayList<>();
                for (int j = start; j < start + opsPerThread; j++) {
                    futures.add(leader.replicate(apply(j)));
                }

                for (Future<Ordered<Object>> f : futures) {
                    try {
                        f.get();
                    } catch (InterruptedException | ExecutionException e) {
                        e.printStackTrace();
                    }
                }
            });
        }

        for (Thread thread : threads) {
            thread.start();
        }

        for (Thread thread : threads) {
            thread.join();
        }

        int entryCount = threadCount * opsPerThread;

        eventually(() -> {
            for (RaftNodeImpl node : group.getNodes()) {
                assertThat(getCommitIndex(node)).isEqualTo(entryCount);
                SimpleStateMachine stateMachine = group.getStateMachine(node.getLocalEndpoint());
                assertThat(stateMachine.size()).isEqualTo(entryCount);
                Set<Object> values = stateMachine.valueSet();
                for (int i = 0; i < entryCount; i++) {
                    assertThat(values.contains(i)).isTrue();
                }
            }
        });
    }

    @Test(timeout = 300_000)
    public void when_5NodeRaftGroupIsStarted_then_entriesAreSubmittedInParallel()
            throws Exception {
        testReplicateEntriesInParallel(5);
    }

    @Test(timeout = 300_000)
    public void when_followerSlowsDown_then_itCatchesLeaderEventually()
            throws Exception {
        int entryCount = 100;
        RaftConfig config = RaftConfig.newBuilder().setCommitCountToTakeSnapshot(entryCount + 2).build();
        group = LocalRaftGroup.start(3, config);
        RaftNodeImpl leader = group.waitUntilLeaderElected();
        RaftNodeImpl slowFollower = group.getAnyFollower();

        group.dropMessagesTo(leader.getLocalEndpoint(), slowFollower.getLocalEndpoint(), AppendEntriesRequest.class);

        for (int i = 0; i < entryCount; i++) {
            Object val = "val" + i;
            leader.replicate(apply(val)).get();
        }

        assertThat(getCommitIndex(slowFollower)).isEqualTo(0);

        group.resetAllRulesFrom(leader.getLocalEndpoint());

        eventually(() -> {
            for (RaftNodeImpl node : group.getNodes()) {
                assertThat(getCommitIndex(node)).isEqualTo(entryCount);
                SimpleStateMachine stateMachine = group.getStateMachine(node.getLocalEndpoint());
                assertThat(stateMachine.size()).isEqualTo(entryCount);
                Set<Object> values = stateMachine.valueSet();
                for (int i = 0; i < entryCount; i++) {
                    Object val = "val" + i;
                    assertThat(values.contains(val)).isTrue();
                }
            }
        });
    }

    @Test(timeout = 300_000)
    public void when_disruptiveFollowerStartsElection_then_itCannotTakeOverLeadershipFromLegitimateLeader()
            throws Exception {
        group = LocalRaftGroup.start(3);

        RaftNodeImpl leader = group.waitUntilLeaderElected();
        int leaderTerm = getTerm(leader);
        RaftNodeImpl disruptiveFollower = group.getAnyFollower();

        RaftEndpoint disruptiveEndpoint = disruptiveFollower.getLocalEndpoint();
        group.dropMessagesTo(leader.getLocalEndpoint(), disruptiveEndpoint, AppendEntriesRequest.class);

        leader.replicate(apply("val")).get();

        group.splitMembers(disruptiveEndpoint);

        int[] disruptiveFollowerTermRef = new int[1];
        allTheTime(() -> {
            int followerTerm = getTerm(disruptiveFollower);
            assertThat(followerTerm).isEqualTo(leaderTerm);
            disruptiveFollowerTermRef[0] = followerTerm;
        }, 3);

        group.resetAllRulesFrom(leader.getLocalEndpoint());
        group.merge();

        RaftNodeImpl newLeader = group.waitUntilLeaderElected();
        RaftEndpoint newLeaderEndpoint = newLeader.getLocalEndpoint();
        assertThat(newLeaderEndpoint).isNotEqualTo(disruptiveEndpoint);
        assertThat(disruptiveFollowerTermRef[0]).isEqualTo(getTerm(newLeader));
    }

    @Test(timeout = 300_000)
    public void when_followerTerminatesInMinority_then_clusterRemainsAvailable()
            throws Exception {
        group = LocalRaftGroup.start(3);
        RaftNodeImpl leaderNode = group.waitUntilLeaderElected();

        group.terminateNode(group.getAnyFollower().getLocalEndpoint());

        String value = "value";
        Future<Ordered<Object>> future = leaderNode.replicate(apply(value));
        assertThat(future.get().getResult()).isEqualTo(value);
    }

    @Test(timeout = 300_000)
    public void when_leaderTerminatesInMinority_then_clusterRemainsAvailable()
            throws Exception {
        group = LocalRaftGroup.start(3, TEST_RAFT_CONFIG);

        RaftNodeImpl leaderNode = group.waitUntilLeaderElected();
        List<RaftNodeImpl> followers = group.getNodesExcept(leaderNode.getLocalEndpoint());
        int leaderTerm = getTerm(leaderNode);

        group.terminateNode(leaderNode.getLocalEndpoint());

        eventually(() -> {
            for (RaftNodeImpl node : followers) {
                assertThat(node.getLeaderEndpoint()).isNotNull().isNotEqualTo(leaderNode.getLocalEndpoint());
            }
        });

        RaftNodeImpl newLeader = group.waitUntilLeaderElected();
        assertThat(getTerm(newLeader)).isGreaterThan(leaderTerm);

        String value = "value";
        Future<Ordered<Object>> future = newLeader.replicate(apply(value));
        assertThat(future.get().getResult()).isEqualTo(value);
    }

    @Test(timeout = 300_000)
    public void when_leaderStaysInMajorityDuringSplit_thenItMergesBackSuccessfully() {
        group = LocalRaftGroup.start(5, TEST_RAFT_CONFIG);
        group.waitUntilLeaderElected();

        List<RaftEndpoint> minoritySplitMembers = group.getRandomNodes(minority(5), false);
        group.splitMembers(minoritySplitMembers);

        eventually(() -> {
            for (RaftEndpoint endpoint : minoritySplitMembers) {
                RaftNodeImpl node = group.getNode(endpoint);
                assertThat(node.getLeaderEndpoint()).isNull();
            }
        });

        group.merge();
        group.waitUntilLeaderElected();
    }

    @Test(timeout = 300_000)
    public void when_leaderStaysInMinorityDuringSplit_thenItMergesBackSuccessfully() {
        int nodeCount = 5;
        group = LocalRaftGroup.start(nodeCount, TEST_RAFT_CONFIG);
        RaftEndpoint leaderEndpoint = group.waitUntilLeaderElected().getLocalEndpoint();

        List<RaftEndpoint> majoritySplitMembers = group.getRandomNodes(majority(nodeCount), false);
        group.splitMembers(majoritySplitMembers);

        eventually(() -> {
            for (RaftEndpoint endpoint : majoritySplitMembers) {
                RaftNodeImpl node = group.getNode(endpoint);
                assertThat(node.getLeaderEndpoint()).isNotNull().isNotEqualTo(leaderEndpoint);
            }
        });

        eventually(() -> {
            for (RaftNodeImpl node : group.getNodes()) {
                if (!majoritySplitMembers.contains(node.getLocalEndpoint())) {
                    assertThat(node.getLeaderEndpoint()).isNull();
                }
            }
        });

        group.merge();
        group.waitUntilLeaderElected();
    }

    @Test(timeout = 300_000)
    public void when_leaderCrashes_then_theFollowerWithLongestLogBecomesLeader()
            throws Exception {
        group = LocalRaftGroup.start(4, TEST_RAFT_CONFIG);

        RaftNodeImpl leader = group.waitUntilLeaderElected();

        leader.replicate(apply("val1")).get();

        List<RaftNodeImpl> followers = group.getNodesExcept(leader.getLocalEndpoint());
        RaftNodeImpl nextLeader = followers.get(0);
        long commitIndex = getCommitIndex(leader);

        eventually(() -> {
            for (RaftNodeImpl node : group.getNodes()) {
                assertThat(getCommitIndex(node)).isEqualTo(commitIndex);
            }
        });

        for (int i = 1; i < followers.size(); i++) {
            group.dropMessagesTo(leader.getLocalEndpoint(), followers.get(i).getLocalEndpoint(), AppendEntriesRequest.class);
        }

        leader.replicate(apply("val2"));

        eventually(() -> assertThat(getLastLogOrSnapshotEntry(nextLeader).getIndex()).isGreaterThan(commitIndex));

        allTheTime(() -> {
            for (RaftNodeImpl node : group.getNodes()) {
                assertThat(getCommitIndex(node)).isEqualTo(commitIndex);
            }
        }, 3);

        group.terminateNode(leader.getLocalEndpoint());

        eventually(() -> {
            for (RaftNodeImpl node : followers) {
                assertThat(node.getLeaderEndpoint()).isEqualTo(nextLeader.getLocalEndpoint());
            }
        });

        eventually(() -> {
            for (RaftNodeImpl node : followers) {
                assertThat(getCommitIndex(node)).isEqualTo(commitIndex);
                assertThat(getLastLogOrSnapshotEntry(node).getIndex()).isGreaterThan(commitIndex);
                SimpleStateMachine stateMachine = group.getStateMachine(node.getLocalEndpoint());
                assertThat(stateMachine.size()).isEqualTo(1);
                assertThat(stateMachine.get(1)).isEqualTo("val1");
                // val2 not committed yet
            }
        });
    }

    @Test(timeout = 300_000)
    public void when_followerBecomesLeaderWithUncommittedEntries_then_thoseEntriesAreCommittedWithANewEntryOfCurrentTerm()
            throws Exception {
        group = LocalRaftGroup.start(3, TEST_RAFT_CONFIG);

        RaftNodeImpl leader = group.waitUntilLeaderElected();

        leader.replicate(apply("val1")).get();

        List<RaftNodeImpl> followers = group.getNodesExcept(leader.getLocalEndpoint());
        RaftNodeImpl nextLeader = followers.get(0);
        long commitIndex = getCommitIndex(leader);

        eventually(() -> {
            for (RaftNodeImpl node : group.getNodes()) {
                assertThat(getCommitIndex(node)).isEqualTo(commitIndex);
            }
        });

        group.dropMessagesTo(leader.getLocalEndpoint(), followers.get(1).getLocalEndpoint(), AppendEntriesRequest.class);
        group.dropMessagesTo(nextLeader.getLocalEndpoint(), leader.getLocalEndpoint(), AppendEntriesSuccessResponse.class);

        leader.replicate(apply("val2"));

        eventually(() -> assertThat(getLastLogOrSnapshotEntry(nextLeader).getIndex()).isGreaterThan(commitIndex));

        allTheTime(() -> {
            for (RaftNodeImpl node : group.getNodes()) {
                assertThat(getCommitIndex(node)).isEqualTo(commitIndex);
            }
        }, 3);

        group.terminateNode(leader.getLocalEndpoint());

        eventually(() -> {
            for (RaftNodeImpl node : followers) {
                assertThat(node.getLeaderEndpoint()).isEqualTo(nextLeader.getLocalEndpoint());
            }
        });

        nextLeader.replicate(apply("val3"));

        eventually(() -> {
            for (RaftNodeImpl node : followers) {
                assertThat(getCommitIndex(node)).isEqualTo(3);
                assertThat(getLastLogOrSnapshotEntry(node).getIndex()).isEqualTo(3);
                SimpleStateMachine stateMachine = group.getStateMachine(node.getLocalEndpoint());
                assertThat(stateMachine.size()).isEqualTo(3);
                assertThat(stateMachine.get(1)).isEqualTo("val1");
                assertThat(stateMachine.get(2)).isEqualTo("val2");
                assertThat(stateMachine.get(3)).isEqualTo("val3");
            }
        });
    }

    @Test(timeout = 300_000)
    public void when_leaderCrashes_then_theFollowerWithLongestLogMayNotBecomeLeaderIfItsLogIsNotMajority()
            throws Exception {
        group = LocalRaftGroup.start(5, TEST_RAFT_CONFIG);

        RaftNodeImpl leader = group.waitUntilLeaderElected();

        leader.replicate(apply("val1")).get();

        List<RaftNodeImpl> followers = group.getNodesExcept(leader.getLocalEndpoint());
        RaftNodeImpl followerWithLongestLog = followers.get(0);
        long commitIndex = getCommitIndex(leader);

        eventually(() -> {
            for (RaftNodeImpl node : group.getNodes()) {
                assertThat(getCommitIndex(node)).isEqualTo(commitIndex);
            }
        });

        for (int i = 1; i < followers.size(); i++) {
            group.dropMessagesTo(leader.getLocalEndpoint(), followers.get(i).getLocalEndpoint(), AppendEntriesRequest.class);
        }

        leader.replicate(apply("val2"));

        eventually(() -> assertThat(getLastLogOrSnapshotEntry(followerWithLongestLog).getIndex()).isGreaterThan(commitIndex));

        allTheTime(() -> {
            for (RaftNodeImpl node : group.getNodes()) {
                assertThat(getCommitIndex(node)).isEqualTo(commitIndex);
            }
        }, 3);

        group.dropMessagesTo(followerWithLongestLog.getLocalEndpoint(), followers.get(1).getLocalEndpoint(), VoteRequest.class);
        group.dropMessagesTo(followerWithLongestLog.getLocalEndpoint(), followers.get(2).getLocalEndpoint(), VoteRequest.class);
        group.dropMessagesTo(followerWithLongestLog.getLocalEndpoint(), followers.get(3).getLocalEndpoint(), VoteRequest.class);

        group.terminateNode(leader.getLocalEndpoint());

        RaftNodeImpl newLeader = group.waitUntilLeaderElected();

        // followerWithLongestLog has 2 entries, other 3 followers have 1 entry
        // and those 3 followers will elect a leader among themselves

        eventually(() -> {
            for (RaftNodeImpl node : followers) {
                RaftEndpoint l = node.getLeaderEndpoint();
                assertThat(l).isNotEqualTo(leader.getLocalEndpoint());
                assertThat(l).isNotEqualTo(followerWithLongestLog.getLocalEndpoint());
            }
        });

        for (int i = 1; i < followers.size(); i++) {
            assertThat(getCommitIndex(followers.get(i))).isEqualTo(commitIndex);
            assertThat(getLastLogOrSnapshotEntry(followers.get(i)).getIndex()).isEqualTo(commitIndex);
        }

        // followerWithLongestLog does not truncate its extra log entry until the new leader appends a new entry
        assertThat(getLastLogOrSnapshotEntry(followerWithLongestLog).getIndex()).isGreaterThan(commitIndex);

        newLeader.replicate(apply("val3")).get();

        eventually(() -> {
            for (RaftNodeImpl follower : followers) {
                assertThat(getCommitIndex(follower)).isEqualTo(2);
                SimpleStateMachine stateMachine = group.getStateMachine(follower.getLocalEndpoint());
                assertThat(stateMachine.size()).isEqualTo(2);
                assertThat(stateMachine.get(1)).isEqualTo("val1");
                assertThat(stateMachine.get(2)).isEqualTo("val3");
            }
        });

        assertThat(getLastLogOrSnapshotEntry(followerWithLongestLog).getIndex()).isEqualTo(2);
    }

    @Test(timeout = 300_000)
    public void when_leaderStaysInMinorityDuringSplit_then_itCannotCommitNewEntries()
            throws Exception {
        RaftConfig config = RaftConfig.newBuilder().setCommitCountToTakeSnapshot(100).build();
        group = LocalRaftGroup.start(3, config);

        RaftNodeImpl leader = group.waitUntilLeaderElected();

        leader.replicate(apply("val1")).get();

        eventually(() -> {
            for (RaftNodeImpl node : group.getNodes()) {
                assertThat(getCommitIndex(node)).isEqualTo(1);
            }
        });

        List<RaftNodeImpl> followers = group.getNodesExcept(leader.getLocalEndpoint());
        group.splitMembers(leader.getLocalEndpoint());

        eventually(() -> {
            for (RaftNodeImpl node : followers) {
                assertThat(node.getLeaderEndpoint()).isNotNull().isNotEqualTo(leader.getLocalEndpoint());
            }
        });

        List<Future<Ordered<Object>>> isolatedFutures = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            isolatedFutures.add(leader.replicate(apply("isolated" + i)));
        }

        RaftNodeImpl newLeader = group.getNode(followers.get(0).getLeaderEndpoint());
        for (int i = 0; i < 10; i++) {
            newLeader.replicate(apply("valNew" + i)).get();
        }

        eventually(() -> {
            for (RaftNodeImpl node : followers) {
                assertThat(getCommitIndex(node)).isEqualTo(11);
            }
        });

        group.merge();

        RaftNodeImpl finalLeader = group.waitUntilLeaderElected();
        RaftEndpoint finalLeaderEndpoint = finalLeader.getLocalEndpoint();
        assertThat(finalLeaderEndpoint).isNotEqualTo(leader.getLocalEndpoint());
        for (Future<Ordered<Object>> f : isolatedFutures) {
            try {
                f.get();
                fail();
            } catch (ExecutionException e) {
                assertThat(e).hasCauseInstanceOf(NotLeaderException.class);
            }
        }

        eventually(() -> {
            for (RaftNodeImpl node : followers) {
                SimpleStateMachine stateMachine = group.getStateMachine(node.getLocalEndpoint());
                assertThat(stateMachine.size()).isEqualTo(11);
                assertThat(stateMachine.get(1)).isEqualTo("val1");
                for (int i = 0; i < 10; i++) {
                    assertThat(stateMachine.get(i + 2)).isEqualTo("valNew" + i);
                }
            }
        });
    }

    @Test(timeout = 300_000)
    public void when_thereAreTooManyInflightAppendedEntries_then_newAppendsAreRejected()
            throws Exception {
        int uncommittedEntryCount = 10;
        RaftConfig config = RaftConfig.newBuilder().setMaxUncommittedLogEntryCount(uncommittedEntryCount).build();
        group = LocalRaftGroup.start(2, config);

        RaftNodeImpl leader = group.waitUntilLeaderElected();
        RaftNodeImpl follower = group.getAnyFollower();
        group.terminateNode(follower.getLocalEndpoint());

        for (int i = 0; i < uncommittedEntryCount; i++) {
            leader.replicate(apply("val" + i));
        }

        try {
            leader.replicate(apply("valFinal")).get();
            fail();
        } catch (ExecutionException e) {
            assertThat(e).hasCauseInstanceOf(CannotReplicateException.class);
        }
    }

    @Test(timeout = 300_000)
    public void when_leaderStaysInMinority_then_itDemotesItselfToFollower()
            throws Exception {
        group = LocalRaftGroup.start(2, TEST_RAFT_CONFIG);

        RaftNodeImpl leader = group.waitUntilLeaderElected();

        group.splitMembers(leader.getLocalEndpoint());
        Future<Ordered<Object>> f = leader.replicate(apply("val"));

        try {
            f.get();
            fail();
        } catch (ExecutionException e) {
            assertThat(e).hasCauseInstanceOf(IndeterminateStateException.class);
        }
    }

    @Test(timeout = 300_000)
    public void when_leaderDemotesToFollower_then_itShouldNotDeleteItsVote() {
        group = LocalRaftGroup.start(2, TEST_RAFT_CONFIG);

        RaftNodeImpl leader = group.waitUntilLeaderElected();

        assertThat(getVotedEndpoint(leader)).isEqualTo(leader.getLocalEndpoint());

        group.splitMembers(leader.getLocalEndpoint());

        eventually(() -> assertThat(getRole(leader)).isEqualTo(RaftRole.FOLLOWER));

        assertThat(getVotedEndpoint(leader)).isEqualTo(leader.getLocalEndpoint());
    }

    @Test(timeout = 300_000)
    public void when_leaderTerminates_then_itFailsPendingFuturesWithIndeterminateStateException() {
        group = LocalRaftGroup.start(3, TEST_RAFT_CONFIG);

        RaftNodeImpl leader = group.waitUntilLeaderElected();
        for (RaftNode follower : group.getNodesExcept(leader.getLeaderEndpoint())) {
            group.dropMessagesTo(leader.getLocalEndpoint(), follower.getLocalEndpoint(), AppendEntriesRequest.class);
        }

        CompletableFuture<Ordered<Object>> f = leader.replicate(apply("val"));

        eventually(() -> assertThat(getLastLogOrSnapshotEntry(leader).getIndex()).isGreaterThan(0));

        leader.terminate().join();

        assertThat(f.isCompletedExceptionally());
        try {
            f.join();
            fail();
        } catch (CompletionException e) {
            assertThat(e).hasCauseInstanceOf(IndeterminateStateException.class);
        }
    }

}
