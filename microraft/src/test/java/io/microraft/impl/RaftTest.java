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
import io.microraft.RaftRole;
import io.microraft.exception.CannotReplicateException;
import io.microraft.exception.IndeterminateStateException;
import io.microraft.exception.NotLeaderException;
import io.microraft.impl.local.LocalRaftGroup;
import io.microraft.impl.local.SimpleStateMachine;
import io.microraft.impl.util.BaseTest;
import io.microraft.model.message.AppendEntriesRequest;
import io.microraft.model.message.AppendEntriesSuccessResponse;
import io.microraft.model.message.VoteRequest;
import org.junit.After;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static io.microraft.impl.local.SimpleStateMachine.apply;
import static io.microraft.impl.util.AssertionUtils.allTheTime;
import static io.microraft.impl.util.AssertionUtils.eventually;
import static io.microraft.impl.util.RaftTestUtils.TEST_RAFT_CONFIG;
import static io.microraft.impl.util.RaftTestUtils.getCommitIndex;
import static io.microraft.impl.util.RaftTestUtils.getLastLogOrSnapshotEntry;
import static io.microraft.impl.util.RaftTestUtils.getRole;
import static io.microraft.impl.util.RaftTestUtils.getTerm;
import static io.microraft.impl.util.RaftTestUtils.getVotedEndpoint;
import static java.util.Arrays.binarySearch;
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
        group = new LocalRaftGroup(nodeCount);
        group.start();
        group.waitUntilLeaderElected();

        RaftEndpoint leaderEndpoint = group.getLeaderEndpoint();
        assertThat(leaderEndpoint).isNotNull();

        int leaderIndex = group.getLeaderIndex();
        assertThat(leaderIndex).isGreaterThanOrEqualTo(0);

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
        group = new LocalRaftGroup(nodeCount);
        group.start();
        group.waitUntilLeaderElected();

        Object val = "val";
        Ordered<Object> result = group.getLeaderNode().replicate(apply(val)).get();
        assertThat(result.getResult()).isEqualTo(val);
        assertThat(result.getCommitIndex()).isEqualTo(1);

        int commitIndex = 1;
        eventually(() -> {
            for (int i = 0; i < nodeCount; i++) {
                RaftNodeImpl node = group.getNode(i);
                long index = getCommitIndex(node);
                assertThat(index).isEqualTo(commitIndex);
                SimpleStateMachine stateMachine = group.getRuntime(i).getStateMachine();
                Object actual = stateMachine.get(commitIndex);
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
        group = new LocalRaftGroup(3);
        group.start();
        RaftNodeImpl leader = group.waitUntilLeaderElected();
        RaftNodeImpl[] followers = group.getNodesExcept(leader.getLocalEndpoint());

        try {
            followers[0].replicate(apply("val")).get();
        } catch (ExecutionException e) {
            assertThat(e).hasCauseInstanceOf(NotLeaderException.class);
        }

        for (RaftNodeImpl raftNode : group.getNodes()) {
            SimpleStateMachine stateMachine = group.getRuntime(raftNode.getLocalEndpoint()).getStateMachine();
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
        group = new LocalRaftGroup(nodeCount);
        group.start();
        RaftNodeImpl leader = group.waitUntilLeaderElected();

        group.dropMessagesToAll(leader.getLocalEndpoint(), AppendEntriesRequest.class);

        try {
            leader.replicate(apply("val")).get(5, TimeUnit.SECONDS);
            fail();
        } catch (TimeoutException ignored) {
        }

        for (RaftNodeImpl raftNode : group.getNodes()) {
            assertThat(getCommitIndex(raftNode)).isEqualTo(0);
            SimpleStateMachine stateMachine = group.getRuntime(raftNode.getLocalEndpoint()).getStateMachine();
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
        group = new LocalRaftGroup(5);
        group.start();
        RaftNodeImpl leader = group.waitUntilLeaderElected();
        RaftNodeImpl[] followers = group.getNodesExcept(leader.getLocalEndpoint());

        for (int i = 1; i < followers.length; i++) {
            group.dropMessagesToMember(leader.getLocalEndpoint(), followers[i].getLocalEndpoint(), AppendEntriesRequest.class);
        }

        Future<Ordered<Object>> f = leader.replicate(apply("val"));

        eventually(() -> {
            assertThat(getLastLogOrSnapshotEntry(leader).getIndex()).isEqualTo(1);
            assertThat(getLastLogOrSnapshotEntry(followers[0]).getIndex()).isEqualTo(1);
        });

        try {
            f.get(5, TimeUnit.SECONDS);
            fail();
        } catch (TimeoutException ignored) {
        }

        for (RaftNodeImpl raftNode : group.getNodes()) {
            assertThat(getCommitIndex(raftNode)).isEqualTo(0);
            SimpleStateMachine stateMachine = group.getRuntime(raftNode.getLocalEndpoint()).getStateMachine();
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
        group = new LocalRaftGroup(nodeCount, config);
        group.start();
        RaftNodeImpl leader = group.waitUntilLeaderElected();

        for (int i = 0; i < entryCount; i++) {
            Object val = "val" + i;
            Future<Ordered<Object>> future = leader.replicate(apply(val));
            Ordered<Object> result = future.get();
            assertThat(result.getCommitIndex()).isEqualTo(i + 1);
        }

        eventually(() -> {
            for (RaftNodeImpl raftNode : group.getNodes()) {
                assertThat(getCommitIndex(raftNode)).isEqualTo(entryCount);
                SimpleStateMachine stateMachine = group.getRuntime(raftNode.getLocalEndpoint()).getStateMachine();
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
        group = new LocalRaftGroup(nodeCount, config);
        group.start();
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
            for (RaftNodeImpl raftNode : group.getNodes()) {
                assertThat(getCommitIndex(raftNode)).isEqualTo(entryCount);
                SimpleStateMachine stateMachine = group.getRuntime(raftNode.getLocalEndpoint()).getStateMachine();
                assertThat(stateMachine.size()).isEqualTo(100);
                Set<Object> values = stateMachine.values();
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
        group = new LocalRaftGroup(nodeCount, config);
        group.start();
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
            for (RaftNodeImpl raftNode : group.getNodes()) {
                assertThat(getCommitIndex(raftNode)).isEqualTo(entryCount);
                SimpleStateMachine stateMachine = group.getRuntime(raftNode.getLocalEndpoint()).getStateMachine();
                assertThat(stateMachine.size()).isEqualTo(entryCount);
                Set<Object> values = stateMachine.values();
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
        group = new LocalRaftGroup(3, config);
        group.start();
        RaftNodeImpl leader = group.waitUntilLeaderElected();
        RaftNodeImpl slowFollower = group.getAnyFollowerNode();

        group.dropMessagesToMember(leader.getLocalEndpoint(), slowFollower.getLocalEndpoint(), AppendEntriesRequest.class);

        for (int i = 0; i < entryCount; i++) {
            Object val = "val" + i;
            leader.replicate(apply(val)).get();
        }

        assertThat(getCommitIndex(slowFollower)).isEqualTo(0);

        group.resetAllRulesFrom(leader.getLocalEndpoint());

        eventually(() -> {
            for (RaftNodeImpl raftNode : group.getNodes()) {
                assertThat(getCommitIndex(raftNode)).isEqualTo(entryCount);
                SimpleStateMachine stateMachine = group.getRuntime(raftNode.getLocalEndpoint()).getStateMachine();
                assertThat(stateMachine.size()).isEqualTo(entryCount);
                Set<Object> values = stateMachine.values();
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
        group = new LocalRaftGroup(3);
        group.start();
        RaftNodeImpl leader = group.waitUntilLeaderElected();
        int leaderTerm = getTerm(leader);
        RaftNodeImpl disruptiveFollower = group.getAnyFollowerNode();

        RaftEndpoint disruptiveEndpoint = disruptiveFollower.getLocalEndpoint();
        group.dropMessagesToMember(leader.getLocalEndpoint(), disruptiveEndpoint, AppendEntriesRequest.class);

        leader.replicate(apply("val")).get();

        group.splitMembers(disruptiveEndpoint);

        int[] disruptiveFollowerTermRef = new int[1];
        allTheTime(() -> {
            int followerTerm = getTerm(disruptiveFollower);
            assertThat(followerTerm).isEqualTo(leaderTerm);
            disruptiveFollowerTermRef[0] = followerTerm;
        }, 5);

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
        group = new LocalRaftGroup(3);
        group.start();
        RaftNodeImpl leaderNode = group.waitUntilLeaderElected();

        int leaderIndex = group.getLeaderIndex();
        for (int i = 0; i < group.size(); i++) {
            if (i != leaderIndex) {
                group.terminateNode(i);
                break;
            }
        }

        String value = "value";
        Future<Ordered<Object>> future = leaderNode.replicate(apply(value));
        assertThat(future.get().getResult()).isEqualTo(value);
    }

    @Test(timeout = 300_000)
    public void when_leaderTerminatesInMinority_then_clusterRemainsAvailable()
            throws Exception {
        group = new LocalRaftGroup(3, TEST_RAFT_CONFIG);
        group.start();

        RaftNodeImpl leaderNode = group.waitUntilLeaderElected();
        RaftNodeImpl[] followers = group.getNodesExcept(leaderNode.getLocalEndpoint());
        int leaderTerm = getTerm(leaderNode);

        group.terminateNode(leaderNode.getLocalEndpoint());

        eventually(() -> {
            for (RaftNodeImpl raftNode : followers) {
                assertThat(raftNode.getLeaderEndpoint()).isNotNull().isNotEqualTo(leaderNode.getLocalEndpoint());
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
        group = new LocalRaftGroup(5, TEST_RAFT_CONFIG);
        group.start();
        group.waitUntilLeaderElected();

        int[] split = group.createMinoritySplitIndexes(false);
        group.split(split);

        eventually(() -> {
            for (int ix : split) {
                assertThat(group.getNode(ix).getLeaderEndpoint()).isNull();
            }
        });

        group.merge();
        group.waitUntilLeaderElected();
    }

    @Test(timeout = 300_000)
    public void when_leaderStaysInMinorityDuringSplit_thenItMergesBackSuccessfully() {
        int nodeCount = 5;
        group = new LocalRaftGroup(nodeCount, TEST_RAFT_CONFIG);
        group.start();
        RaftEndpoint leaderEndpoint = group.waitUntilLeaderElected().getLocalEndpoint();

        int[] majorityNodeIndices = group.createMajoritySplitIndexes(false);
        group.split(majorityNodeIndices);

        eventually(() -> {
            for (int nodeIndex : majorityNodeIndices) {
                assertThat(group.getNode(nodeIndex).getLeaderEndpoint()).isNotNull().isNotEqualTo(leaderEndpoint);
            }
        });

        eventually(() -> {
            for (int i = 0; i < nodeCount; i++) {
                if (binarySearch(majorityNodeIndices, i) < 0) {
                    RaftNodeImpl minorityNode = group.getNode(i);
                    assertThat(minorityNode.getLeaderEndpoint()).isNull();
                }
            }
        });

        group.merge();
        group.waitUntilLeaderElected();
    }

    @Test(timeout = 300_000)
    public void when_leaderCrashes_then_theFollowerWithLongestLogBecomesLeader()
            throws Exception {
        group = new LocalRaftGroup(4, TEST_RAFT_CONFIG);
        group.start();

        RaftNodeImpl leader = group.waitUntilLeaderElected();

        leader.replicate(apply("val1")).get();

        RaftNodeImpl[] followers = group.getNodesExcept(leader.getLocalEndpoint());
        RaftNodeImpl nextLeader = followers[0];
        long commitIndex = getCommitIndex(leader);

        eventually(() -> {
            for (RaftNodeImpl raftNode : group.getNodes()) {
                assertThat(getCommitIndex(raftNode)).isEqualTo(commitIndex);
            }
        });

        for (int i = 1; i < followers.length; i++) {
            group.dropMessagesToMember(leader.getLocalEndpoint(), followers[i].getLocalEndpoint(), AppendEntriesRequest.class);
        }

        leader.replicate(apply("val2"));

        eventually(() -> assertThat(getLastLogOrSnapshotEntry(nextLeader).getIndex()).isGreaterThan(commitIndex));

        allTheTime(() -> {
            for (RaftNodeImpl raftNode : group.getNodes()) {
                assertThat(getCommitIndex(raftNode)).isEqualTo(commitIndex);
            }
        }, 10);

        group.terminateNode(leader.getLocalEndpoint());

        eventually(() -> {
            for (RaftNodeImpl raftNode : followers) {
                assertThat(raftNode.getLeaderEndpoint()).isEqualTo(nextLeader.getLocalEndpoint());
            }
        });

        eventually(() -> {
            for (RaftNodeImpl raftNode : followers) {
                assertThat(getCommitIndex(raftNode)).isEqualTo(commitIndex);
                assertThat(getLastLogOrSnapshotEntry(raftNode).getIndex()).isGreaterThan(commitIndex);
                SimpleStateMachine stateMachine = group.getRuntime(raftNode.getLocalEndpoint()).getStateMachine();
                assertThat(stateMachine.size()).isEqualTo(1);
                assertThat(stateMachine.get(1)).isEqualTo("val1");
                // val2 not committed yet
            }
        });
    }

    @Test(timeout = 300_000)
    public void when_followerBecomesLeaderWithUncommittedEntries_then_thoseEntriesAreCommittedWithANewEntryOfCurrentTerm()
            throws Exception {
        group = new LocalRaftGroup(3, TEST_RAFT_CONFIG);
        group.start();

        RaftNodeImpl leader = group.waitUntilLeaderElected();

        leader.replicate(apply("val1")).get();

        RaftNodeImpl[] followers = group.getNodesExcept(leader.getLocalEndpoint());
        RaftNodeImpl nextLeader = followers[0];
        long commitIndex = getCommitIndex(leader);

        eventually(() -> {
            for (RaftNodeImpl raftNode : group.getNodes()) {
                assertThat(getCommitIndex(raftNode)).isEqualTo(commitIndex);
            }
        });

        group.dropMessagesToMember(leader.getLocalEndpoint(), followers[1].getLocalEndpoint(), AppendEntriesRequest.class);
        group.dropMessagesToMember(nextLeader.getLocalEndpoint(), leader.getLocalEndpoint(), AppendEntriesSuccessResponse.class);

        leader.replicate(apply("val2"));

        eventually(() -> assertThat(getLastLogOrSnapshotEntry(nextLeader).getIndex()).isGreaterThan(commitIndex));

        allTheTime(() -> {
            for (RaftNodeImpl raftNode : group.getNodes()) {
                assertThat(getCommitIndex(raftNode)).isEqualTo(commitIndex);
            }
        }, 10);

        group.terminateNode(leader.getLocalEndpoint());

        eventually(() -> {
            for (RaftNodeImpl raftNode : followers) {
                assertThat(raftNode.getLeaderEndpoint()).isEqualTo(nextLeader.getLocalEndpoint());
            }
        });

        nextLeader.replicate(apply("val3"));

        eventually(() -> {
            for (RaftNodeImpl raftNode : followers) {
                assertThat(getCommitIndex(raftNode)).isEqualTo(3);
                assertThat(getLastLogOrSnapshotEntry(raftNode).getIndex()).isEqualTo(3);
                SimpleStateMachine stateMachine = group.getRuntime(raftNode.getLocalEndpoint()).getStateMachine();
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
        group = new LocalRaftGroup(5, TEST_RAFT_CONFIG);
        group.start();

        RaftNodeImpl leader = group.waitUntilLeaderElected();

        leader.replicate(apply("val1")).get();

        RaftNodeImpl[] followers = group.getNodesExcept(leader.getLocalEndpoint());
        RaftNodeImpl followerWithLongestLog = followers[0];
        long commitIndex = getCommitIndex(leader);

        eventually(() -> {
            for (RaftNodeImpl raftNode : group.getNodes()) {
                assertThat(getCommitIndex(raftNode)).isEqualTo(commitIndex);
            }
        });

        for (int i = 1; i < followers.length; i++) {
            group.dropMessagesToMember(leader.getLocalEndpoint(), followers[i].getLocalEndpoint(), AppendEntriesRequest.class);
        }

        leader.replicate(apply("val2"));

        eventually(() -> assertThat(getLastLogOrSnapshotEntry(followerWithLongestLog).getIndex()).isGreaterThan(commitIndex));

        allTheTime(() -> {
            for (RaftNodeImpl raftNode : group.getNodes()) {
                assertThat(getCommitIndex(raftNode)).isEqualTo(commitIndex);
            }
        }, 10);

        group.dropMessagesToMember(followerWithLongestLog.getLocalEndpoint(), followers[1].getLocalEndpoint(), VoteRequest.class);
        group.dropMessagesToMember(followerWithLongestLog.getLocalEndpoint(), followers[2].getLocalEndpoint(), VoteRequest.class);

        group.terminateNode(leader.getLocalEndpoint());

        RaftNodeImpl newLeader = group.waitUntilLeaderElected();

        // followerWithLongestLog has 2 entries, other 3 followers have 1 entry
        // and those 3 followers will elect a leader among themselves

        eventually(() -> {
            for (RaftNodeImpl raftNode : followers) {
                RaftEndpoint l = raftNode.getLeaderEndpoint();
                assertThat(l).isNotEqualTo(leader.getLocalEndpoint());
                assertThat(l).isNotEqualTo(followerWithLongestLog.getLocalEndpoint());
            }
        });

        for (int i = 1; i < followers.length; i++) {
            assertThat(getCommitIndex(followers[i])).isEqualTo(commitIndex);
            assertThat(getLastLogOrSnapshotEntry(followers[i]).getIndex()).isEqualTo(commitIndex);
        }

        // followerWithLongestLog does not truncate its extra log entry until the new leader appends a new entry
        assertThat(getLastLogOrSnapshotEntry(followerWithLongestLog).getIndex()).isGreaterThan(commitIndex);

        newLeader.replicate(apply("val3")).get();

        eventually(() -> {
            for (RaftNodeImpl follower : followers) {
                assertThat(getCommitIndex(follower)).isEqualTo(2);
                SimpleStateMachine stateMachine = group.getRuntime(follower.getLocalEndpoint()).getStateMachine();
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
        group = new LocalRaftGroup(3, config);
        group.start();

        RaftNodeImpl leader = group.waitUntilLeaderElected();

        leader.replicate(apply("val1")).get();

        eventually(() -> {
            for (RaftNodeImpl raftNode : group.getNodes()) {
                assertThat(getCommitIndex(raftNode)).isEqualTo(1);
            }
        });

        RaftNodeImpl[] followers = group.getNodesExcept(leader.getLocalEndpoint());
        group.splitMembers(leader.getLocalEndpoint());

        eventually(() -> {
            for (RaftNodeImpl raftNode : followers) {
                assertThat(raftNode.getLeaderEndpoint()).isNotNull().isNotEqualTo(leader.getLocalEndpoint());
            }
        });

        List<Future<Ordered<Object>>> isolatedFutures = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            isolatedFutures.add(leader.replicate(apply("isolated" + i)));
        }

        RaftNodeImpl newLeader = group.getNode(followers[0].getLeaderEndpoint());
        for (int i = 0; i < 10; i++) {
            newLeader.replicate(apply("valNew" + i)).get();
        }

        eventually(() -> {
            for (RaftNodeImpl raftNode : followers) {
                assertThat(getCommitIndex(raftNode)).isEqualTo(11);
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
            for (RaftNodeImpl raftNode : followers) {
                SimpleStateMachine stateMachine = group.getRuntime(raftNode.getLocalEndpoint()).getStateMachine();
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
        group = new LocalRaftGroup(2, config);
        group.start();

        RaftNodeImpl leader = group.waitUntilLeaderElected();
        RaftNodeImpl follower = group.getAnyFollowerNode();
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
        group = new LocalRaftGroup(2, TEST_RAFT_CONFIG);
        group.start();

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
        group = new LocalRaftGroup(2, TEST_RAFT_CONFIG);
        group.start();

        RaftNodeImpl leader = group.waitUntilLeaderElected();

        assertThat(getVotedEndpoint(leader)).isEqualTo(leader.getLocalEndpoint());

        group.split(group.getIndexOf(leader.getLocalEndpoint()));

        eventually(() -> assertThat(getRole(leader)).isEqualTo(RaftRole.FOLLOWER));

        assertThat(getVotedEndpoint(leader)).isEqualTo(leader.getLocalEndpoint());
    }

}
