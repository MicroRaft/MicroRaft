package com.hazelcast.raft.impl;

import com.hazelcast.raft.RaftConfig;
import com.hazelcast.raft.RaftEndpoint;
import com.hazelcast.raft.exception.CannotReplicateException;
import com.hazelcast.raft.exception.LeaderDemotedException;
import com.hazelcast.raft.exception.NotLeaderException;
import com.hazelcast.raft.impl.local.LocalRaftGroup;
import com.hazelcast.raft.impl.local.RaftDummyService;
import com.hazelcast.raft.impl.msg.AppendEntriesRequest;
import com.hazelcast.raft.impl.msg.AppendEntriesSuccessResponse;
import com.hazelcast.raft.impl.msg.VoteRequest;
import org.junit.After;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static com.hazelcast.raft.impl.local.RaftDummyService.apply;
import static com.hazelcast.raft.impl.util.AssertUtil.allTheTime;
import static com.hazelcast.raft.impl.util.AssertUtil.eventually;
import static com.hazelcast.raft.impl.util.RaftUtil.getCommitIndex;
import static com.hazelcast.raft.impl.util.RaftUtil.getLastLogOrSnapshotEntry;
import static com.hazelcast.raft.impl.util.RaftUtil.getLeaderMember;
import static com.hazelcast.raft.impl.util.RaftUtil.getTerm;
import static com.hazelcast.raft.impl.util.RaftUtil.newGroup;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.fail;

/**
 * @author mdogan
 * @author metanet
 */
public class RaftTest {

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

    @Test(timeout = 300_000)
    public void when_3NodeRaftGroupIsStarted_then_leaderIsElected() {
        testLeaderElection(3);
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
                RaftEndpoint leader = getLeaderMember(node);
                assertThat(leader).isEqualTo(leaderEndpoint);
            }
        });
    }

    @Test(timeout = 300_000)
    public void when_2NodeRaftGroupIsStarted_then_singleEntryCommitted()
            throws Exception {
        testSingleCommitEntry(2);
    }

    @Test(timeout = 300_000)
    public void when_3NodeRaftGroupIsStarted_then_singleEntryCommitted()
            throws Exception {
        testSingleCommitEntry(3);
    }

    private void testSingleCommitEntry(int nodeCount)
            throws Exception {
        group = newGroup(nodeCount);
        group.start();
        group.waitUntilLeaderElected();

        Object val = "val";
        Object result = group.getLeaderNode().replicate(apply(val)).get();
        assertThat(val).isEqualTo(result);

        int commitIndex = 1;
        eventually(() -> {
            for (int i = 0; i < nodeCount; i++) {
                RaftNodeImpl node = group.getNode(i);
                long index = getCommitIndex(node);
                assertThat(index).isEqualTo(commitIndex);
                RaftDummyService service = group.getIntegration(i).getService();
                Object actual = service.get(commitIndex);
                assertThat(actual).isEqualTo(val);
            }
        });
    }

    @Test(timeout = 300_000)
    public void when_followerAttemptsToReplicate_then_itFails()
            throws InterruptedException {
        group = newGroup(3);
        group.start();
        RaftNodeImpl leader = group.waitUntilLeaderElected();
        RaftNodeImpl[] followers = group.getNodesExcept(leader.getLocalEndpoint());

        try {
            followers[0].replicate(apply("val")).get();
        } catch (ExecutionException e) {
            assertThat(e).hasCauseInstanceOf(NotLeaderException.class);
        }

        for (RaftNodeImpl raftNode : group.getNodes()) {
            RaftDummyService service = group.getIntegration(raftNode.getLocalEndpoint()).getService();
            assertThat(service.size()).isEqualTo(0);
        }
    }

    @Test(timeout = 300_000)
    public void when_2NodeRaftGroupIsStarted_then_leaderCannotCommitWithOnlyLocalAppend()
            throws ExecutionException, InterruptedException {
        testNoCommitWhenOnlyLeaderAppends(2);
    }

    @Test(timeout = 300_000)
    public void when_3NodeRaftGroupIsStarted_then_leaderCannotCommitWithOnlyLocalAppend()
            throws ExecutionException, InterruptedException {
        testNoCommitWhenOnlyLeaderAppends(3);
    }

    private void testNoCommitWhenOnlyLeaderAppends(int nodeCount)
            throws InterruptedException, ExecutionException {
        group = newGroup(nodeCount);
        group.start();
        RaftNodeImpl leader = group.waitUntilLeaderElected();

        group.dropMessagesToAll(leader.getLocalEndpoint(), AppendEntriesRequest.class);

        try {
            leader.replicate(apply("val")).get(10, TimeUnit.SECONDS);
            fail();
        } catch (TimeoutException ignored) {
        }

        for (RaftNodeImpl raftNode : group.getNodes()) {
            assertThat(getCommitIndex(raftNode)).isEqualTo(0);
            RaftDummyService service = group.getIntegration(raftNode.getLocalEndpoint()).getService();
            assertThat(service.size()).isEqualTo(0);
        }
    }

    @Test(timeout = 300_000)
    public void when_leaderAppendsToMinority_then_itCannotCommit()
            throws ExecutionException, InterruptedException {
        group = newGroup(5);
        group.start();
        RaftNodeImpl leader = group.waitUntilLeaderElected();
        RaftNodeImpl[] followers = group.getNodesExcept(leader.getLocalEndpoint());

        for (int i = 1; i < followers.length; i++) {
            group.dropMessagesToMember(leader.getLocalEndpoint(), followers[i].getLocalEndpoint(), AppendEntriesRequest.class);
        }

        Future f = leader.replicate(apply("val"));

        eventually(() -> {
            assertThat(getLastLogOrSnapshotEntry(leader).index()).isEqualTo(1);
            assertThat(getLastLogOrSnapshotEntry(followers[0]).index()).isEqualTo(1);
        });

        try {
            f.get(10, TimeUnit.SECONDS);
            fail();
        } catch (TimeoutException ignored) {
        }

        for (RaftNodeImpl raftNode : group.getNodes()) {
            assertThat(getCommitIndex(raftNode)).isEqualTo(0);
            RaftDummyService service = group.getIntegration(raftNode.getLocalEndpoint()).getService();
            assertThat(service.size()).isEqualTo(0);
        }
    }

    @Test(timeout = 300_000)
    public void when_4NodeRaftGroupIsStarted_then_leaderReplicateEntriesSequentially()
            throws ExecutionException, InterruptedException {
        testReplicateEntriesSequentially(4);
    }

    @Test(timeout = 300_000)
    public void when_5NodeRaftGroupIsStarted_then_leaderReplicateEntriesSequentially()
            throws ExecutionException, InterruptedException {
        testReplicateEntriesSequentially(5);
    }

    private void testReplicateEntriesSequentially(int nodeCount)
            throws ExecutionException, InterruptedException {
        int entryCount = 100;
        group = newGroup(nodeCount, RaftConfig.builder().setCommitIndexAdvanceCountToTakeSnapshot(entryCount + 2).build());
        group.start();
        RaftNodeImpl leader = group.waitUntilLeaderElected();

        for (int i = 0; i < entryCount; i++) {
            Object val = "val" + i;
            leader.replicate(apply(val)).get();
        }

        eventually(() -> {
            for (RaftNodeImpl raftNode : group.getNodes()) {
                assertThat(getCommitIndex(raftNode)).isEqualTo(entryCount);
                RaftDummyService service = group.getIntegration(raftNode.getLocalEndpoint()).getService();
                assertThat(service.size()).isEqualTo(100);
                for (int i = 0; i < entryCount; i++) {
                    int commitIndex = i + 1;
                    Object val = "val" + i;
                    assertThat(service.get(commitIndex)).isEqualTo(val);
                }
            }
        });
    }

    @Test(timeout = 300_000)
    public void when_4NodeRaftGroupIsStarted_then_leaderReplicatesEntriesConcurrently()
            throws ExecutionException, InterruptedException {
        testReplicateEntriesConcurrently(4);
    }

    @Test(timeout = 300_000)
    public void when_5NodeRaftGroupIsStarted_then_leaderReplicatesEntriesConcurrently()
            throws ExecutionException, InterruptedException {
        testReplicateEntriesConcurrently(5);
    }

    private void testReplicateEntriesConcurrently(int nodeCount)
            throws ExecutionException, InterruptedException {
        int entryCount = 100;
        group = newGroup(nodeCount, RaftConfig.builder().setCommitIndexAdvanceCountToTakeSnapshot(entryCount + 2).build());
        group.start();
        RaftNodeImpl leader = group.waitUntilLeaderElected();

        List<Future> futures = new ArrayList<>(entryCount);
        for (int i = 0; i < entryCount; i++) {
            Object val = "val" + i;
            futures.add(leader.replicate(apply(val)));
        }

        for (Future f : futures) {
            f.get();
        }

        eventually(() -> {
            for (RaftNodeImpl raftNode : group.getNodes()) {
                assertThat(getCommitIndex(raftNode)).isEqualTo(entryCount);
                RaftDummyService service = group.getIntegration(raftNode.getLocalEndpoint()).getService();
                assertThat(service.size()).isEqualTo(100);
                Set<Object> values = service.values();
                for (int i = 0; i < entryCount; i++) {
                    Object val = "val" + i;
                    assertThat(values.contains(val)).isTrue();
                }
            }
        });
    }

    @Test(timeout = 300_000)
    public void when_4NodeRaftGroupIsStarted_then_entriesAreSubmittedInParallel()
            throws InterruptedException {
        testReplicateEntriesInParallel(4);
    }

    @Test(timeout = 300_000)
    public void when_5NodeRaftGroupIsStarted_then_entriesAreSubmittedInParallel()
            throws InterruptedException {
        testReplicateEntriesInParallel(5);
    }

    private void testReplicateEntriesInParallel(int nodeCount)
            throws InterruptedException {
        int threadCount = 10;
        int opsPerThread = 10;
        group = newGroup(nodeCount,
                RaftConfig.builder().setCommitIndexAdvanceCountToTakeSnapshot(threadCount * opsPerThread + 2).build());
        group.start();
        RaftNodeImpl leader = group.waitUntilLeaderElected();

        Thread[] threads = new Thread[threadCount];
        for (int i = 0; i < threadCount; i++) {
            int start = i * opsPerThread;
            threads[i] = new Thread(() -> {
                List<Future> futures = new ArrayList<>();
                for (int j = start; j < start + opsPerThread; j++) {
                    futures.add(leader.replicate(apply(j)));
                }

                for (Future f : futures) {
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
                RaftDummyService service = group.getIntegration(raftNode.getLocalEndpoint()).getService();
                assertThat(service.size()).isEqualTo(entryCount);
                Set<Object> values = service.values();
                for (int i = 0; i < entryCount; i++) {
                    assertThat(values.contains(i)).isTrue();
                }
            }
        });
    }

    @Test(timeout = 300_000)
    public void when_followerSlowsDown_then_itCatchesLeaderEventually()
            throws ExecutionException, InterruptedException {
        int entryCount = 100;
        group = newGroup(3, RaftConfig.builder().setCommitIndexAdvanceCountToTakeSnapshot(entryCount + 2).build());
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
                RaftDummyService service = group.getIntegration(raftNode.getLocalEndpoint()).getService();
                assertThat(service.size()).isEqualTo(entryCount);
                Set<Object> values = service.values();
                for (int i = 0; i < entryCount; i++) {
                    Object val = "val" + i;
                    assertThat(values.contains(val)).isTrue();
                }
            }
        });
    }

    @Test(timeout = 300_000)
    public void when_disruptiveFollowerStartsElection_then_itCannotTakeOverLeadershipFromLegitimateLeader()
            throws ExecutionException, InterruptedException {
        group = newGroup(3);
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
        group = newGroup(3);
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
        Future future = leaderNode.replicate(apply(value));
        assertThat(future.get()).isEqualTo(value);
    }

    @Test(timeout = 300_000)
    public void when_leaderTerminatesInMinority_then_clusterRemainsAvailable()
            throws Exception {
        group = newGroup(3);
        group.start();

        RaftNodeImpl leaderNode = group.waitUntilLeaderElected();
        RaftNodeImpl[] followers = group.getNodesExcept(leaderNode.getLocalEndpoint());
        int leaderTerm = getTerm(leaderNode);

        group.terminateNode(leaderNode.getLocalEndpoint());

        eventually(() -> {
            for (RaftNodeImpl raftNode : followers) {
                RaftEndpoint newLeader = getLeaderMember(raftNode);
                assertThat(newLeader).isNotNull().isNotEqualTo(leaderNode.getLocalEndpoint());
            }
        });

        RaftNodeImpl newLeader = group.waitUntilLeaderElected();
        assertThat(getTerm(newLeader)).isGreaterThan(leaderTerm);

        String value = "value";
        Future future = newLeader.replicate(apply(value));
        assertThat(future.get()).isEqualTo(value);
    }

    @Test(timeout = 300_000)
    public void when_leaderStaysInMajorityDuringSplit_thenItMergesBackSuccessfully() {
        group = new LocalRaftGroup(5);
        group.start();
        group.waitUntilLeaderElected();

        int[] split = group.createMinoritySplitIndexes(false);
        group.split(split);

        eventually(() -> {
            for (int ix : split) {
                RaftEndpoint leader = getLeaderMember(group.getNode(ix));
                assertThat(leader).isNull();
            }
        });

        group.merge();
        group.waitUntilLeaderElected();
    }

    @Test(timeout = 300_000)
    public void when_leaderStaysInMinorityDuringSplit_thenItMergesBackSuccessfully() {
        int nodeCount = 5;
        group = new LocalRaftGroup(nodeCount);
        group.start();
        RaftEndpoint leaderEndpoint = group.waitUntilLeaderElected().getLocalEndpoint();

        int[] split = group.createMajoritySplitIndexes(false);
        group.split(split);

        eventually(() -> {
            for (int ix : split) {
                RaftEndpoint newLeader = getLeaderMember(group.getNode(ix));
                assertThat(newLeader).isNotNull().isNotEqualTo(leaderEndpoint);
            }
        });

        for (int i = 0; i < nodeCount; i++) {
            if (Arrays.binarySearch(split, i) < 0) {
                RaftEndpoint leader = getLeaderMember(group.getNode(i));
                assertThat(leader).isEqualTo(leaderEndpoint);
            }
        }

        group.merge();
        group.waitUntilLeaderElected();
    }

    @Test(timeout = 300_000)
    public void when_leaderCrashes_then_theFollowerWithLongestLogBecomesLeader()
            throws Exception {
        group = newGroup(4);
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

        eventually(() -> assertThat(getLastLogOrSnapshotEntry(nextLeader).index()).isGreaterThan(commitIndex));

        allTheTime(() -> {
            for (RaftNodeImpl raftNode : group.getNodes()) {
                assertThat(getCommitIndex(raftNode)).isEqualTo(commitIndex);
            }
        }, 10);

        group.terminateNode(leader.getLocalEndpoint());

        eventually(() -> {
            for (RaftNodeImpl raftNode : followers) {
                RaftEndpoint l = getLeaderMember(raftNode);
                assertThat(l).isEqualTo(nextLeader.getLocalEndpoint());
            }
        });

        eventually(() -> {
            for (RaftNodeImpl raftNode : followers) {
                assertThat(getCommitIndex(raftNode)).isEqualTo(commitIndex);
                assertThat(getLastLogOrSnapshotEntry(raftNode).index()).isGreaterThan(commitIndex);
                RaftDummyService service = group.getIntegration(raftNode.getLocalEndpoint()).getService();
                assertThat(service.size()).isEqualTo(1);
                assertThat(service.get(1)).isEqualTo("val1");
                // val2 not committed yet
            }
        });
    }

    @Test(timeout = 300_000)
    public void when_followerBecomesLeaderWithUncommittedEntries_then_thoseEntriesAreCommittedWithANewEntryOfCurrentTerm()
            throws Exception {
        group = newGroup(3);
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

        eventually(() -> assertThat(getLastLogOrSnapshotEntry(nextLeader).index()).isGreaterThan(commitIndex));

        allTheTime(() -> {
            for (RaftNodeImpl raftNode : group.getNodes()) {
                assertThat(getCommitIndex(raftNode)).isEqualTo(commitIndex);
            }
        }, 10);

        group.terminateNode(leader.getLocalEndpoint());

        eventually(() -> {
            for (RaftNodeImpl raftNode : followers) {
                RaftEndpoint l = getLeaderMember(raftNode);
                assertThat(l).isEqualTo(nextLeader.getLocalEndpoint());
            }
        });

        nextLeader.replicate(apply("val3"));

        eventually(() -> {
            for (RaftNodeImpl raftNode : followers) {
                assertThat(getCommitIndex(raftNode)).isEqualTo(3);
                assertThat(getLastLogOrSnapshotEntry(raftNode).index()).isEqualTo(3);
                RaftDummyService service = group.getIntegration(raftNode.getLocalEndpoint()).getService();
                assertThat(service.size()).isEqualTo(3);
                assertThat(service.get(1)).isEqualTo("val1");
                assertThat(service.get(2)).isEqualTo("val2");
                assertThat(service.get(3)).isEqualTo("val3");
            }
        });
    }

    @Test(timeout = 300_000)
    public void when_leaderCrashes_then_theFollowerWithLongestLogMayNotBecomeLeaderIfItsLogIsNotMajority()
            throws Exception {
        group = newGroup(5);
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

        eventually(() -> assertThat(getLastLogOrSnapshotEntry(followerWithLongestLog).index()).isGreaterThan(commitIndex));

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
                RaftEndpoint l = getLeaderMember(raftNode);
                assertThat(l).isNotEqualTo(leader.getLocalEndpoint());
                assertThat(l).isNotEqualTo(followerWithLongestLog.getLocalEndpoint());
            }
        });

        for (int i = 1; i < followers.length; i++) {
            assertThat(getCommitIndex(followers[i])).isEqualTo(commitIndex);
            assertThat(getLastLogOrSnapshotEntry(followers[i]).index()).isEqualTo(commitIndex);
        }

        // followerWithLongestLog does not truncate its extra log entry until the new leader appends a new entry
        assertThat(getLastLogOrSnapshotEntry(followerWithLongestLog).index()).isGreaterThan(commitIndex);

        newLeader.replicate(apply("val3")).get();

        eventually(() -> {
            for (RaftNodeImpl follower : followers) {
                assertThat(getCommitIndex(follower)).isEqualTo(2);
                RaftDummyService service = group.getIntegration(follower.getLocalEndpoint()).getService();
                assertThat(service.size()).isEqualTo(2);
                assertThat(service.get(1)).isEqualTo("val1");
                assertThat(service.get(2)).isEqualTo("val3");
            }
        });

        assertThat(getLastLogOrSnapshotEntry(followerWithLongestLog).index()).isEqualTo(2);
    }

    @Test(timeout = 300_000)
    public void when_leaderStaysInMinorityDuringSplit_then_itCannotCommitNewEntries()
            throws Exception {
        group = newGroup(3, RaftConfig.builder().setCommitIndexAdvanceCountToTakeSnapshot(100).build());
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
                RaftEndpoint leaderEndpoint = getLeaderMember(raftNode);
                assertThat(leaderEndpoint).isNotNull().isNotEqualTo(leader.getLocalEndpoint());
            }
        });

        List<Future> isolatedFutures = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            isolatedFutures.add(leader.replicate(apply("isolated" + i)));
        }

        RaftNodeImpl newLeader = group.getNode(getLeaderMember(followers[0]));
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
        for (Future f : isolatedFutures) {
            try {
                f.get();
                fail();
            } catch (ExecutionException e) {
                assertThat(e).hasCauseInstanceOf(LeaderDemotedException.class);
            }
        }

        eventually(() -> {
            for (RaftNodeImpl raftNode : followers) {
                RaftDummyService service = group.getIntegration(raftNode.getLocalEndpoint()).getService();
                assertThat(service.size()).isEqualTo(11);
                assertThat(service.get(1)).isEqualTo("val1");
                for (int i = 0; i < 10; i++) {
                    assertThat(service.get(i + 2)).isEqualTo("valNew" + i);
                }
            }
        });
    }

    @Test(timeout = 300_000)
    public void when_thereAreTooManyInflightAppendedEntries_then_newAppendsAreRejected()
            throws Exception {
        int uncommittedEntryCount = 10;
        group = newGroup(2, RaftConfig.builder().setUncommittedLogEntryCountToRejectNewAppends(uncommittedEntryCount).build());
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

}
