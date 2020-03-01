package com.hazelcast.raft.impl;

import com.hazelcast.raft.MembershipChangeMode;
import com.hazelcast.raft.RaftConfig;
import com.hazelcast.raft.RaftEndpoint;
import com.hazelcast.raft.RaftNode;
import com.hazelcast.raft.exception.OperationResultUnknownException;
import com.hazelcast.raft.impl.groupop.UpdateRaftGroupMembersOp;
import com.hazelcast.raft.impl.local.LocalRaftGroup;
import com.hazelcast.raft.impl.local.RaftDummyService;
import com.hazelcast.raft.impl.log.LogEntry;
import com.hazelcast.raft.impl.msg.AppendEntriesFailureResponse;
import com.hazelcast.raft.impl.msg.AppendEntriesRequest;
import com.hazelcast.raft.impl.msg.AppendEntriesSuccessResponse;
import com.hazelcast.raft.impl.msg.InstallSnapshotRequest;
import org.junit.After;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.function.Function;

import static com.hazelcast.raft.RaftNodeStatus.ACTIVE;
import static com.hazelcast.raft.impl.local.RaftDummyService.apply;
import static com.hazelcast.raft.impl.util.AssertUtil.eventually;
import static com.hazelcast.raft.impl.util.RaftUtil.getCommitIndex;
import static com.hazelcast.raft.impl.util.RaftUtil.getCommittedGroupMembers;
import static com.hazelcast.raft.impl.util.RaftUtil.getLastLogOrSnapshotEntry;
import static com.hazelcast.raft.impl.util.RaftUtil.getLeaderMember;
import static com.hazelcast.raft.impl.util.RaftUtil.getMatchIndex;
import static com.hazelcast.raft.impl.util.RaftUtil.getSnapshotEntry;
import static com.hazelcast.raft.impl.util.RaftUtil.getStatus;
import static com.hazelcast.raft.impl.util.RaftUtil.newGroup;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.fail;

/**
 * @author mdogan
 * @author metanet
 */
public class SnapshotTest {

    private LocalRaftGroup group;

    @After
    public void destroy() {
        if (group != null) {
            group.destroy();
        }
    }

    @Test(timeout = 300_000)
    public void when_commitLogAdvances_then_snapshotIsTaken()
            throws ExecutionException, InterruptedException {
        int entryCount = 50;
        group = newGroup(3, RaftConfig.builder().setCommitIndexAdvanceCountToTakeSnapshot(entryCount).build());
        group.start();

        RaftNodeImpl leader = group.waitUntilLeaderElected();

        for (int i = 0; i < entryCount; i++) {
            leader.replicate(apply("val" + i)).get();
        }

        eventually(() -> {
            for (RaftNodeImpl raftNode : group.getNodes()) {
                assertThat(getCommitIndex(raftNode)).isEqualTo(entryCount);
                assertThat(getSnapshotEntry(raftNode).index()).isEqualTo(entryCount);

                RaftDummyService service = group.getIntegration(raftNode.getLocalEndpoint()).getService();
                assertThat(service.size()).isEqualTo(entryCount);
                for (int i = 0; i < entryCount; i++) {
                    assertThat(service.get(i + 1)).isEqualTo("val" + i);
                }
            }
        });
    }

    @Test(timeout = 300_000)
    public void when_snapshotIsTaken_then_nextEntryIsCommitted()
            throws ExecutionException, InterruptedException {
        int entryCount = 50;
        group = newGroup(3, RaftConfig.builder().setCommitIndexAdvanceCountToTakeSnapshot(entryCount).build());
        group.start();

        RaftNodeImpl leader = group.waitUntilLeaderElected();

        for (int i = 0; i < entryCount; i++) {
            leader.replicate(apply("val" + i)).get();
        }

        eventually(() -> {
            for (RaftNodeImpl raftNode : group.getNodes()) {
                assertThat(getCommitIndex(raftNode)).isEqualTo(entryCount);
                assertThat(getSnapshotEntry(raftNode).index()).isEqualTo(entryCount);
            }
        });

        leader.replicate(apply("valFinal")).get();

        eventually(() -> {
            for (RaftNodeImpl raftNode : group.getNodes()) {
                assertThat(getCommitIndex(raftNode)).isEqualTo(entryCount + 1);
                RaftDummyService service = group.getIntegration(raftNode.getLocalEndpoint()).getService();
                assertThat(service.size()).isEqualTo(entryCount + 1);
                for (int i = 0; i < entryCount; i++) {
                    assertThat(service.get(i + 1)).isEqualTo("val" + i);
                }
                assertThat(service.get(51)).isEqualTo("valFinal");
            }
        });
    }

    @Test(timeout = 300_000)
    public void when_followersMatchIndexIsUnknown_then_itInstallsSnapshot()
            throws ExecutionException, InterruptedException {
        int entryCount = 50;
        group = newGroup(3, RaftConfig.builder().setCommitIndexAdvanceCountToTakeSnapshot(entryCount).build());
        group.start();

        RaftNodeImpl leader = group.waitUntilLeaderElected();

        RaftNodeImpl[] followers = group.getNodesExcept(leader.getLocalEndpoint());
        RaftNodeImpl slowFollower = followers[1];

        group.dropMessagesToMember(leader.getLocalEndpoint(), slowFollower.getLocalEndpoint(), AppendEntriesRequest.class);
        group.dropMessagesToMember(leader.getLocalEndpoint(), slowFollower.getLocalEndpoint(), InstallSnapshotRequest.class);

        for (int i = 0; i < entryCount; i++) {
            leader.replicate(apply("val" + i)).get();
        }

        eventually(() -> assertThat(getSnapshotEntry(leader).index()).isEqualTo(entryCount));

        leader.replicate(apply("valFinal")).get();

        group.allowMessagesToMember(leader.getLocalEndpoint(), slowFollower.getLocalEndpoint(), InstallSnapshotRequest.class);

        eventually(() -> assertThat(getCommitIndex(slowFollower)).isEqualTo(entryCount));

        group.resetAllRulesFrom(leader.getLocalEndpoint());

        eventually(() -> {
            for (RaftNodeImpl raftNode : group.getNodes()) {
                assertThat(getCommitIndex(raftNode)).isEqualTo(entryCount + 1);
                RaftDummyService service = group.getIntegration(raftNode.getLocalEndpoint()).getService();
                assertThat(service.size()).isEqualTo(entryCount + 1);
                for (int i = 0; i < entryCount; i++) {
                    assertThat(service.get(i + 1)).isEqualTo("val" + i);
                }
                assertThat(service.get(51)).isEqualTo("valFinal");
            }
        });
    }

    @Test(timeout = 300_000)
    public void when_followersIsFarBehind_then_itInstallsSnapshot()
            throws ExecutionException, InterruptedException {
        int entryCount = 50;
        group = newGroup(3, RaftConfig.builder().setCommitIndexAdvanceCountToTakeSnapshot(entryCount).build());
        group.start();

        RaftNodeImpl leader = group.waitUntilLeaderElected();

        leader.replicate(apply("val0")).get();

        RaftNodeImpl[] followers = group.getNodesExcept(leader.getLocalEndpoint());
        RaftNodeImpl slowFollower = followers[1];

        group.dropMessagesToMember(leader.getLocalEndpoint(), slowFollower.getLocalEndpoint(), AppendEntriesRequest.class);
        group.dropMessagesToMember(leader.getLocalEndpoint(), slowFollower.getLocalEndpoint(), InstallSnapshotRequest.class);

        for (int i = 1; i < entryCount; i++) {
            leader.replicate(apply("val" + i)).get();
        }

        eventually(() -> assertThat(getSnapshotEntry(leader).index()).isEqualTo(entryCount));

        leader.replicate(apply("valFinal")).get();

        group.allowMessagesToMember(leader.getLocalEndpoint(), slowFollower.getLocalEndpoint(), InstallSnapshotRequest.class);

        eventually(() -> assertThat(getCommitIndex(slowFollower)).isEqualTo(entryCount));

        group.resetAllRulesFrom(leader.getLocalEndpoint());

        eventually(() -> {
            for (RaftNodeImpl raftNode : group.getNodes()) {
                assertThat(getCommitIndex(raftNode)).isEqualTo(entryCount + 1);
                RaftDummyService service = group.getIntegration(raftNode.getLocalEndpoint()).getService();
                assertThat(service.size()).isEqualTo(entryCount + 1);
                for (int i = 0; i < entryCount; i++) {
                    assertThat(service.get(i + 1)).isEqualTo("val" + i);
                }
                assertThat(service.get(51)).isEqualTo("valFinal");
            }
        });
    }

    @Test(timeout = 300_000)
    public void when_leaderMissesInstallSnapshotResponse_then_itAdvancesMatchIndexWithNextInstallSnapshotResponse()
            throws ExecutionException, InterruptedException {
        int entryCount = 50;
        RaftConfig config = RaftConfig.builder().setCommitIndexAdvanceCountToTakeSnapshot(entryCount)
                                      .setAppendEntriesRequestBackoffTimeoutMs(1000).build();
        group = newGroup(3, config);
        group.start();

        RaftNodeImpl leader = group.waitUntilLeaderElected();

        RaftNodeImpl[] followers = group.getNodesExcept(leader.getLocalEndpoint());
        RaftNodeImpl slowFollower = followers[1];

        // the leader cannot send AppendEntriesRPC to the follower
        group.dropMessagesToMember(leader.getLocalEndpoint(), slowFollower.getLocalEndpoint(), AppendEntriesRequest.class);

        // the follower cannot send append response to the leader after installing the snapshot
        group.dropMessagesToMember(slowFollower.getLocalEndpoint(), leader.getLocalEndpoint(),
                AppendEntriesSuccessResponse.class);

        for (int i = 0; i < entryCount; i++) {
            leader.replicate(apply("val" + i)).get();
        }

        eventually(() -> assertThat(getSnapshotEntry(leader).index()).isEqualTo(entryCount));

        leader.replicate(apply("valFinal")).get();

        group.resetAllRulesFrom(leader.getLocalEndpoint());

        eventually(() -> {
            for (RaftNodeImpl raftNode : group.getNodesExcept(slowFollower.getLocalEndpoint())) {
                assertThat(getCommitIndex(raftNode)).isEqualTo(entryCount + 1);
                RaftDummyService service = group.getIntegration(raftNode.getLocalEndpoint()).getService();
                assertThat(service.size()).isEqualTo(entryCount + 1);
                for (int i = 0; i < entryCount; i++) {
                    assertThat(service.get(i + 1)).isEqualTo("val" + i);
                }
                assertThat(service.get(51)).isEqualTo("valFinal");
            }

            assertThat(getCommitIndex(slowFollower)).isEqualTo(entryCount);
            RaftDummyService service = group.getIntegration(slowFollower.getLocalEndpoint()).getService();
            assertThat(service.size()).isEqualTo(entryCount);
            for (int i = 0; i < entryCount; i++) {
                assertThat(service.get(i + 1)).isEqualTo("val" + i);
            }
        });

        group.resetAllRulesFrom(slowFollower.getLocalEndpoint());

        long commitIndex = getCommitIndex(leader);

        eventually(() -> {
            for (RaftNode raftNode : group.getNodesExcept(leader.getLocalEndpoint())) {
                assertThat(getMatchIndex(leader, raftNode.getLocalEndpoint())).isEqualTo(commitIndex);
            }
        });
    }

    @Test(timeout = 300_000)
    public void when_followerMissesTheLastEntryThatGoesIntoTheSnapshot_then_itCatchesUpWithoutInstallingSnapshot()
            throws ExecutionException, InterruptedException {
        int entryCount = 50;
        group = newGroup(3, RaftConfig.builder().setCommitIndexAdvanceCountToTakeSnapshot(entryCount).build());
        group.start();

        RaftNodeImpl leader = group.waitUntilLeaderElected();

        RaftNodeImpl[] followers = group.getNodesExcept(leader.getLocalEndpoint());
        RaftNodeImpl slowFollower = followers[1];

        for (int i = 0; i < entryCount - 1; i++) {
            leader.replicate(apply("val" + i)).get();
        }

        eventually(() -> {
            for (RaftNodeImpl follower : group.getNodesExcept(leader.getLocalEndpoint())) {
                assertThat(getMatchIndex(leader, follower.getLocalEndpoint())).isEqualTo(entryCount - 1);
            }
        });

        group.dropMessagesToMember(leader.getLocalEndpoint(), slowFollower.getLocalEndpoint(), AppendEntriesRequest.class);
        group.dropMessagesToMember(leader.getLocalEndpoint(), slowFollower.getLocalEndpoint(), InstallSnapshotRequest.class);

        leader.replicate(apply("val" + (entryCount - 1))).get();

        eventually(() -> assertThat(getSnapshotEntry(leader).index()).isEqualTo(entryCount));

        leader.replicate(apply("valFinal")).get();

        group.allowMessagesToMember(leader.getLocalEndpoint(), slowFollower.getLocalEndpoint(), AppendEntriesRequest.class);

        eventually(() -> {
            for (RaftNodeImpl raftNode : group.getNodes()) {
                assertThat(getCommitIndex(raftNode)).isEqualTo(entryCount + 1);
                RaftDummyService service = group.getIntegration(raftNode.getLocalEndpoint()).getService();
                assertThat(service.size()).isEqualTo(entryCount + 1);
                for (int i = 0; i < entryCount; i++) {
                    assertThat(service.get(i + 1)).isEqualTo("val" + i);
                }
                assertThat(service.get(51)).isEqualTo("valFinal");
            }
        });
    }

    @Test(timeout = 300_000)
    public void when_followerMissesAFewEntriesBeforeTheSnapshot_then_itCatchesUpWithoutInstallingSnapshot()
            throws ExecutionException, InterruptedException {
        int entryCount = 50;
        int missingEntryCountOnSlowFollower = 4; // entryCount * 0.1 - 2
        group = newGroup(3, RaftConfig.builder().setCommitIndexAdvanceCountToTakeSnapshot(entryCount).build());
        group.start();

        RaftNodeImpl leader = group.waitUntilLeaderElected();

        RaftNodeImpl[] followers = group.getNodesExcept(leader.getLocalEndpoint());
        RaftNodeImpl slowFollower = followers[1];

        for (int i = 0; i < entryCount - missingEntryCountOnSlowFollower; i++) {
            leader.replicate(apply("val" + i)).get();
        }

        eventually(() -> {
            for (RaftNodeImpl follower : group.getNodesExcept(leader.getLocalEndpoint())) {
                assertThat(getMatchIndex(leader, follower.getLocalEndpoint()))
                        .isEqualTo(entryCount - missingEntryCountOnSlowFollower);
            }
        });

        group.dropMessagesToMember(leader.getLocalEndpoint(), slowFollower.getLocalEndpoint(), AppendEntriesRequest.class);
        group.dropMessagesToMember(leader.getLocalEndpoint(), slowFollower.getLocalEndpoint(), InstallSnapshotRequest.class);

        for (int i = entryCount - missingEntryCountOnSlowFollower; i < entryCount; i++) {
            leader.replicate(apply("val" + i)).get();
        }

        eventually(() -> assertThat(getSnapshotEntry(leader).index()).isEqualTo(entryCount));

        leader.replicate(apply("valFinal")).get();

        group.allowMessagesToMember(leader.getLocalEndpoint(), slowFollower.getLocalEndpoint(), AppendEntriesRequest.class);

        eventually(() -> {
            for (RaftNodeImpl raftNode : group.getNodes()) {
                assertThat(getCommitIndex(raftNode)).isEqualTo(entryCount + 1);
                RaftDummyService service = group.getIntegration(raftNode.getLocalEndpoint()).getService();
                assertThat(service.size()).isEqualTo(entryCount + 1);
                for (int i = 0; i < entryCount; i++) {
                    assertThat(service.get(i + 1)).isEqualTo("val" + i);
                }
                assertThat(service.get(51)).isEqualTo("valFinal");
            }
        });
    }

    @Test(timeout = 300_000)
    public void when_isolatedLeaderAppendsEntries_then_itInvalidatesTheirFeaturesUponInstallSnapshot()
            throws ExecutionException, InterruptedException {
        int entryCount = 50;
        group = newGroup(3, RaftConfig.builder().setCommitIndexAdvanceCountToTakeSnapshot(entryCount).build());
        group.start();

        RaftNodeImpl leader = group.waitUntilLeaderElected();
        RaftNodeImpl[] followers = group.getNodesExcept(leader.getLocalEndpoint());

        for (int i = 0; i < 40; i++) {
            leader.replicate(apply("val" + i)).get();
        }

        eventually(() -> {
            for (RaftNodeImpl raftNode : group.getNodes()) {
                assertThat(getCommitIndex(raftNode)).isEqualTo(40);
            }
        });

        group.splitMembers(leader.getLocalEndpoint());

        eventually(() -> {
            for (RaftNodeImpl raftNode : followers) {
                RaftEndpoint leaderEndpoint = getLeaderMember(raftNode);
                assertThat(leaderEndpoint).isNotNull().isNotEqualTo(leader.getLocalEndpoint());
            }
        });

        List<Future> futures = new ArrayList<>();
        for (int i = 40; i < 45; i++) {
            Future f = leader.replicate(apply("isolated" + i));
            futures.add(f);
        }

        RaftNodeImpl newLeader = group.getNode(getLeaderMember(followers[0]));

        for (int i = 40; i < 51; i++) {
            newLeader.replicate(apply("val" + i)).get();
        }

        eventually(() -> {
            for (RaftNodeImpl raftNode : followers) {
                assertThat(getSnapshotEntry(raftNode).index()).isGreaterThan(0);
            }
        });

        group.dropMessagesToMember(leader.getLocalEndpoint(), followers[0].getLocalEndpoint(), AppendEntriesRequest.class);
        group.dropMessagesToMember(leader.getLocalEndpoint(), followers[1].getLocalEndpoint(), AppendEntriesRequest.class);
        group.merge();

        for (Future f : futures) {
            try {
                f.get();
                fail();
            } catch (ExecutionException e) {
                assertThat(e).hasCauseInstanceOf(OperationResultUnknownException.class);
            }
        }

        eventually(() -> {
            for (RaftNodeImpl raftNode : group.getNodes()) {
                assertThat(getCommitIndex(raftNode)).isEqualTo(51);
                RaftDummyService service = group.getIntegration(raftNode.getLocalEndpoint()).getService();
                assertThat(service.size()).isEqualTo(51);
                for (int i = 0; i < 51; i++) {
                    assertThat(service.get(i + 1)).isEqualTo("val" + i);
                }

            }
        });
    }

    @Test(timeout = 300_000)
    public void when_followersLastAppendIsMembershipChange_then_itUpdatesRaftNodeStateWithInstalledSnapshot()
            throws ExecutionException, InterruptedException {
        int entryCount = 50;
        group = newGroup(5, RaftConfig.builder().setCommitIndexAdvanceCountToTakeSnapshot(entryCount).build());
        group.start();

        RaftNodeImpl leader = group.waitUntilLeaderElected();
        RaftNodeImpl[] followers = group.getNodesExcept(leader.getLocalEndpoint());

        leader.replicate(apply("val")).get();

        eventually(() -> {
            for (RaftNodeImpl follower : followers) {
                assertThat(getCommitIndex(follower)).isEqualTo(1);
            }
        });

        RaftNodeImpl slowFollower = followers[0];

        for (RaftNodeImpl follower : followers) {
            if (follower != slowFollower) {
                group.dropMessagesToMember(follower.getLocalEndpoint(), follower.getLeaderEndpoint(),
                        AppendEntriesSuccessResponse.class);
                group.dropMessagesToMember(follower.getLocalEndpoint(), follower.getLeaderEndpoint(),
                        AppendEntriesFailureResponse.class);
            }
        }

        RaftNodeImpl newRaftNode1 = group.createNewRaftNode();
        Future f1 = leader.changeMembership(newRaftNode1.getLocalEndpoint(), MembershipChangeMode.ADD, 0);

        eventually(() -> {
            for (RaftNodeImpl follower : followers) {
                assertThat(getLastLogOrSnapshotEntry(follower).index()).isEqualTo(2);
            }
        });

        group.dropMessagesToMember(leader.getLocalEndpoint(), slowFollower.getLocalEndpoint(), AppendEntriesRequest.class);
        group.dropMessagesToMember(leader.getLocalEndpoint(), slowFollower.getLocalEndpoint(), InstallSnapshotRequest.class);

        for (RaftNodeImpl follower : followers) {
            if (follower != slowFollower) {
                group.allowAllMessagesToMember(follower.getLocalEndpoint(), leader.getLeaderEndpoint());
            }
        }

        f1.get();

        for (int i = 0; i < entryCount; i++) {
            leader.replicate(apply("val" + i)).get();
        }

        eventually(() -> assertThat(getSnapshotEntry(leader).index()).isGreaterThanOrEqualTo(entryCount));

        group.allowAllMessagesToMember(leader.getLeaderEndpoint(), slowFollower.getLocalEndpoint());

        eventually(() -> assertThat(getSnapshotEntry(slowFollower).index()).isGreaterThanOrEqualTo(entryCount));

        eventually(() -> {
            assertThat(getCommittedGroupMembers(slowFollower).index()).isEqualTo(getCommittedGroupMembers(leader).index());
            assertThat(getStatus(slowFollower)).isEqualTo(ACTIVE);
        });
    }

    @Test(timeout = 300_000)
    public void testMembershipChangeBlocksSnapshotBug()
            throws ExecutionException, InterruptedException {
        // The comments below show how the code behaves before the mentioned bug is fixed.

        int commitIndexAdvanceCount = 50;
        int uncommittedEntryCount = 10;
        RaftConfig config = RaftConfig.builder().setCommitIndexAdvanceCountToTakeSnapshot(commitIndexAdvanceCount)
                                      .setUncommittedLogEntryCountToRejectNewAppends(uncommittedEntryCount).build();
        group = newGroup(3, config);
        group.start();

        RaftNodeImpl leader = group.waitUntilLeaderElected();
        RaftNodeImpl[] followers = group.getNodesExcept(leader.getLocalEndpoint());

        group.dropMessagesToMember(leader.getLocalEndpoint(), followers[0].getLocalEndpoint(), AppendEntriesRequest.class);

        while (getSnapshotEntry(leader).index() == 0) {
            leader.replicate(apply("into_snapshot")).get();
        }

        // now, the leader has taken a snapshot.
        // It also keeps some already committed entries in the log because followers[0] hasn't appended them.
        // LOG: [ <46 - 49>, <50>], SNAPSHOT INDEX: 50, COMMIT INDEX: 50

        long leaderCommitIndex = getCommitIndex(leader);
        do {
            leader.replicate(apply("committed_after_snapshot")).get();
        } while (getCommitIndex(leader) < leaderCommitIndex + commitIndexAdvanceCount - 1);

        // committing new entries.
        // one more entry is needed to take the next snapshot.
        // LOG: [ <46 - 49>, <50>, <51 - 99 (committed)> ], SNAPSHOT INDEX: 50, COMMIT INDEX: 99

        group.dropMessagesToMember(leader.getLocalEndpoint(), followers[1].getLocalEndpoint(), AppendEntriesRequest.class);

        for (int i = 0; i < uncommittedEntryCount - 1; i++) {
            leader.replicate(apply("uncommitted_after_snapshot"));
        }

        // appended some more entries which will not be committed because the leader has no majority.
        // the last uncommitted index is reserved for membership changed.
        // LOG: [ <46 - 49>, <50>, <51 - 99 (committed)>, <100 - 108 (uncommitted)> ], SNAPSHOT INDEX: 50, COMMIT INDEX: 99
        // There are only 2 empty indices in the log.

        RaftNodeImpl newRaftNode = group.createNewRaftNode();

        Function<Object, Object> alterFunc = o -> {
            if (o instanceof AppendEntriesRequest) {
                AppendEntriesRequest request = (AppendEntriesRequest) o;
                LogEntry[] entries = request.entries();
                if (entries.length > 0) {
                    if (entries[entries.length - 1].operation() instanceof UpdateRaftGroupMembersOp) {
                        entries = Arrays.copyOf(entries, entries.length - 1);
                        return new AppendEntriesRequest(request.leader(), request.term(), request.prevLogTerm(),
                                request.prevLogIndex(), request.leaderCommitIndex(), entries, 0);
                    } else if (entries[0].operation() instanceof UpdateRaftGroupMembersOp) {
                        entries = new LogEntry[0];
                        return new AppendEntriesRequest(request.leader(), request.term(), request.prevLogTerm(),
                                request.prevLogIndex(), request.leaderCommitIndex(), entries, 0);
                    }
                }
            }

            return null;
        };

        group.alterMessagesToMember(leader.getLocalEndpoint(), followers[1].getLocalEndpoint(), alterFunc);
        group.alterMessagesToMember(leader.getLocalEndpoint(), newRaftNode.getLocalEndpoint(), alterFunc);

        long lastLogIndex1 = getLastLogOrSnapshotEntry(leader).index();

        leader.changeMembership(newRaftNode.getLocalEndpoint(), MembershipChangeMode.ADD, 0);

        // When the membership change entry is appended, the leader's Log will be as following:
        // LOG: [ <46 - 49>, <50>, <51 - 99 (committed)>, <100 - 108 (uncommitted)>, <109 (membership change)> ], SNAPSHOT INDEX: 50, COMMIT INDEX: 99

        eventually(() -> assertThat(getLastLogOrSnapshotEntry(leader).index()).isGreaterThan(lastLogIndex1));

        group.allowMessagesToMember(leader.getLocalEndpoint(), followers[1].getLocalEndpoint(), AppendEntriesRequest.class);

        // Then, only the entries before the membership change will be committed because we alter the append request. The log will be:
        // LOG: [ <46 - 49>, <50>, <51 - 108 (committed)>, <109 (membership change)> ], SNAPSHOT INDEX: 50, COMMIT INDEX: 108
        // There is only 1 empty index in the log.

        eventually(() -> {
            assertThat(getCommitIndex(leader)).isEqualTo(lastLogIndex1);
            assertThat(getCommitIndex(followers[1])).isEqualTo(lastLogIndex1);
        });

        //        eventually(() -> {
        //        assertThat(getCommitIndex(leader)).isEqualTo(lastLogIndex1 + 1);
        //        assertThat(getCommitIndex(followers[1])).isEqualTo(lastLogIndex1 + 1);
        //        });

        long lastLogIndex2 = getLastLogOrSnapshotEntry(leader).index();

        leader.replicate(apply("after_membership_change_append"));

        eventually(() -> assertThat(getLastLogOrSnapshotEntry(leader).index()).isGreaterThan(lastLogIndex2));

        // Now the log is full. There is no empty space left.
        // LOG: [ <46 - 49>, <50>, <51 - 108 (committed)>, <109 (membership change)>, <110 (uncommitted)> ], SNAPSHOT INDEX: 50, COMMIT INDEX: 108

        long lastLogIndex3 = getLastLogOrSnapshotEntry(leader).index();

        Future f = leader.replicate(apply("after_membership_change_append"));

        eventually(() -> assertThat(getLastLogOrSnapshotEntry(leader).index()).isGreaterThan(lastLogIndex3));

        assertThat(f).isNotDone();
    }

    @Test(timeout = 300_000)
    public void when_slowFollowerReceivesAppendRequestThatDoesNotFitIntoItsRaftLog_then_itTruncatesAppendRequestEntries()
            throws ExecutionException, InterruptedException {
        int appendRequestMaxEntryCount = 100;
        int commitIndexAdvanceCount = 100;
        int uncommittedEntryCount = 10;

        RaftConfig config = RaftConfig.builder().setAppendEntriesRequestMaxLogEntryCount(appendRequestMaxEntryCount)
                                      .setCommitIndexAdvanceCountToTakeSnapshot(commitIndexAdvanceCount)
                                      .setUncommittedLogEntryCountToRejectNewAppends(uncommittedEntryCount).build();
        group = newGroup(5, config);
        group.start();

        RaftNodeImpl leader = group.waitUntilLeaderElected();
        RaftNodeImpl[] followers = group.getNodesExcept(leader.getLocalEndpoint());
        RaftNodeImpl slowFollower1 = followers[0];
        RaftNodeImpl slowFollower2 = followers[1];

        int count = 1;
        for (int i = 0; i < commitIndexAdvanceCount; i++) {
            leader.replicate(apply("val" + (count++))).get();
        }

        eventually(() -> {
            for (RaftNodeImpl node : group.getNodes()) {
                assertThat(getSnapshotEntry(node).index()).isGreaterThan(0);
            }
        });

        group.dropMessagesToMember(leader.getLocalEndpoint(), slowFollower1.getLocalEndpoint(), AppendEntriesRequest.class);

        for (int i = 0; i < commitIndexAdvanceCount - 1; i++) {
            leader.replicate(apply("val" + (count++))).get();
        }

        eventually(() -> assertThat(getCommitIndex(slowFollower2)).isEqualTo(getCommitIndex(leader)));

        // slowFollower2's log: [ <91 - 100 before snapshot>, <100 snapshot>, <101 - 199 committed> ]

        group.dropMessagesToMember(leader.getLocalEndpoint(), slowFollower2.getLocalEndpoint(), AppendEntriesRequest.class);

        for (int i = 0; i < commitIndexAdvanceCount / 2; i++) {
            leader.replicate(apply("val" + (count++))).get();
        }

        eventually(() -> assertThat(getSnapshotEntry(leader).index()).isGreaterThan(commitIndexAdvanceCount));

        // leader's log: [ <191 - 199 before snapshot>, <200 snapshot>, <201 - 249 committed> ]

        group.allowMessagesToMember(leader.getLocalEndpoint(), slowFollower2.getLocalEndpoint(), AppendEntriesRequest.class);

        // leader replicates 50 entries to slowFollower2 but slowFollower2 has only available capacity of 11 indices.
        // so, slowFollower2 appends 11 of these 50 entries in the first AppendRequest, takes a snapshot,
        // and receives another AppendRequest for the remaining entries...

        eventually(() -> {
            assertThat(getCommitIndex(slowFollower2)).isEqualTo(getCommitIndex(leader));
            assertThat(getSnapshotEntry(slowFollower2).index()).isGreaterThan(commitIndexAdvanceCount);
        });
    }

}
