package com.hazelcast.raft.impl;

import com.hazelcast.raft.RaftConfig;
import com.hazelcast.raft.RaftEndpoint;
import com.hazelcast.raft.RaftGroupMembers;
import com.hazelcast.raft.RaftNodeStatus;
import com.hazelcast.raft.exception.CannotReplicateException;
import com.hazelcast.raft.impl.local.LocalRaftGroup;
import com.hazelcast.raft.impl.local.RaftDummyService;
import com.hazelcast.raft.impl.msg.AppendEntriesFailureResponse;
import com.hazelcast.raft.impl.msg.AppendEntriesRequest;
import com.hazelcast.raft.impl.msg.AppendEntriesSuccessResponse;
import com.hazelcast.raft.impl.msg.PreVoteRequest;
import com.hazelcast.raft.impl.msg.VoteRequest;
import org.junit.After;
import org.junit.Test;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import static com.hazelcast.raft.MembershipChangeMode.ADD;
import static com.hazelcast.raft.MembershipChangeMode.REMOVE;
import static com.hazelcast.raft.RaftConfig.DEFAULT_RAFT_CONFIG;
import static com.hazelcast.raft.impl.local.RaftDummyService.apply;
import static com.hazelcast.raft.impl.util.AssertUtil.allTheTime;
import static com.hazelcast.raft.impl.util.AssertUtil.eventually;
import static com.hazelcast.raft.impl.util.RaftUtil.getCommitIndex;
import static com.hazelcast.raft.impl.util.RaftUtil.getCommittedGroupMembers;
import static com.hazelcast.raft.impl.util.RaftUtil.getLastGroupMembers;
import static com.hazelcast.raft.impl.util.RaftUtil.getLastLogOrSnapshotEntry;
import static com.hazelcast.raft.impl.util.RaftUtil.getLeaderMember;
import static com.hazelcast.raft.impl.util.RaftUtil.getSnapshotEntry;
import static com.hazelcast.raft.impl.util.RaftUtil.getStatus;
import static com.hazelcast.raft.impl.util.RaftUtil.newGroup;
import static java.util.Arrays.asList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.fail;

/**
 * @author mdogan
 * @author metanet
 */
public class MembershipChangeTest {

    private LocalRaftGroup group;

    @After
    public void destroy() {
        if (group != null) {
            group.destroy();
        }
    }

    @Test(timeout = 300_000)
    public void when_newRaftNodeJoins_then_itAppendsMissingEntries()
            throws ExecutionException, InterruptedException {
        group = newGroup(3);
        group.start();

        RaftNodeImpl leader = group.waitUntilLeaderElected();
        leader.replicate(apply("val")).get();

        RaftGroupMembers initialMembers = leader.getInitialMembers();

        RaftNodeImpl newRaftNode = group.createNewRaftNode();

        leader.changeMembership(newRaftNode.getLocalEndpoint(), ADD, 0).get();

        assertThat(leader.getInitialMembers().members()).isEqualTo(initialMembers.members());
        assertThat(leader.getCommittedMembers().isKnownMember(newRaftNode.getLocalEndpoint())).isTrue();
        assertThat(leader.getEffectiveMembers().isKnownMember(newRaftNode.getLocalEndpoint())).isTrue();

        long commitIndex = getCommitIndex(leader);
        eventually(() -> assertThat(getCommitIndex(newRaftNode)).isEqualTo(commitIndex));

        RaftGroupMembers lastGroupMembers = getLastGroupMembers(leader);
        eventually(() -> {
            RaftNodeImpl[] nodes = group.getNodes();
            for (RaftNodeImpl raftNode : nodes) {
                assertThat(getStatus(raftNode)).isEqualTo(RaftNodeStatus.ACTIVE);
                assertThat(getLastGroupMembers(raftNode).members()).isEqualTo(lastGroupMembers.members());
                assertThat(getLastGroupMembers(raftNode).index()).isEqualTo(lastGroupMembers.index());
                assertThat(getCommittedGroupMembers(raftNode).members()).isEqualTo(lastGroupMembers.members());
                assertThat(getCommittedGroupMembers(raftNode).index()).isEqualTo(lastGroupMembers.index());
            }
        });

        RaftDummyService service = group.getIntegration(newRaftNode.getLocalEndpoint()).getService();
        assertThat(service.size()).isEqualTo(1);
        assertThat(service.values()).contains("val");

        assertThat(newRaftNode.getInitialMembers().members()).isEqualTo(initialMembers.members());
        assertThat(newRaftNode.getCommittedMembers().members()).isEqualTo(leader.getCommittedMembers().members());
        assertThat(newRaftNode.getCommittedMembers().index()).isEqualTo(leader.getCommittedMembers().index());
        assertThat(newRaftNode.getEffectiveMembers().members()).isEqualTo(leader.getEffectiveMembers().members());
        assertThat(newRaftNode.getEffectiveMembers().index()).isEqualTo(leader.getEffectiveMembers().index());
    }

    @Test(timeout = 300_000)
    public void when_followerLeaves_then_itIsRemovedFromTheGroupMembers()
            throws ExecutionException, InterruptedException {
        group = newGroup(3);
        group.start();

        RaftNodeImpl leader = group.waitUntilLeaderElected();
        RaftNodeImpl[] followers = group.getNodesExcept(leader.getLocalEndpoint());
        RaftNodeImpl leavingFollower = followers[0];
        RaftNodeImpl stayingFollower = followers[1];

        leader.replicate(apply("val")).get();

        leader.changeMembership(leavingFollower.getLocalEndpoint(), REMOVE, 0).get();

        eventually(() -> {
            for (RaftNodeImpl raftNode : asList(leader, stayingFollower)) {
                assertThat(getLastGroupMembers(raftNode).isKnownMember(leavingFollower.getLocalEndpoint())).isFalse();
                assertThat(getCommittedGroupMembers(raftNode).isKnownMember(leavingFollower.getLocalEndpoint())).isFalse();
            }
        });

        group.terminateNode(leavingFollower.getLocalEndpoint());
    }

    @Test(timeout = 300_000)
    public void when_newRaftNodeJoinsAfterAnotherNodeLeaves_then_itAppendsMissingEntries()
            throws ExecutionException, InterruptedException {
        group = newGroup(3);
        group.start();

        RaftNodeImpl leader = group.waitUntilLeaderElected();
        leader.replicate(apply("val")).get();

        RaftNodeImpl[] followers = group.getNodesExcept(leader.getLocalEndpoint());
        RaftNodeImpl leavingFollower = followers[0];
        RaftNodeImpl stayingFollower = followers[1];

        long newMembersCommitIndex = leader.changeMembership(leavingFollower.getLocalEndpoint(), REMOVE, 0).get();

        RaftNodeImpl newRaftNode = group.createNewRaftNode();

        leader.changeMembership(newRaftNode.getLocalEndpoint(), ADD, newMembersCommitIndex).get();

        long commitIndex = getCommitIndex(leader);
        eventually(() -> assertThat(getCommitIndex(newRaftNode)).isEqualTo(commitIndex));

        RaftGroupMembers lastGroupMembers = getLastGroupMembers(leader);
        eventually(() -> {
            for (RaftNodeImpl raftNode : asList(leader, stayingFollower, newRaftNode)) {
                assertThat(getStatus(raftNode)).isEqualTo(RaftNodeStatus.ACTIVE);
                assertThat(getLastGroupMembers(raftNode).members()).isEqualTo(lastGroupMembers.members());
                assertThat(getLastGroupMembers(raftNode).index()).isEqualTo(lastGroupMembers.index());
                assertThat(getCommittedGroupMembers(raftNode).members()).isEqualTo(lastGroupMembers.members());
                assertThat(getCommittedGroupMembers(raftNode).index()).isEqualTo(lastGroupMembers.index());
                assertThat(getLastGroupMembers(raftNode).isKnownMember(leavingFollower.getLocalEndpoint())).isFalse();
                assertThat(getCommittedGroupMembers(raftNode).isKnownMember(leavingFollower.getLocalEndpoint())).isFalse();
            }
        });

        RaftDummyService service = group.getIntegration(newRaftNode.getLocalEndpoint()).getService();
        assertThat(service.size()).isEqualTo(1);
        assertThat(service.values()).contains("val");
    }

    @Test(timeout = 300_000)
    public void when_newRaftNodeJoinsAfterAnotherNodeLeavesAndSnapshotIsTaken_then_itAppendsMissingEntries()
            throws ExecutionException, InterruptedException {
        int commitIndexAdvanceCountToSnapshot = 10;
        group = newGroup(3,
                RaftConfig.builder().setCommitIndexAdvanceCountToTakeSnapshot(commitIndexAdvanceCountToSnapshot).build());
        group.start();

        RaftNodeImpl leader = group.waitUntilLeaderElected();

        leader.replicate(apply("val")).get();

        RaftNodeImpl[] followers = group.getNodesExcept(leader.getLocalEndpoint());
        RaftNodeImpl leavingFollower = followers[0];
        RaftNodeImpl stayingFollower = followers[1];

        long newMembersIndex = leader.changeMembership(leavingFollower.getLocalEndpoint(), REMOVE, 0).get();

        for (int i = 0; i < commitIndexAdvanceCountToSnapshot; i++) {
            leader.replicate(apply("val" + i)).get();
        }

        eventually(() -> assertThat(getSnapshotEntry(leader).index()).isGreaterThan(0));

        RaftNodeImpl newRaftNode = group.createNewRaftNode();

        leader.changeMembership(newRaftNode.getLocalEndpoint(), ADD, newMembersIndex).get();

        long commitIndex = getCommitIndex(leader);
        eventually(() -> assertThat(getCommitIndex(newRaftNode)).isEqualTo(commitIndex));

        RaftGroupMembers lastGroupMembers = getLastGroupMembers(leader);
        eventually(() -> {
            for (RaftNodeImpl raftNode : asList(leader, stayingFollower, newRaftNode)) {
                assertThat(getStatus(raftNode)).isEqualTo(RaftNodeStatus.ACTIVE);
                assertThat(getLastGroupMembers(raftNode).members()).isEqualTo(lastGroupMembers.members());
                assertThat(getLastGroupMembers(raftNode).index()).isEqualTo(lastGroupMembers.index());
                assertThat(getCommittedGroupMembers(raftNode).members()).isEqualTo(lastGroupMembers.members());
                assertThat(getCommittedGroupMembers(raftNode).index()).isEqualTo(lastGroupMembers.index());
                assertThat(getLastGroupMembers(raftNode).isKnownMember(leavingFollower.getLocalEndpoint())).isFalse();
                assertThat(getCommittedGroupMembers(raftNode).isKnownMember(leavingFollower.getLocalEndpoint())).isFalse();
            }
        });

        RaftDummyService service = group.getIntegration(newRaftNode.getLocalEndpoint()).getService();
        assertThat(service.size()).isEqualTo(commitIndexAdvanceCountToSnapshot + 1);
        assertThat(service.values()).contains("val");
        for (int i = 0; i < commitIndexAdvanceCountToSnapshot; i++) {
            assertThat(service.values()).contains("val" + i);
        }
    }

    @Test(timeout = 300_000)
    public void when_leaderLeaves_then_itIsRemovedFromTheGroupMembers()
            throws ExecutionException, InterruptedException {
        group = newGroup(3);
        group.start();

        RaftNodeImpl leader = group.waitUntilLeaderElected();
        RaftNodeImpl[] followers = group.getNodesExcept(leader.getLocalEndpoint());

        leader.replicate(apply("val")).get();
        leader.changeMembership(leader.getLocalEndpoint(), REMOVE, 0).get();

        assertThat(leader.getStatus()).isEqualTo(RaftNodeStatus.STEPPED_DOWN);

        eventually(() -> {
            for (RaftNodeImpl raftNode : followers) {
                assertThat(getLastGroupMembers(raftNode).isKnownMember(leader.getLocalEndpoint())).isFalse();
                assertThat(getCommittedGroupMembers(raftNode).isKnownMember(leader.getLocalEndpoint())).isFalse();
            }
        });
    }

    @Test(timeout = 300_000)
    public void when_leaderLeaves_then_itCannotVoteForCommitOfMemberChange()
            throws ExecutionException, InterruptedException {
        group = newGroup(3, RaftConfig.builder().setLeaderHeartbeatPeriodMs(1000).build());
        group.start();

        RaftNodeImpl leader = group.waitUntilLeaderElected();
        RaftNodeImpl[] followers = group.getNodesExcept(leader.getLocalEndpoint());

        group.dropMessagesToMember(followers[0].getLocalEndpoint(), leader.getLocalEndpoint(),
                AppendEntriesSuccessResponse.class);
        leader.replicate(apply("val")).get();

        leader.changeMembership(leader.getLocalEndpoint(), REMOVE, 0);

        allTheTime(() -> assertThat(getCommitIndex(leader)).isEqualTo(1), 10);
    }

    @Test(timeout = 300_000)
    public void when_leaderLeaves_then_followersElectNewLeader()
            throws ExecutionException, InterruptedException {
        group = newGroup(3);
        group.start();

        RaftNodeImpl leader = group.waitUntilLeaderElected();
        RaftNodeImpl[] followers = group.getNodesExcept(leader.getLocalEndpoint());

        leader.replicate(apply("val")).get();
        leader.changeMembership(leader.getLocalEndpoint(), REMOVE, 0).get();

        eventually(() -> {
            for (RaftNodeImpl raftNode : followers) {
                assertThat(getLastGroupMembers(raftNode).isKnownMember(leader.getLocalEndpoint())).isFalse();
                assertThat(getCommittedGroupMembers(raftNode).isKnownMember(leader.getLocalEndpoint())).isFalse();
            }
        });

        group.terminateNode(leader.getLocalEndpoint());

        eventually(() -> {
            for (RaftNodeImpl raftNode : followers) {
                RaftEndpoint newLeader = getLeaderMember(raftNode);
                assertThat(newLeader).isNotNull().isNotEqualTo(leader.getLocalEndpoint());
            }
        });
    }

    @Test(timeout = 300_000)
    public void when_membershipChangeRequestIsMadeWithWrongType_then_theChangeFails()
            throws ExecutionException, InterruptedException {
        group = newGroup(3);
        group.start();

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
    public void when_nonExistingEndpointIsRemoved_then_theChangeFails()
            throws ExecutionException, InterruptedException {
        group = newGroup(3);
        group.start();

        RaftNodeImpl leader = group.waitUntilLeaderElected();
        RaftNodeImpl leavingFollower = group.getAnyFollowerNode();

        leader.replicate(apply("val")).get();
        long newMembersIndex = leader.changeMembership(leavingFollower.getLocalEndpoint(), REMOVE, 0).get();

        try {
            leader.changeMembership(leavingFollower.getLocalEndpoint(), REMOVE, newMembersIndex).get();
            fail();
        } catch (ExecutionException e) {
            assertThat(e).hasCauseInstanceOf(IllegalArgumentException.class);
        }
    }

    @Test(timeout = 300_000)
    public void when_existingEndpointIsAdded_then_theChangeFails()
            throws ExecutionException, InterruptedException {
        group = newGroup(3);
        group.start();

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

        group = newGroup(3);
        group.start();

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

        group = newGroup(3, DEFAULT_RAFT_CONFIG, true);
        group.start();

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
            throws ExecutionException, InterruptedException {
        group = newGroup(3, RaftConfig.builder().setCommitIndexAdvanceCountToTakeSnapshot(5).build());
        group.start();

        RaftNodeImpl leader = group.waitUntilLeaderElected();
        for (int i = 0; i < 4; i++) {
            leader.replicate(apply("val" + i)).get();
        }

        RaftNodeImpl newRaftNode = group.createNewRaftNode();

        group.dropMessagesToMember(leader.getLocalEndpoint(), newRaftNode.getLocalEndpoint(), AppendEntriesRequest.class);

        leader.changeMembership(newRaftNode.getLocalEndpoint(), ADD, 0).get();

        eventually(() -> assertThat(getSnapshotEntry(leader).index()).isGreaterThan(0));

        group.resetAllRulesFrom(leader.getLocalEndpoint());

        eventually(() -> {
            assertThat(getCommitIndex(newRaftNode)).isEqualTo(getCommitIndex(leader));
            assertThat(getLastGroupMembers(newRaftNode).members()).isEqualTo(getLastGroupMembers(leader).members());
            assertThat(getCommittedGroupMembers(newRaftNode).members()).isEqualTo(getLastGroupMembers(leader).members());
            RaftDummyService service = group.getIntegration(newRaftNode.getLocalEndpoint()).getService();
            assertThat(service.size()).isEqualTo(4);
        });
    }

    @Test(timeout = 300_000)
    public void when_leaderFailsWhileLeavingRaftGroup_othersCommitTheMemberChange()
            throws ExecutionException, InterruptedException {
        group = newGroup(3);
        group.start();

        RaftNodeImpl leader = group.waitUntilLeaderElected();
        RaftNodeImpl[] followers = group.getNodesExcept(leader.getLocalEndpoint());

        leader.replicate(apply("val")).get();

        for (RaftNodeImpl follower : followers) {
            group.dropMessagesToMember(follower.getLocalEndpoint(), leader.getLocalEndpoint(),
                    AppendEntriesSuccessResponse.class);
            group.dropMessagesToMember(follower.getLocalEndpoint(), leader.getLocalEndpoint(),
                    AppendEntriesFailureResponse.class);
        }

        leader.changeMembership(leader.getLocalEndpoint(), REMOVE, 0);

        eventually(() -> {
            for (RaftNodeImpl follower : followers) {
                assertThat(getLastLogOrSnapshotEntry(follower).index()).isEqualTo(2);
            }
        });

        group.terminateNode(leader.getLocalEndpoint());

        eventually(() -> {
            for (RaftNodeImpl follower : followers) {
                RaftEndpoint newLeaderEndpoint = getLeaderMember(follower);
                assertThat(newLeaderEndpoint).isNotNull().isNotEqualTo(leader.getLocalEndpoint());
            }
        });

        RaftNodeImpl newLeader = group.getNode(getLeaderMember(followers[0]));
        newLeader.replicate(apply("val2"));

        eventually(() -> {
            for (RaftNodeImpl follower : followers) {
                assertThat(getCommittedGroupMembers(follower).isKnownMember(leader.getLocalEndpoint())).isFalse();
            }
        });
    }

    @Test(timeout = 300_000)
    public void when_followerAppendsMultipleMembershipChangesAtOnce_then_itCommitsThemCorrectly()
            throws ExecutionException, InterruptedException {
        group = newGroup(5, RaftConfig.builder().setLeaderHeartbeatPeriodMs(1000).build());
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
        group.dropMessagesToMember(leader.getLocalEndpoint(), newRaftNode1.getLocalEndpoint(), AppendEntriesRequest.class);
        Future<Long> f1 = leader.changeMembership(newRaftNode1.getLocalEndpoint(), ADD, 0);

        eventually(() -> {
            for (RaftNodeImpl follower : followers) {
                assertThat(getLastLogOrSnapshotEntry(follower).index()).isEqualTo(2);
            }
        });

        group.dropMessagesToMember(leader.getLocalEndpoint(), slowFollower.getLocalEndpoint(), AppendEntriesRequest.class);

        for (RaftNodeImpl follower : followers) {
            if (follower != slowFollower) {
                group.allowAllMessagesToMember(follower.getLocalEndpoint(), leader.getLeaderEndpoint());
            }
        }

        long newMembersIndex = f1.get();
        eventually(() -> {
            for (RaftNodeImpl follower : followers) {
                if (follower != slowFollower) {
                    assertThat(getCommittedGroupMembers(follower).members()).hasSize(6);
                } else {
                    assertThat(getCommittedGroupMembers(follower).members()).hasSize(5);
                    assertThat(getLastGroupMembers(follower).members()).hasSize(6);
                }
            }
        });

        RaftNodeImpl newRaftNode2 = group.createNewRaftNode();
        leader.changeMembership(newRaftNode2.getLocalEndpoint(), ADD, newMembersIndex).get();

        group.allowAllMessagesToMember(leader.getLocalEndpoint(), slowFollower.getLocalEndpoint());
        group.allowAllMessagesToMember(slowFollower.getLocalEndpoint(), leader.getLocalEndpoint());
        group.allowAllMessagesToMember(leader.getLocalEndpoint(), newRaftNode1.getLocalEndpoint());

        RaftGroupMembers leaderCommittedGroupMembers = getCommittedGroupMembers(leader);
        eventually(() -> {
            assertThat(getCommittedGroupMembers(slowFollower).index()).isEqualTo(leaderCommittedGroupMembers.index());
            assertThat(getCommittedGroupMembers(newRaftNode1).index()).isEqualTo(leaderCommittedGroupMembers.index());
            assertThat(getCommittedGroupMembers(newRaftNode2).index()).isEqualTo(leaderCommittedGroupMembers.index());
        });
    }

    @Test(timeout = 300_000)
    public void when_leaderIsSteppingDown_then_itDoesNotAcceptNewAppends()
            throws InterruptedException {
        group = newGroup(3, DEFAULT_RAFT_CONFIG, true);
        group.start();

        RaftNodeImpl leader = group.waitUntilLeaderElected();
        RaftNodeImpl[] followers = group.getNodesExcept(leader.getLocalEndpoint());
        group.dropMessagesToMember(leader.getLocalEndpoint(), followers[0].getLocalEndpoint(), AppendEntriesRequest.class);
        group.dropMessagesToMember(leader.getLocalEndpoint(), followers[1].getLocalEndpoint(), AppendEntriesRequest.class);

        Future f1 = leader.changeMembership(leader.getLocalEndpoint(), REMOVE, 0);
        Future f2 = leader.replicate(apply("1"));

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
            throws ExecutionException, InterruptedException {
        group = newGroup(3, DEFAULT_RAFT_CONFIG, true);
        group.start();

        RaftNodeImpl leader = group.waitUntilLeaderElected();
        RaftNodeImpl[] followers = group.getNodesExcept(leader.getLocalEndpoint());

        leader.replicate(apply("val1")).get();
        long oldLeaderCommitIndexBeforeMembershipChange = getCommitIndex(leader);

        eventually(() -> {
            for (RaftNodeImpl follower : followers) {
                assertThat(getCommitIndex(follower)).isEqualTo(oldLeaderCommitIndexBeforeMembershipChange);
            }
        });

        group.dropMessagesToMember(leader.getLocalEndpoint(), followers[0].getLocalEndpoint(), AppendEntriesRequest.class);
        group.dropMessagesToMember(leader.getLocalEndpoint(), followers[1].getLocalEndpoint(), AppendEntriesRequest.class);

        RaftNodeImpl newRaftNode = group.createNewRaftNode();

        leader.changeMembership(newRaftNode.getLocalEndpoint(), ADD, 0);

        eventually(() -> {
            long leaderLastLogIndex = getLastLogOrSnapshotEntry(leader).index();
            assertThat(leaderLastLogIndex).isGreaterThan(oldLeaderCommitIndexBeforeMembershipChange)
                                          .isEqualTo(getLastLogOrSnapshotEntry(newRaftNode).index());
        });

        group.dropMessagesToAll(newRaftNode.getLocalEndpoint(), PreVoteRequest.class);
        group.dropMessagesToAll(newRaftNode.getLocalEndpoint(), VoteRequest.class);

        group.terminateNode(leader.getLocalEndpoint());

        eventually(() -> {
            RaftEndpoint l0 = followers[0].getLeaderEndpoint();
            RaftEndpoint l1 = followers[1].getLeaderEndpoint();
            assertThat(l0).isNotNull().isNotEqualTo(leader.getLocalEndpoint());
            assertThat(l1).isNotNull().isNotEqualTo(leader.getLocalEndpoint()).isEqualTo(l0);
        });

        RaftNodeImpl newLeader = group.getNode(followers[0].getLeaderEndpoint());
        newLeader.replicate(apply("val1")).get();
        newLeader.changeMembership(newRaftNode.getLocalEndpoint(), ADD, 0).get();

        eventually(() -> {
            assertThat(getCommitIndex(newRaftNode)).isEqualTo(getCommitIndex(newLeader));
            assertThat(getCommittedGroupMembers(newRaftNode).index()).isEqualTo(getCommittedGroupMembers(newLeader).index());
        });
    }

}
