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

import static io.microraft.MembershipChangeMode.ADD_LEARNER;
import static io.microraft.MembershipChangeMode.ADD_OR_PROMOTE_TO_FOLLOWER;
import static io.microraft.MembershipChangeMode.REMOVE_MEMBER;
import static io.microraft.QueryPolicy.EVENTUAL_CONSISTENCY;
import static io.microraft.RaftNodeStatus.ACTIVE;
import static io.microraft.RaftNodeStatus.UPDATING_RAFT_GROUP_MEMBER_LIST;
import static io.microraft.RaftRole.FOLLOWER;
import static io.microraft.RaftRole.LEARNER;
import static io.microraft.impl.local.SimpleStateMachine.applyValue;
import static io.microraft.impl.local.SimpleStateMachine.queryLastValue;
import static io.microraft.impl.log.RaftLog.FIRST_VALID_LOG_INDEX;
import static io.microraft.test.util.AssertionUtils.eventually;
import static io.microraft.test.util.RaftTestUtils.getCommitIndex;
import static io.microraft.test.util.RaftTestUtils.getCommittedGroupMembers;
import static io.microraft.test.util.RaftTestUtils.getEffectiveGroupMembers;
import static io.microraft.test.util.RaftTestUtils.getInstallSnapshotCount;
import static io.microraft.test.util.RaftTestUtils.getLastApplied;
import static io.microraft.test.util.RaftTestUtils.getLastLogOrSnapshotEntry;
import static io.microraft.test.util.RaftTestUtils.getMatchIndex;
import static io.microraft.test.util.RaftTestUtils.getRole;
import static io.microraft.test.util.RaftTestUtils.getSnapshotChunkCollector;
import static io.microraft.test.util.RaftTestUtils.getSnapshotEntry;
import static io.microraft.test.util.RaftTestUtils.getStatus;
import static io.microraft.test.util.RaftTestUtils.readRaftState;
import static java.util.Collections.emptyList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.fail;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.function.BiFunction;
import java.util.function.Function;

import org.junit.After;
import org.junit.Test;

import io.microraft.Ordered;
import io.microraft.RaftConfig;
import io.microraft.RaftNode;
import io.microraft.RaftRole;
import io.microraft.exception.IndeterminateStateException;
import io.microraft.impl.handler.InstallSnapshotRequestHandler;
import io.microraft.impl.handler.PreVoteResponseHandler;
import io.microraft.impl.local.LocalRaftGroup;
import io.microraft.impl.local.SimpleStateMachine;
import io.microraft.impl.log.SnapshotChunkCollector;
import io.microraft.impl.task.HeartbeatTask;
import io.microraft.impl.task.PreVoteTimeoutTask;
import io.microraft.model.impl.groupop.DefaultUpdateRaftGroupMembersOpOrBuilder;
import io.microraft.model.impl.message.DefaultAppendEntriesRequestOrBuilder;
import io.microraft.model.log.LogEntry;
import io.microraft.model.log.RaftGroupMembersView;
import io.microraft.model.log.SnapshotChunk;
import io.microraft.model.log.SnapshotEntry;
import io.microraft.model.message.AppendEntriesFailureResponse;
import io.microraft.model.message.AppendEntriesRequest;
import io.microraft.model.message.AppendEntriesSuccessResponse;
import io.microraft.model.message.InstallSnapshotRequest;
import io.microraft.model.message.InstallSnapshotResponse;
import io.microraft.model.message.PreVoteResponse;
import io.microraft.model.message.RaftMessage;
import io.microraft.report.RaftGroupMembers;
import io.microraft.report.RaftNodeReport;
import io.microraft.test.util.BaseTest;

public class SnapshotTest extends BaseTest {

    private LocalRaftGroup group;

    @After
    public void destroy() {
        if (group != null) {
            group.destroy();
        }
    }

    @Test(timeout = 300_000)
    public void whenSnapshotTransferTermChangesThenOldChunksAreDiscarded() {
        RaftConfig config = RaftConfig.newBuilder().setTransferSnapshotsFromFollowersEnabled(false).build();
        group = LocalRaftGroup.newBuilder(3).setConfig(config).build();
        group.start();
        RaftNodeImpl leader = group.waitUntilLeaderElected();
        RaftNodeImpl follower = group.getAnyNodeExcept(leader.getLocalEndpoint());
        group.splitMembers(follower.getLocalEndpoint());
        readRaftState(follower, () -> {
            int term = follower.state().term();
            RaftGroupMembersView members = follower.state().committedGroupMembers()
                    .populate(follower.getModelFactory().createRaftGroupMembersViewBuilder());
            BiFunction<Integer, Map<Long, Object>, SnapshotChunk> chunk = (index, values) -> follower.getModelFactory()
                    .createSnapshotChunkBuilder().setIndex(3).setTerm(term).setSnapshotChunkIndex(index)
                    .setSnapshotChunkCount(2).setGroupMembersView(members).setOperation(values).build();
            BiFunction<Integer, SnapshotChunk, InstallSnapshotRequest> request = (transferTerm, snapshotChunk) -> follower
                    .getModelFactory().createInstallSnapshotRequestBuilder().setGroupId(follower.getGroupId())
                    .setSender(leader.getLocalEndpoint()).setSenderLeader(true).setTerm(transferTerm)
                    .setSnapshotIndex(3).setSnapshotTerm(term).setTotalSnapshotChunkCount(2)
                    .setSnapshotChunk(snapshotChunk).setGroupMembersView(members)
                    .setSnapshottedMembers(List.of(leader.getLocalEndpoint())).build();

            new InstallSnapshotRequestHandler(follower, request.apply(term, chunk.apply(0, Map.of(1L, "one")))).run();
            // A leader in a later term may use different chunk boundaries for the same snapshot.
            new InstallSnapshotRequestHandler(follower,
                    request.apply(term + 1, chunk.apply(1, Map.of(3L, "three")))).run();
            assertThat(follower.state().commitIndex()).isZero();
            assertThat(follower.state().snapshotChunkCollector().getChunks()).hasSize(1);
            assertThat(follower.state().snapshotChunkCollector().getChunks().get(0).getSnapshotChunkIndex())
                    .isEqualTo(1);

            new InstallSnapshotRequestHandler(follower,
                    request.apply(term + 1, chunk.apply(0, Map.of(1L, "one", 2L, "two")))).run();
            assertThat(follower.state().commitIndex()).isEqualTo(3);
            assertThat(group.getStateMachine(follower.getLocalEndpoint()).size()).isEqualTo(3);
            assertThat(group.getStateMachine(follower.getLocalEndpoint()).get(2)).isEqualTo("two");
            return null;
        });
    }

    @Test(timeout = 300_000)
    public void when_commitLogAdvances_then_snapshotIsTaken() {
        int entryCount = 50;
        RaftConfig config = RaftConfig.newBuilder().setCommitCountToTakeSnapshot(entryCount).build();
        group = LocalRaftGroup.start(3, config);

        RaftNodeImpl leader = group.waitUntilLeaderElected();

        for (int i = 0; i < entryCount; i++) {
            leader.replicate(applyValue("val" + i)).join();
        }

        eventually(() -> {
            for (RaftNodeImpl node : group.getNodes()) {
                assertThat(getCommitIndex(node)).isEqualTo(entryCount);
                assertThat(getSnapshotEntry(node).getIndex()).isEqualTo(entryCount);

                SimpleStateMachine stateMachine = group.getStateMachine(node.getLocalEndpoint());
                assertThat(stateMachine.size()).isEqualTo(entryCount);
                for (int i = 0; i < entryCount; i++) {
                    assertThat(stateMachine.get(i + 1)).isEqualTo("val" + i);
                }
            }
        });
    }

    @Test(timeout = 300_000)
    public void when_snapshotIsTaken_then_nextEntryIsCommitted() {
        int entryCount = 50;
        RaftConfig config = RaftConfig.newBuilder().setCommitCountToTakeSnapshot(entryCount).build();
        group = LocalRaftGroup.start(3, config);

        RaftNodeImpl leader = group.waitUntilLeaderElected();

        for (int i = 0; i < entryCount; i++) {
            leader.replicate(applyValue("val" + i)).join();
        }

        eventually(() -> {
            for (RaftNodeImpl node : group.getNodes()) {
                assertThat(getCommitIndex(node)).isEqualTo(entryCount);
                assertThat(getSnapshotEntry(node).getIndex()).isEqualTo(entryCount);
            }
        });

        leader.replicate(applyValue("valFinal")).join();

        eventually(() -> {
            for (RaftNodeImpl node : group.getNodes()) {
                assertThat(getCommitIndex(node)).isEqualTo(entryCount + 1);
                SimpleStateMachine stateMachine = group.getStateMachine(node.getLocalEndpoint());
                assertThat(stateMachine.size()).isEqualTo(entryCount + 1);
                for (int i = 0; i < entryCount; i++) {
                    assertThat(stateMachine.get(i + 1)).isEqualTo("val" + i);
                }
                assertThat(stateMachine.get(51)).isEqualTo("valFinal");
            }
        });
    }

    @Test(timeout = 300_000)
    public void when_followersMatchIndexIsUnknown_then_itInstallsSnapshotFromLeaderOnly() {
        when_followersMatchIndexIsUnknown_then_itInstallsSnapshot(false);
    }

    @Test(timeout = 300_000)
    public void when_followersMatchIndexIsUnknown_then_itInstallsSnapshotFromLeaderAndFollowers() {
        when_followersMatchIndexIsUnknown_then_itInstallsSnapshot(true);
    }

    private void when_followersMatchIndexIsUnknown_then_itInstallsSnapshot(
            boolean transferSnapshotFromFollowersEnabled) {
        int entryCount = 50;
        RaftConfig config = RaftConfig.newBuilder().setCommitCountToTakeSnapshot(entryCount)
                .setTransferSnapshotsFromFollowersEnabled(transferSnapshotFromFollowersEnabled).build();
        group = LocalRaftGroup.start(3, config);

        RaftNodeImpl leader = group.waitUntilLeaderElected();

        List<RaftNodeImpl> followers = group.getNodesExcept(leader.getLocalEndpoint());
        RaftNodeImpl slowFollower = followers.get(1);

        group.dropMessagesTo(leader.getLocalEndpoint(), slowFollower.getLocalEndpoint(), AppendEntriesRequest.class);
        group.dropMessagesTo(leader.getLocalEndpoint(), slowFollower.getLocalEndpoint(), InstallSnapshotRequest.class);

        for (int i = 0; i < entryCount; i++) {
            leader.replicate(applyValue("val" + i)).join();
        }

        eventually(() -> assertThat(getSnapshotEntry(leader).getIndex()).isEqualTo(entryCount));

        leader.replicate(applyValue("valFinal")).join();

        group.allowMessagesTo(leader.getLocalEndpoint(), slowFollower.getLocalEndpoint(), AppendEntriesRequest.class);
        group.allowMessagesTo(leader.getLocalEndpoint(), slowFollower.getLocalEndpoint(), InstallSnapshotRequest.class);

        eventually(() -> assertThat(getCommitIndex(slowFollower)).isEqualTo(getCommitIndex(leader)));

        group.resetAllRulesFrom(leader.getLocalEndpoint());

        eventually(() -> {
            for (RaftNodeImpl node : group.getNodes()) {
                assertThat(getCommitIndex(node)).isEqualTo(entryCount + 1);
                SimpleStateMachine stateMachine = group.getStateMachine(node.getLocalEndpoint());
                assertThat(stateMachine.size()).isEqualTo(entryCount + 1);
                for (int i = 0; i < entryCount; i++) {
                    assertThat(stateMachine.get(i + 1)).isEqualTo("val" + i);
                }
                assertThat(stateMachine.get(51)).isEqualTo("valFinal");
            }
        });

        assertThat(getSnapshotChunkCollector(slowFollower)).isNull();
    }

    @Test(timeout = 300_000)
    public void when_leaderKnowsOthersSnapshot_then_slowFollowerInstallsSnapshotFromLeaderWithoutOptimization() {
        when_leaderKnowsOtherFollowerInstalledSnapshot_then_slowFollowerInstallsSnapshot(false);
    }

    @Test(timeout = 300_000)
    public void when_leaderKnowsOthersSnapshot_then_slowFollowerInstallsSnapshotFromAllWithOptimization() {
        when_leaderKnowsOtherFollowerInstalledSnapshot_then_slowFollowerInstallsSnapshot(true);
    }

    private void when_leaderKnowsOtherFollowerInstalledSnapshot_then_slowFollowerInstallsSnapshot(
            boolean transferSnapshotFromFollowersEnabled) {
        int entryCount = 50;
        RaftConfig config = RaftConfig.newBuilder().setCommitCountToTakeSnapshot(entryCount)
                .setTransferSnapshotsFromFollowersEnabled(transferSnapshotFromFollowersEnabled).build();
        group = LocalRaftGroup.start(3, config);

        RaftNodeImpl leader = group.waitUntilLeaderElected();

        leader.replicate(applyValue("val0")).join();

        List<RaftNodeImpl> followers = group.getNodesExcept(leader.getLocalEndpoint());
        eventually(() -> {
            for (RaftNodeImpl follower : followers) {
                assertThat(getCommitIndex(follower)).isEqualTo(getCommitIndex(leader));
            }
        });

        RaftNodeImpl slowFollower = followers.get(1);

        group.dropMessagesTo(leader.getLocalEndpoint(), slowFollower.getLocalEndpoint(), AppendEntriesRequest.class);
        group.dropMessagesTo(leader.getLocalEndpoint(), slowFollower.getLocalEndpoint(), InstallSnapshotRequest.class);

        for (int i = 1; i < entryCount; i++) {
            leader.replicate(applyValue("val" + i)).join();
        }

        eventually(() -> assertThat(getSnapshotEntry(leader).getIndex()).isEqualTo(entryCount));

        leader.replicate(applyValue("valFinal")).join();

        group.allowMessagesTo(leader.getLocalEndpoint(), slowFollower.getLocalEndpoint(), AppendEntriesRequest.class);
        group.allowMessagesTo(leader.getLocalEndpoint(), slowFollower.getLocalEndpoint(), InstallSnapshotRequest.class);

        eventually(() -> assertThat(getCommitIndex(slowFollower)).isEqualTo(getCommitIndex(leader)));

        group.resetAllRulesFrom(leader.getLocalEndpoint());

        eventually(() -> {
            for (RaftNodeImpl node : group.getNodes()) {
                assertThat(getCommitIndex(node)).isEqualTo(entryCount + 1);
                SimpleStateMachine stateMachine = group.getStateMachine(node.getLocalEndpoint());
                assertThat(stateMachine.size()).isEqualTo(entryCount + 1);
                for (int i = 0; i < entryCount; i++) {
                    assertThat(stateMachine.get(i + 1)).isEqualTo("val" + i);
                }
                assertThat(stateMachine.get(51)).isEqualTo("valFinal");
            }
        });

        assertThat(getSnapshotChunkCollector(slowFollower)).isNull();
    }

    @Test(timeout = 300_000)
    public void when_leaderDoesNotKnowOthersSnapshot_then_slowFollowerInstallsSnapshotFromLeaderOnlyWithoutOptimization() {
        when_leaderDoesNotKnowOtherFollowerInstalledSnapshot_then_slowFollowerInstallsSnapshotFromLeaderOnly(false);
    }

    @Test(timeout = 300_000)
    public void when_leaderDoesNotKnowOthersSnapshot_then_slowFollowerInstallsSnapshotFromLeaderOnlyWithOptimization() {
        when_leaderDoesNotKnowOtherFollowerInstalledSnapshot_then_slowFollowerInstallsSnapshotFromLeaderOnly(true);
    }

    private void when_leaderDoesNotKnowOtherFollowerInstalledSnapshot_then_slowFollowerInstallsSnapshotFromLeaderOnly(
            boolean transferSnapshotFromFollowersEnabled) {
        int entryCount = 50;
        RaftConfig config = RaftConfig.newBuilder().setCommitCountToTakeSnapshot(entryCount)
                .setTransferSnapshotsFromFollowersEnabled(transferSnapshotFromFollowersEnabled).build();
        group = LocalRaftGroup.start(3, config);

        RaftNodeImpl leader = group.waitUntilLeaderElected();

        List<RaftNodeImpl> followers = group.getNodesExcept(leader.getLocalEndpoint());
        RaftNodeImpl fastFollower = followers.get(0);
        RaftNodeImpl slowFollower = followers.get(1);

        group.dropMessagesTo(leader.getLocalEndpoint(), slowFollower.getLocalEndpoint(), AppendEntriesRequest.class);
        group.dropMessagesTo(leader.getLocalEndpoint(), slowFollower.getLocalEndpoint(), InstallSnapshotRequest.class);

        while (getSnapshotEntry(leader).getIndex() == 0) {
            leader.replicate(applyValue("val")).join();
        }

        eventually(() -> assertThat(getSnapshotEntry(leader).getIndex()).isGreaterThan(0));
        eventually(() -> assertThat(getSnapshotEntry(fastFollower).getIndex()).isGreaterThan(0));

        group.allowMessagesTo(leader.getLocalEndpoint(), slowFollower.getLocalEndpoint(), AppendEntriesRequest.class);
        group.allowMessagesTo(leader.getLocalEndpoint(), slowFollower.getLocalEndpoint(), InstallSnapshotRequest.class);

        eventually(() -> assertThat(getCommitIndex(slowFollower)).isEqualTo(entryCount));

        group.resetAllRulesFrom(leader.getLocalEndpoint());

        eventually(() -> {
            for (RaftNodeImpl node : group.getNodes()) {
                assertThat(getCommitIndex(node)).isEqualTo(getCommitIndex(leader));
                SimpleStateMachine stateMachine = group.getStateMachine(node.getLocalEndpoint());
                assertThat(stateMachine.size()).isEqualTo(entryCount);
            }
        });

        assertThat(getSnapshotChunkCollector(slowFollower)).isNull();
    }

    @Test(timeout = 300_000)
    public void when_leaderMissesResponse_then_itAdvancesMatchIndexWithNextResponse_leaderOnly() {
        when_leaderMissesInstallSnapshotResponse_then_itAdvancesMatchIndexWithNextInstallSnapshotResponse(false);
    }

    @Test(timeout = 300_000)
    public void when_leaderMissesResponse_then_itAdvancesMatchIndexWithNextResponse_leaderAndFollowers() {
        when_leaderMissesInstallSnapshotResponse_then_itAdvancesMatchIndexWithNextInstallSnapshotResponse(true);
    }

    private void when_leaderMissesInstallSnapshotResponse_then_itAdvancesMatchIndexWithNextInstallSnapshotResponse(
            boolean transferSnapshotFromFollowersEnabled) {
        int entryCount = 50;
        RaftConfig config = RaftConfig.newBuilder().setCommitCountToTakeSnapshot(entryCount)
                .setTransferSnapshotsFromFollowersEnabled(transferSnapshotFromFollowersEnabled).build();
        group = LocalRaftGroup.start(3, config);

        RaftNodeImpl leader = group.waitUntilLeaderElected();

        RaftNodeImpl slowFollower = group.getAnyNodeExcept(leader.getLocalEndpoint());

        // the leader cannot send AppendEntriesRPC to the follower
        group.dropMessagesTo(leader.getLocalEndpoint(), slowFollower.getLocalEndpoint(), AppendEntriesRequest.class);

        // the follower cannot send append response to the leader after installing the
        // snapshot
        group.dropMessagesTo(slowFollower.getLocalEndpoint(), leader.getLocalEndpoint(),
                AppendEntriesSuccessResponse.class);

        for (int i = 0; i < entryCount; i++) {
            leader.replicate(applyValue("val" + i)).join();
        }

        eventually(() -> assertThat(getSnapshotEntry(leader).getIndex()).isEqualTo(entryCount));

        leader.replicate(applyValue("valFinal")).join();

        group.resetAllRulesFrom(leader.getLocalEndpoint());

        eventually(() -> {
            for (RaftNodeImpl node : group.<RaftNodeImpl>getNodesExcept(slowFollower.getLocalEndpoint())) {
                assertThat(getCommitIndex(node)).isEqualTo(entryCount + 1);
                SimpleStateMachine stateMachine = group.getStateMachine(node.getLocalEndpoint());
                assertThat(stateMachine.size()).isEqualTo(entryCount + 1);
                for (int i = 0; i < entryCount; i++) {
                    assertThat(stateMachine.get(i + 1)).isEqualTo("val" + i);
                }
                assertThat(stateMachine.get(51)).isEqualTo("valFinal");
            }

            assertThat(getCommitIndex(slowFollower)).isEqualTo(entryCount);
            SimpleStateMachine service = group.getStateMachine(slowFollower.getLocalEndpoint());
            assertThat(service.size()).isEqualTo(entryCount);
            for (int i = 0; i < entryCount; i++) {
                assertThat(service.get(i + 1)).isEqualTo("val" + i);
            }
        });

        group.resetAllRulesFrom(slowFollower.getLocalEndpoint());

        long commitIndex = getCommitIndex(leader);

        eventually(() -> {
            for (RaftNode node : group.getNodesExcept(leader.getLocalEndpoint())) {
                assertThat(getMatchIndex(leader, node.getLocalEndpoint())).isEqualTo(commitIndex);
            }
        });
    }

    @Test(timeout = 300_000)
    public void when_leaderMissesInstallSnapshotResponses_then_followerInstallsSnapshotsViaOtherFollowers() {
        int entryCount = 50;
        RaftConfig config = RaftConfig.newBuilder().setCommitCountToTakeSnapshot(entryCount).build();
        group = LocalRaftGroup.start(3, config);

        RaftNodeImpl leader = group.waitUntilLeaderElected();

        leader.replicate(applyValue("val0")).join();

        RaftNodeImpl slowFollower = group.getAnyNodeExcept(leader.getLocalEndpoint());

        group.dropMessagesTo(leader.getLocalEndpoint(), slowFollower.getLocalEndpoint(), AppendEntriesRequest.class);
        group.dropMessagesTo(leader.getLocalEndpoint(), slowFollower.getLocalEndpoint(), InstallSnapshotRequest.class);
        group.dropMessagesTo(slowFollower.getLocalEndpoint(), leader.getLocalEndpoint(), InstallSnapshotResponse.class);

        for (int i = 1; i < entryCount; i++) {
            leader.replicate(applyValue("val" + i)).join();
        }

        eventually(() -> assertThat(getSnapshotEntry(leader).getIndex()).isEqualTo(entryCount));

        leader.replicate(applyValue("valFinal")).join();

        group.allowMessagesTo(leader.getLocalEndpoint(), slowFollower.getLocalEndpoint(), InstallSnapshotRequest.class);

        eventually(() -> assertThat(getCommitIndex(slowFollower)).isEqualTo(entryCount));

        group.resetAllRulesFrom(leader.getLocalEndpoint());

        eventually(() -> {
            for (RaftNodeImpl node : group.getNodes()) {
                assertThat(getCommitIndex(node)).isEqualTo(entryCount + 1);
                SimpleStateMachine stateMachine = group.getStateMachine(node.getLocalEndpoint());
                assertThat(stateMachine.size()).isEqualTo(entryCount + 1);
                for (int i = 0; i < entryCount; i++) {
                    assertThat(stateMachine.get(i + 1)).isEqualTo("val" + i);
                }
                assertThat(stateMachine.get(51)).isEqualTo("valFinal");
            }
        });

        assertThat(getSnapshotChunkCollector(slowFollower)).isNull();
    }

    @Test(timeout = 300_000)
    public void when_leaderAndSomeFollowersMissInstallSnapshotResponses_then_followerInstallsSnapshotsViaOtherFollowers() {
        int entryCount = 50;
        RaftConfig config = RaftConfig.newBuilder().setCommitCountToTakeSnapshot(entryCount).build();
        group = LocalRaftGroup.start(5, config);

        RaftNodeImpl leader = group.waitUntilLeaderElected();

        leader.replicate(applyValue("val0")).join();

        List<RaftNodeImpl> followers = group.getNodesExcept(leader.getLocalEndpoint());
        RaftNodeImpl slowFollower = followers.get(0);

        group.dropMessagesTo(leader.getLocalEndpoint(), slowFollower.getLocalEndpoint(), AppendEntriesRequest.class);
        group.dropMessagesTo(leader.getLocalEndpoint(), slowFollower.getLocalEndpoint(), InstallSnapshotRequest.class);
        group.dropMessagesTo(slowFollower.getLocalEndpoint(), leader.getLocalEndpoint(), InstallSnapshotResponse.class);
        group.dropMessagesTo(slowFollower.getLocalEndpoint(), followers.get(2).getLocalEndpoint(),
                InstallSnapshotResponse.class);
        group.dropMessagesTo(slowFollower.getLocalEndpoint(), followers.get(3).getLocalEndpoint(),
                InstallSnapshotResponse.class);

        for (int i = 1; i < entryCount; i++) {
            leader.replicate(applyValue("val" + i)).join();
        }

        eventually(() -> assertThat(getSnapshotEntry(leader).getIndex()).isEqualTo(entryCount));

        leader.replicate(applyValue("valFinal")).join();

        group.allowMessagesTo(leader.getLocalEndpoint(), slowFollower.getLocalEndpoint(), InstallSnapshotRequest.class);

        eventually(() -> assertThat(getCommitIndex(slowFollower)).isEqualTo(entryCount));

        group.resetAllRulesFrom(leader.getLocalEndpoint());

        eventually(() -> {
            for (RaftNodeImpl node : group.getNodes()) {
                assertThat(getCommitIndex(node)).isEqualTo(entryCount + 1);
                SimpleStateMachine stateMachine = group.getStateMachine(node.getLocalEndpoint());
                assertThat(stateMachine.size()).isEqualTo(entryCount + 1);
                for (int i = 0; i < entryCount; i++) {
                    assertThat(stateMachine.get(i + 1)).isEqualTo("val" + i);
                }
                assertThat(stateMachine.get(51)).isEqualTo("valFinal");
            }
        });

        assertThat(getSnapshotChunkCollector(slowFollower)).isNull();
    }

    @Test(timeout = 300_000)
    public void when_followerMissesTheLastEntryThatGoesIntoTheSnapshot_then_itCatchesUpWithoutInstallingSnapshot() {
        int entryCount = 50;
        RaftConfig config = RaftConfig.newBuilder().setCommitCountToTakeSnapshot(entryCount).build();
        group = LocalRaftGroup.start(3, config);

        RaftNodeImpl leader = group.waitUntilLeaderElected();

        RaftNodeImpl slowFollower = group.getAnyNodeExcept(leader.getLocalEndpoint());

        for (int i = 0; i < entryCount - 1; i++) {
            leader.replicate(applyValue("val" + i)).join();
        }

        eventually(() -> {
            for (RaftNodeImpl follower : group.<RaftNodeImpl>getNodesExcept(leader.getLocalEndpoint())) {
                assertThat(getMatchIndex(leader, follower.getLocalEndpoint())).isEqualTo(entryCount - 1);
            }
        });

        group.dropMessagesTo(leader.getLocalEndpoint(), slowFollower.getLocalEndpoint(), AppendEntriesRequest.class);
        group.dropMessagesTo(leader.getLocalEndpoint(), slowFollower.getLocalEndpoint(), InstallSnapshotRequest.class);

        leader.replicate(applyValue("val" + (entryCount - 1))).join();

        eventually(() -> assertThat(getSnapshotEntry(leader).getIndex()).isEqualTo(entryCount));

        leader.replicate(applyValue("valFinal")).join();

        group.allowMessagesTo(leader.getLocalEndpoint(), slowFollower.getLocalEndpoint(), AppendEntriesRequest.class);

        eventually(() -> {
            for (RaftNodeImpl node : group.getNodes()) {
                assertThat(getCommitIndex(node)).isEqualTo(entryCount + 1);
                SimpleStateMachine stateMachine = group.getStateMachine(node.getLocalEndpoint());
                assertThat(stateMachine.size()).isEqualTo(entryCount + 1);
                for (int i = 0; i < entryCount; i++) {
                    assertThat(stateMachine.get(i + 1)).isEqualTo("val" + i);
                }
                assertThat(stateMachine.get(51)).isEqualTo("valFinal");
            }
        });
    }

    @Test(timeout = 300_000)
    public void when_followerMissesAFewEntriesBeforeTheSnapshot_then_itCatchesUpWithoutInstallingSnapshot() {
        int entryCount = 50;
        // entryCount * 0.1 - 2
        int missingEntryCountOnSlowFollower = 4;
        RaftConfig config = RaftConfig.newBuilder().setCommitCountToTakeSnapshot(entryCount).build();
        group = LocalRaftGroup.start(3, config);

        RaftNodeImpl leader = group.waitUntilLeaderElected();

        RaftNodeImpl slowFollower = group.getAnyNodeExcept(leader.getLocalEndpoint());

        for (int i = 0; i < entryCount - missingEntryCountOnSlowFollower; i++) {
            leader.replicate(applyValue("val" + i)).join();
        }

        eventually(() -> {
            for (RaftNodeImpl follower : group.<RaftNodeImpl>getNodesExcept(leader.getLocalEndpoint())) {
                assertThat(getMatchIndex(leader, follower.getLocalEndpoint()))
                        .isEqualTo(entryCount - missingEntryCountOnSlowFollower);
            }
        });

        group.dropMessagesTo(leader.getLocalEndpoint(), slowFollower.getLocalEndpoint(), AppendEntriesRequest.class);
        group.dropMessagesTo(leader.getLocalEndpoint(), slowFollower.getLocalEndpoint(), InstallSnapshotRequest.class);

        for (int i = entryCount - missingEntryCountOnSlowFollower; i < entryCount; i++) {
            leader.replicate(applyValue("val" + i)).join();
        }

        eventually(() -> assertThat(getSnapshotEntry(leader).getIndex()).isEqualTo(entryCount));

        leader.replicate(applyValue("valFinal")).join();

        group.allowMessagesTo(leader.getLocalEndpoint(), slowFollower.getLocalEndpoint(), AppendEntriesRequest.class);

        eventually(() -> {
            for (RaftNodeImpl node : group.getNodes()) {
                assertThat(getCommitIndex(node)).isEqualTo(entryCount + 1);
                SimpleStateMachine stateMachine = group.getStateMachine(node.getLocalEndpoint());
                assertThat(stateMachine.size()).isEqualTo(entryCount + 1);
                for (int i = 0; i < entryCount; i++) {
                    assertThat(stateMachine.get(i + 1)).isEqualTo("val" + i);
                }
                assertThat(stateMachine.get(51)).isEqualTo("valFinal");
            }
        });
    }

    @Test(timeout = 300_000)
    public void when_prevLogEntryIsTruncatedByFollowersSnapshot_then_followerCatchesUpWithoutLivelock() {
        // The new leader snapshots at 100 and keeps the entries from 95, since the
        // follower's match index is 95. The follower snapshots at 105 and keeps only
        // the last 10 entries, 96-105.
        int commitCountToTakeSnapshot = 100;
        long lastOldTermIndex = 95;
        long followerSnapshotIndex = 105;
        RaftConfig config = RaftConfig.newBuilder().setCommitCountToTakeSnapshot(commitCountToTakeSnapshot).build();
        group = LocalRaftGroup.start(3, config);

        RaftNodeImpl oldLeader = group.waitUntilLeaderElected();
        List<RaftNodeImpl> followers = group.getNodesExcept(oldLeader.getLocalEndpoint());
        RaftNodeImpl newLeader = followers.get(0);
        RaftNodeImpl follower = followers.get(1);

        // entries 1-95 are in the old leader's term
        for (int i = 0; i < lastOldTermIndex; i++) {
            oldLeader.replicate(applyValue("val" + i)).join();
        }

        oldLeader.transferLeadership(newLeader.getLocalEndpoint()).join();

        // the new leader learns that the follower has everything up to 95, so its
        // next index for the follower is 96
        eventually(() -> {
            assertThat(newLeader.getLeaderEndpoint()).isEqualTo(newLeader.getLocalEndpoint());
            assertThat(getMatchIndex(newLeader, follower.getLocalEndpoint())).isEqualTo(lastOldTermIndex);
        });

        // the follower's acks are lost from now on, so the new leader's match index
        // for it stays at 95
        group.dropMessagesTo(follower.getLocalEndpoint(), newLeader.getLocalEndpoint(),
                AppendEntriesSuccessResponse.class);
        // the follower misses entries 96-105 while the new leader commits them
        group.dropMessagesTo(newLeader.getLocalEndpoint(), follower.getLocalEndpoint(), AppendEntriesRequest.class);

        // entries 96-105 are in the new term, and the new leader snapshots at 100
        // keeping entry 95
        for (long i = lastOldTermIndex; i < followerSnapshotIndex; i++) {
            newLeader.replicate(applyValue("val" + i)).join();
        }

        assertThat(getSnapshotEntry(newLeader).getIndex()).isEqualTo(commitCountToTakeSnapshot);

        // the follower receives entries 96-105 and learns that they are committed
        group.allowMessagesTo(newLeader.getLocalEndpoint(), follower.getLocalEndpoint(), AppendEntriesRequest.class);
        eventually(() -> assertThat(getCommitIndex(follower)).isEqualTo(followerSnapshotIndex));

        // MicroRaft takes snapshots only at multiples of commitCountToTakeSnapshot,
        // so the follower takes its snapshot at 105 manually. It truncates entry 95,
        // while its ack for 105 is lost and the leader keeps sending appends with the
        // previous entry at 95.
        follower.takeSnapshot().join();
        assertThat(getSnapshotEntry(follower).getIndex()).isEqualTo(followerSnapshotIndex);

        group.allowMessagesTo(follower.getLocalEndpoint(), newLeader.getLocalEndpoint(),
                AppendEntriesSuccessResponse.class);

        // the previous entry at 95 is committed on the follower and inside its own
        // snapshot, so the follower must not reject the leader's appends, and it
        // catches up with the new entry
        newLeader.replicate(applyValue("val" + followerSnapshotIndex)).join();

            // the follower accepts the leader's appends again, so the leader receives its
            // ack for the new entry, and the follower learns that the entry is committed
        eventually(() -> {
            assertThat(getCommitIndex(follower)).isEqualTo(followerSnapshotIndex + 1);
            assertThat(getMatchIndex(newLeader, follower.getLocalEndpoint())).isEqualTo(followerSnapshotIndex + 1);
        });
    }

    @Test(timeout = 300_000)
    public void when_isolatedLeaderAppendsEntries_then_itInvalidatesTheirFeaturesUponInstallSnapshot()
            throws Exception {
        int entryCount = 50;
        RaftConfig config = RaftConfig.newBuilder().setLeaderHeartbeatPeriodSecs(1).setLeaderHeartbeatTimeoutSecs(5)
                .setCommitCountToTakeSnapshot(entryCount).build();
        group = LocalRaftGroup.start(3, config);

        RaftNodeImpl leader = group.waitUntilLeaderElected();
        List<RaftNodeImpl> followers = group.getNodesExcept(leader.getLocalEndpoint());

        for (int i = 0; i < 40; i++) {
            leader.replicate(applyValue("val" + i)).join();
        }

        eventually(() -> {
            for (RaftNodeImpl node : group.getNodes()) {
                assertThat(getCommitIndex(node)).isEqualTo(40);
            }
        });

        group.splitMembers(leader.getLocalEndpoint());

        List<Future<Ordered<Object>>> futures = new ArrayList<>();
        for (int i = 40; i < 45; i++) {
            Future<Ordered<Object>> f = leader.replicate(applyValue("isolated" + i));
            futures.add(f);
        }

        eventually(() -> {
            for (RaftNodeImpl raftNode : followers) {
                assertThat(raftNode.getLeaderEndpoint()).isNotNull().isNotEqualTo(leader.getLocalEndpoint());
            }
        });

        RaftNodeImpl newLeader = group.getNode(followers.get(0).getLeaderEndpoint());

        for (int i = 40; i < 51; i++) {
            newLeader.replicate(applyValue("val" + i)).join();
        }

        eventually(() -> {
            for (RaftNodeImpl node : followers) {
                assertThat(getSnapshotEntry(node).getIndex()).isGreaterThan(0);
            }
        });

        group.dropMessagesTo(leader.getLocalEndpoint(), followers.get(0).getLocalEndpoint(),
                AppendEntriesRequest.class);
        group.dropMessagesTo(leader.getLocalEndpoint(), followers.get(1).getLocalEndpoint(),
                AppendEntriesRequest.class);
        group.merge();

        for (Future<Ordered<Object>> f : futures) {
            try {
                f.get();
                fail();
            } catch (ExecutionException e) {
                assertThat(e).hasCauseInstanceOf(IndeterminateStateException.class);
            }
        }

        eventually(() -> {
            for (RaftNodeImpl node : group.getNodes()) {
                assertThat(getCommitIndex(node)).isEqualTo(51);
                SimpleStateMachine stateMachine = group.getStateMachine(node.getLocalEndpoint());
                assertThat(stateMachine.size()).isEqualTo(51);
                for (int i = 0; i < 51; i++) {
                    assertThat(stateMachine.get(i + 1)).isEqualTo("val" + i);
                }
            }
        });
    }

    @Test(timeout = 300_000)
    public void when_followerReceivesSnapshotMatchingItsLog_then_itFastForwardsCommitIndexWithoutInstallingSnapshot() {
        // a snapshot is taken every 10 commits, and the leader keeps the entries before it only for followers whose
        // match index is above 9, so a follower whose ack for entry 10 is lost is sent the snapshot
        int commitCountToTakeSnapshot = 10;
        RaftConfig config = RaftConfig.newBuilder().setCommitCountToTakeSnapshot(commitCountToTakeSnapshot).build();
        group = LocalRaftGroup.start(3, config);

        RaftNodeImpl leader = group.waitUntilLeaderElected();
        RaftNodeImpl follower = group.getAnyNodeExcept(leader.getLocalEndpoint());

        for (int i = 0; i < commitCountToTakeSnapshot - 1; i++) {
            leader.replicate(applyValue("val" + i)).join();
        }

        eventually(() -> assertThat(getCommitIndex(follower)).isEqualTo(commitCountToTakeSnapshot - 1));

        // the query waits until the follower applies the entries up to the snapshot's index
        long snapshotIndex = commitCountToTakeSnapshot;
        CompletableFuture<Ordered<Object>> query = follower.query(queryLastValue(), EVENTUAL_CONSISTENCY,
                Optional.of(snapshotIndex), Optional.of(Duration.ofSeconds(60)));

        // The follower receives entry 10, but its ack is lost. The leader commits 10 with the other follower
        // and snapshots it, truncating its log up to 10. So the next message to the follower is the snapshot,
        // while the follower still has entry 10 uncommitted in its log.
        group.dropMessagesTo(follower.getLocalEndpoint(), leader.getLocalEndpoint(),
                AppendEntriesSuccessResponse.class);
        leader.replicate(applyValue("val" + (commitCountToTakeSnapshot - 1))).join();

        eventually(() -> assertThat(getSnapshotEntry(leader).getIndex()).isEqualTo(snapshotIndex));

        // The follower already has all the snapshot's entries, so it commits up to 10 and applies them from its
        // own log, without transferring or installing the snapshot. The fast-forward also runs the query, while
        // the follower's acks are still lost, so no other commit can run it.
        eventually(() -> {
            assertThat(getCommitIndex(follower)).isEqualTo(snapshotIndex);
            assertThat(getLastApplied(follower)).isEqualTo(snapshotIndex);
            assertThat(query).isCompleted();
        });
        assertThat(getInstallSnapshotCount(follower)).isZero();
    }

    @Test(timeout = 300_000)
    public void when_followerReceivesSnapshotConflictingWithItsLog_then_itDiscardsItsEntriesAfterSnapshot() {
        // a snapshot is taken every 10 commits, and a new leader keeps only its snapshot's last entry (20)
        // for a follower whose progress it does not know, so it must send its snapshot to the old leader
        RaftConfig config = RaftConfig.newBuilder().setCommitCountToTakeSnapshot(10).build();
        group = LocalRaftGroup.start(3, config);

        RaftNodeImpl leader = group.waitUntilLeaderElected();
        RaftNodeImpl follower = group.getAnyNodeExcept(leader.getLocalEndpoint());

        for (int i = 0; i < 9; i++) {
            leader.replicate(applyValue("val" + i)).join();
        }

        group.splitMembers(leader.getLocalEndpoint());

        // the isolated leader appends entries 10-21 in its own term, which can never be committed
        long snapshotIndex = 20;
        for (long i = 10; i <= snapshotIndex + 1; i++) {
            leader.replicate(applyValue("isolated" + i));
        }

        eventually(() -> assertThat(getLastLogOrSnapshotEntry(leader).getIndex()).isEqualTo(snapshotIndex + 1));

        // the followers elect a new leader
        eventually(() -> assertThat(follower.getLeaderEndpoint()).isNotNull()
                .isNotEqualTo(leader.getLocalEndpoint()));

        // the new leader commits its own entries 10-20 and takes a snapshot at 20
        RaftNodeImpl newLeader = group.getNode(follower.getLeaderEndpoint());
        for (long i = 10; i <= snapshotIndex; i++) {
            newLeader.replicate(applyValue("val" + (i - 1))).join();
        }

        eventually(() -> assertThat(getSnapshotEntry(newLeader).getIndex()).isEqualTo(snapshotIndex));

        // the old leader's entry at 20 is from its own term, so it conflicts with the new leader's snapshot
        assertThat(readRaftState(leader, () -> leader.state().log().getLogEntry(snapshotIndex).getTerm()))
                .isLessThan(getSnapshotEntry(newLeader).getTerm());

        group.merge();

        // After merging, the old leader discards its uncommitted entries 10-21 and installs the snapshot
        eventually(() -> assertThat(getSnapshotEntry(leader).getIndex()).isEqualTo(snapshotIndex));

        assertThat(getLastLogOrSnapshotEntry(leader).getIndex()).isEqualTo(snapshotIndex);
        assertThat(getInstallSnapshotCount(leader)).isEqualTo(1);
    }

    @Test(timeout = 300_000)
    public void when_followerReceivesSnapshotConflictingWithItsLog_then_itRevertsPreparedRaftGroupOpAfterSnapshot() {
        // Raft group operations are prepared when they are appended, before they are committed, so discarding
        // them must also revert them. The only Raft group operation is a membership change, and its revert is
        // visible in the group members.

        // a snapshot is taken every 10 commits, and a new leader keeps only its snapshot's last entry (20)
        // for a follower whose progress it does not know, so it must send its snapshot to the old leader
        RaftConfig config = RaftConfig.newBuilder().setCommitCountToTakeSnapshot(10).build();
        group = LocalRaftGroup.start(3, config);

        RaftNodeImpl leader = group.waitUntilLeaderElected();
        RaftNodeImpl follower = group.getAnyNodeExcept(leader.getLocalEndpoint());

        for (int i = 0; i < 9; i++) {
            leader.replicate(applyValue("val" + i)).join();
        }

        group.splitMembers(leader.getLocalEndpoint());

        // the isolated leader appends entries 10-20 and a membership change at 21 in its own term, which can never
        // be committed, and prepares the membership change
        long snapshotIndex = 20;
        for (long i = 10; i <= snapshotIndex; i++) {
            leader.replicate(applyValue("isolated" + i));
        }
        leader.changeMembership(follower.getLocalEndpoint(), REMOVE_MEMBER, 0);

        eventually(() -> {
            assertThat(getEffectiveGroupMembers(leader).getLogIndex()).isEqualTo(snapshotIndex + 1);
            assertThat(getStatus(leader)).isEqualTo(UPDATING_RAFT_GROUP_MEMBER_LIST);
        });

        // the followers elect a new leader
        eventually(() -> assertThat(follower.getLeaderEndpoint()).isNotNull()
                .isNotEqualTo(leader.getLocalEndpoint()));

        // the new leader commits its own entries 10-20 and takes a snapshot at 20, which conflicts with the old
        // leader's entries at 20 and 21 from its own term
        RaftNodeImpl newLeader = group.getNode(follower.getLeaderEndpoint());
        for (long i = 10; i <= snapshotIndex; i++) {
            newLeader.replicate(applyValue("val" + (i - 1))).join();
        }

        eventually(() -> assertThat(getSnapshotEntry(newLeader).getIndex()).isEqualTo(snapshotIndex));

        group.merge();

        // After merging, the old leader discards the membership change at 21 and reverts it, so its group members
        // are the snapshot's. Without the revert, the assertion in RaftState.installGroupMembers fails, so the group
        // members are not installed and keep the prepared membership change's log index.
        eventually(() -> assertThat(getSnapshotEntry(leader).getIndex()).isEqualTo(snapshotIndex));

        assertThat(getEffectiveGroupMembers(leader).getLogIndex())
                .isEqualTo(getCommittedGroupMembers(newLeader).getLogIndex());
        assertThat(getEffectiveGroupMembers(leader).getMembers())
                .isEqualTo(getCommittedGroupMembers(newLeader).getMembers());
    }

    @Test(timeout = 300_000)
    public void when_followerReceivesSnapshotConflictingWithItsLog_then_itRevertsPreparedRaftGroupOpAtSnapshotIndex() {
        // Unlike
        // when_followerReceivesSnapshotConflictingWithItsLog_then_itRevertsPreparedRaftGroupOpAfterSnapshot,
        // the old leader's log ends at the snapshot's index, so there are no entries after it to discard,
        // and the conflicting membership change is replaced by the snapshot itself.

        // a snapshot is taken every 10 commits, and a new leader keeps only its snapshot's last entry (20)
        // for a follower whose progress it does not know, so it must send its snapshot to the old leader
        RaftConfig config = RaftConfig.newBuilder().setCommitCountToTakeSnapshot(10).build();
        group = LocalRaftGroup.start(3, config);

        RaftNodeImpl leader = group.waitUntilLeaderElected();
        RaftNodeImpl follower = group.getAnyNodeExcept(leader.getLocalEndpoint());

        for (int i = 0; i < 9; i++) {
            leader.replicate(applyValue("val" + i)).join();
        }

        group.splitMembers(leader.getLocalEndpoint());

        // the isolated leader appends entries 10-19 and a membership change at 20 in its own term, which can never
        // be committed, and prepares the membership change
        long snapshotIndex = 20;
        for (long i = 10; i < snapshotIndex; i++) {
            leader.replicate(applyValue("isolated" + i));
        }
        leader.changeMembership(follower.getLocalEndpoint(), REMOVE_MEMBER, 0);

        eventually(() -> {
            assertThat(getEffectiveGroupMembers(leader).getLogIndex()).isEqualTo(snapshotIndex);
            assertThat(getStatus(leader)).isEqualTo(UPDATING_RAFT_GROUP_MEMBER_LIST);
        });

        // the followers elect a new leader
        eventually(() -> assertThat(follower.getLeaderEndpoint()).isNotNull()
                .isNotEqualTo(leader.getLocalEndpoint()));

        // the new leader commits its own entries 10-20 and takes a snapshot at 20, which conflicts with the old
        // leader's membership change at 20 from its own term
        RaftNodeImpl newLeader = group.getNode(follower.getLeaderEndpoint());
        for (long i = 10; i <= snapshotIndex; i++) {
            newLeader.replicate(applyValue("val" + (i - 1))).join();
        }

        eventually(() -> assertThat(getSnapshotEntry(newLeader).getIndex()).isEqualTo(snapshotIndex));

        group.merge();

        // After merging, the old leader discards the membership change at 20 and reverts it, so its group members
        // are the snapshot's. Without the revert, the assertion in RaftState.installGroupMembers fails, so the group
        // members are not installed and keep the prepared membership change's log index.
        eventually(() -> assertThat(getSnapshotEntry(leader).getIndex()).isEqualTo(snapshotIndex));

        assertThat(getEffectiveGroupMembers(leader).getLogIndex())
                .isEqualTo(getCommittedGroupMembers(newLeader).getLogIndex());
        assertThat(getEffectiveGroupMembers(leader).getMembers())
                .isEqualTo(getCommittedGroupMembers(newLeader).getMembers());
    }

    @Test(timeout = 300_000)
    public void when_followersLastAppendIsMembershipChange_then_itUpdatesRaftNodeStateWithInstalledSnapshot() {
        int entryCount = 50;
        RaftConfig config = RaftConfig.newBuilder().setCommitCountToTakeSnapshot(entryCount).build();
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

        RaftNodeImpl newRaftNode1 = group.createNewNode();
        CompletableFuture<Ordered<RaftGroupMembers>> f1 = leader.changeMembership(newRaftNode1.getLocalEndpoint(),
                ADD_LEARNER, 0);

        eventually(() -> {
            for (RaftNodeImpl follower : followers) {
                assertThat(getLastLogOrSnapshotEntry(follower).getIndex()).isEqualTo(2);
            }
        });

        group.dropMessagesTo(leader.getLocalEndpoint(), slowFollower.getLocalEndpoint(), AppendEntriesRequest.class);
        group.dropMessagesTo(leader.getLocalEndpoint(), slowFollower.getLocalEndpoint(), InstallSnapshotRequest.class);

        for (RaftNodeImpl follower : followers) {
            if (follower != slowFollower) {
                group.allowAllMessagesTo(follower.getLocalEndpoint(), leader.getLeaderEndpoint());
            }
        }

        f1.join();

        for (int i = 0; i < entryCount; i++) {
            leader.replicate(applyValue("val" + i)).join();
        }

        eventually(() -> assertThat(getSnapshotEntry(leader).getIndex()).isGreaterThanOrEqualTo(entryCount));

        group.allowAllMessagesTo(leader.getLeaderEndpoint(), slowFollower.getLocalEndpoint());

        eventually(() -> assertThat(getSnapshotEntry(slowFollower).getIndex()).isGreaterThanOrEqualTo(entryCount));

        eventually(() -> {
            assertThat(getCommittedGroupMembers(slowFollower).getLogIndex())
                    .isEqualTo(getCommittedGroupMembers(leader).getLogIndex());
            assertThat(getStatus(slowFollower)).isEqualTo(ACTIVE);
        });
    }

    @Test(timeout = 300_000)
    public void testMembershipChangeBlocksSnapshotBug() {
        // The comments below show how the code behaves before the mentioned bug is
        // fixed.

        int commitCountToTakeSnapshot = 50;
        int pendingEntryCount = 10;
        RaftConfig config = RaftConfig.newBuilder().setCommitCountToTakeSnapshot(commitCountToTakeSnapshot)
                .setMaxPendingLogEntryCount(pendingEntryCount).build();
        group = LocalRaftGroup.start(3, config);

        RaftNodeImpl leader = group.waitUntilLeaderElected();
        List<RaftNodeImpl> followers = group.getNodesExcept(leader.getLocalEndpoint());

        group.dropMessagesTo(leader.getLocalEndpoint(), followers.get(0).getLocalEndpoint(),
                AppendEntriesRequest.class);

        while (getSnapshotEntry(leader).getIndex() == 0) {
            leader.replicate(applyValue("into_snapshot")).join();
        }

        // now, the leader has taken a snapshot.
        // It also keeps some already committed entries in the log because followers[0]
        // hasn't appended them.
        // LOG: [ <46 - 49>, <50>], SNAPSHOT INDEX: 50, COMMIT INDEX: 50

        long leaderCommitIndex = getCommitIndex(leader);
        do {
            leader.replicate(applyValue("committed_after_snapshot")).join();
        } while (getCommitIndex(leader) < leaderCommitIndex + commitCountToTakeSnapshot - 1);

        // committing new entries.
        // one more entry is needed to take the next snapshot.
        // LOG: [ <46 - 49>, <50>, <51 - 99 (committed)> ],
        // SNAPSHOT INDEX: 50, COMMIT INDEX: 99

        group.dropMessagesTo(leader.getLocalEndpoint(), followers.get(1).getLocalEndpoint(),
                AppendEntriesRequest.class);

        for (int i = 0; i < pendingEntryCount - 1; i++) {
            leader.replicate(applyValue("uncommitted_after_snapshot"));
        }

        // appended some more entries which will not be committed because the leader has
        // no majority.
        // the last uncommitted index is reserved for membership change.
        // LOG: [ <46 - 49>, <50>, <51 - 99 (committed)>, <100 - 108 (uncommitted)> ],
        // SNAPSHOT INDEX: 50, COMMIT INDEX: 99
        // There are only 2 empty indices in the log.

        RaftNodeImpl newRaftNode = group.createNewNode();

        Function<RaftMessage, RaftMessage> alterFunc = message -> {
            if (message instanceof AppendEntriesRequest) {
                AppendEntriesRequest request = (AppendEntriesRequest) message;
                List<LogEntry> entries = request.getLogEntries();
                if (entries.size() > 0) {
                    if (entries.get(entries.size() - 1)
                            .getOperation() instanceof DefaultUpdateRaftGroupMembersOpOrBuilder) {
                        entries = entries.subList(0, entries.size() - 1);
                        return new DefaultAppendEntriesRequestOrBuilder().setSender(request.getSender())
                                .setTerm(request.getTerm()).setPreviousLogTerm(request.getPreviousLogTerm())
                                .setPreviousLogIndex(request.getPreviousLogIndex())
                                .setCommitIndex(request.getCommitIndex()).setLogEntries(entries)
                                .setQuerySequenceNumber(request.getQuerySequenceNumber())
                                .setFlowControlSequenceNumber(request.getFlowControlSequenceNumber()).build();
                    } else if (entries.get(0).getOperation() instanceof DefaultUpdateRaftGroupMembersOpOrBuilder) {
                        entries = emptyList();
                        return new DefaultAppendEntriesRequestOrBuilder().setSender(request.getSender())
                                .setTerm(request.getTerm()).setPreviousLogTerm(request.getPreviousLogTerm())
                                .setPreviousLogIndex(request.getPreviousLogIndex())
                                .setCommitIndex(request.getCommitIndex()).setLogEntries(entries)
                                .setQuerySequenceNumber(request.getQuerySequenceNumber())
                                .setFlowControlSequenceNumber(request.getFlowControlSequenceNumber()).build();
                    }
                }
            }

            return message;
        };

        group.alterMessagesTo(leader.getLocalEndpoint(), followers.get(1).getLocalEndpoint(), alterFunc);
        group.alterMessagesTo(leader.getLocalEndpoint(), newRaftNode.getLocalEndpoint(), alterFunc);

        long lastLogIndex1 = getLastLogOrSnapshotEntry(leader).getIndex();

        leader.changeMembership(newRaftNode.getLocalEndpoint(), ADD_LEARNER, 0);

        // When the membership change entry is appended, the leader's log will be as
        // following:
        // LOG: [ <46 - 49>, <50>, <51 - 99 (committed)>, <100 - 108 (uncommitted)>,
        // <109 (membership change)> ],
        // SNAPSHOT INDEX: 50, COMMIT INDEX: 99

        eventually(() -> assertThat(getLastLogOrSnapshotEntry(leader).getIndex()).isGreaterThan(lastLogIndex1));

        group.allowMessagesTo(leader.getLocalEndpoint(), followers.get(1).getLocalEndpoint(),
                AppendEntriesRequest.class);

        // Then, only the entries before the membership change will be committed because
        // we alter the append request.
        // The log will be:
        // LOG: [ <46 - 49>, <50>, <51 - 108 (committed)>, <109 (membership change)> ],
        // SNAPSHOT INDEX: 50, COMMIT INDEX: 108
        // There is only 1 empty index in the log.

        eventually(() -> {
            assertThat(getCommitIndex(leader)).isEqualTo(lastLogIndex1);
            assertThat(getCommitIndex(followers.get(1))).isEqualTo(lastLogIndex1);
        });

        // eventually(() -> {
        // assertThat(getCommitIndex(leader)).isEqualTo(lastLogIndex1 + 1);
        // assertThat(getCommitIndex(followers[1])).isEqualTo(lastLogIndex1 + 1);
        // });

        long lastLogIndex2 = getLastLogOrSnapshotEntry(leader).getIndex();

        leader.replicate(applyValue("after_membership_change_append"));

        eventually(() -> assertThat(getLastLogOrSnapshotEntry(leader).getIndex()).isGreaterThan(lastLogIndex2));

        // Now the log is full. There is no empty space left.
        // LOG: [ <46 - 49>, <50>, <51 - 108 (committed)>, <109 (membership change)>,
        // <110 (uncommitted)> ],
        // SNAPSHOT INDEX: 50, COMMIT INDEX: 108

        long lastLogIndex3 = getLastLogOrSnapshotEntry(leader).getIndex();

        Future<Ordered<Object>> f = leader.replicate(applyValue("after_membership_change_append"));

        eventually(() -> assertThat(getLastLogOrSnapshotEntry(leader).getIndex()).isGreaterThan(lastLogIndex3));

        assertThat(f).isNotDone();
    }

    @Test(timeout = 300_000)
    public void when_slowFollowerReceivesAppendRequestThatDoesNotFitIntoItsRaftLog_then_itTruncatesAppendRequestEntries() {
        int appendEntriesRequestBatchSize = 100;
        int commitCountToTakeSnapshot = 100;
        int pendingEntryCount = 10;

        RaftConfig config = RaftConfig.newBuilder().setAppendEntriesRequestBatchSize(appendEntriesRequestBatchSize)
                .setCommitCountToTakeSnapshot(commitCountToTakeSnapshot).setMaxPendingLogEntryCount(pendingEntryCount)
                .build();
        group = LocalRaftGroup.start(5, config);

        RaftNodeImpl leader = group.waitUntilLeaderElected();
        List<RaftNodeImpl> followers = group.getNodesExcept(leader.getLocalEndpoint());
        RaftNodeImpl slowFollower1 = followers.get(0);
        RaftNodeImpl slowFollower2 = followers.get(1);

        int count = 1;
        for (int i = 0; i < commitCountToTakeSnapshot; i++) {
            leader.replicate(applyValue("val" + (count++))).join();
        }

        eventually(() -> {
            for (RaftNodeImpl node : group.getNodes()) {
                assertThat(getSnapshotEntry(node).getIndex()).isGreaterThan(0);
            }
        });

        group.dropMessagesTo(leader.getLocalEndpoint(), slowFollower1.getLocalEndpoint(), AppendEntriesRequest.class);

        for (int i = 0; i < commitCountToTakeSnapshot - 1; i++) {
            leader.replicate(applyValue("val" + (count++))).join();
        }

        eventually(() -> assertThat(getCommitIndex(slowFollower2)).isEqualTo(getCommitIndex(leader)));

        // slowFollower2's log: [ <91 - 100 before snapshot>, <100 snapshot>, <101 - 199
        // committed> ]

        group.dropMessagesTo(leader.getLocalEndpoint(), slowFollower2.getLocalEndpoint(), AppendEntriesRequest.class);

        for (int i = 0; i < commitCountToTakeSnapshot / 2; i++) {
            leader.replicate(applyValue("val" + (count++))).join();
        }

        eventually(() -> assertThat(getSnapshotEntry(leader).getIndex()).isGreaterThan(commitCountToTakeSnapshot));

        // leader's log: [ <191 - 199 before snapshot>, <200 snapshot>, <201 - 249
        // committed> ]

        group.allowMessagesTo(leader.getLocalEndpoint(), slowFollower2.getLocalEndpoint(), AppendEntriesRequest.class);

        // leader replicates 50 entries to slowFollower2 but slowFollower2 has only
        // available capacity of 11 indices.
        // so, slowFollower2 appends 11 of these 50 entries in the first AppendRequest,
        // takes a snapshot,
        // and receives another AppendRequest for the remaining entries...

        eventually(() -> {
            assertThat(getCommitIndex(slowFollower2)).isEqualTo(getCommitIndex(leader));
            assertThat(getSnapshotEntry(slowFollower2).getIndex()).isGreaterThan(commitCountToTakeSnapshot);
        });
    }

    @Test(timeout = 300_000)
    public void when_leaderFailsDuringSnapshotTransfer_then_followerTransfersSnapshotFromNewLeader_leaderOnly() {
        when_leaderFailsDuringSnapshotTransfer_then_followerTransfersSnapshotFromNewLeader(false);
    }

    @Test(timeout = 300_000)
    public void when_leaderFailsDuringSnapshotTransfer_then_followerTransfersSnapshotFromNewLeader_leaderAndFollowers() {
        when_leaderFailsDuringSnapshotTransfer_then_followerTransfersSnapshotFromNewLeader(true);
    }

    private void when_leaderFailsDuringSnapshotTransfer_then_followerTransfersSnapshotFromNewLeader(
            boolean transferSnapshotFromFollowersEnabled) {
        int entryCount = 50;
        RaftConfig config = RaftConfig.newBuilder().setLeaderHeartbeatPeriodSecs(1).setLeaderHeartbeatTimeoutSecs(5)
                .setCommitCountToTakeSnapshot(entryCount)
                .setTransferSnapshotsFromFollowersEnabled(transferSnapshotFromFollowersEnabled).build();
        group = LocalRaftGroup.start(3, config);

        RaftNodeImpl leader = group.waitUntilLeaderElected();
        List<RaftNodeImpl> followers = group.getNodesExcept(leader.getLocalEndpoint());
        RaftNodeImpl slowFollower = followers.get(0);
        RaftNodeImpl newLeader = followers.get(1);

        group.dropMessagesTo(leader.getLocalEndpoint(), slowFollower.getLocalEndpoint(), AppendEntriesRequest.class);
        group.dropMessagesTo(leader.getLocalEndpoint(), slowFollower.getLocalEndpoint(), InstallSnapshotRequest.class);

        while (getSnapshotEntry(leader).getIndex() == 0) {
            leader.replicate(applyValue("val")).join();
        }

        eventually(() -> assertThat(getSnapshotEntry(newLeader).getIndex()).isGreaterThan(0));

        group.dropMessagesTo(slowFollower.getLocalEndpoint(), leader.getLocalEndpoint(), InstallSnapshotResponse.class);
        group.dropMessagesTo(slowFollower.getLocalEndpoint(), newLeader.getLocalEndpoint(),
                InstallSnapshotResponse.class);

        group.allowMessagesTo(leader.getLocalEndpoint(), slowFollower.getLocalEndpoint(), InstallSnapshotRequest.class);

        eventually(() -> assertThat(getSnapshotChunkCollector(slowFollower)).isNotNull());

        group.terminateNode(leader.getLocalEndpoint());

        eventually(() -> assertThat(newLeader.getLeaderEndpoint()).isEqualTo(newLeader.getLocalEndpoint()));

        group.allowMessagesTo(slowFollower.getLocalEndpoint(), newLeader.getLocalEndpoint(),
                InstallSnapshotResponse.class);

        eventually(() -> assertThat(getSnapshotEntry(slowFollower).getIndex()).isGreaterThan(0));
    }

    @Test(timeout = 300_000)
    public void when_thereAreCrashedFollowers_then_theyAreSkippedDuringSnapshotTransfer() {
        int entryCount = 50;
        RaftConfig config = RaftConfig.newBuilder().setCommitCountToTakeSnapshot(entryCount).build();
        group = LocalRaftGroup.start(3, config);

        RaftNodeImpl leader = group.waitUntilLeaderElected();

        List<RaftNodeImpl> followers = group.getNodesExcept(leader.getLocalEndpoint());
        RaftNodeImpl slowFollower = followers.get(0);
        RaftNodeImpl otherFollower = followers.get(1);

        group.dropMessagesTo(leader.getLocalEndpoint(), slowFollower.getLocalEndpoint(), AppendEntriesRequest.class);
        group.dropMessagesTo(leader.getLocalEndpoint(), slowFollower.getLocalEndpoint(), InstallSnapshotRequest.class);
        group.dropMessagesTo(slowFollower.getLocalEndpoint(), leader.getLocalEndpoint(), InstallSnapshotResponse.class);

        for (int i = 0; i < entryCount; i++) {
            leader.replicate(applyValue("val" + i)).join();
        }

        eventually(() -> assertThat(getSnapshotEntry(leader).getIndex()).isEqualTo(entryCount));

        leader.replicate(applyValue("valFinal")).join();

        group.terminateNode(otherFollower.getLocalEndpoint());

        group.allowMessagesTo(leader.getLocalEndpoint(), slowFollower.getLocalEndpoint(), AppendEntriesRequest.class);
        group.allowMessagesTo(leader.getLocalEndpoint(), slowFollower.getLocalEndpoint(), InstallSnapshotRequest.class);

        eventually(() -> {
            SnapshotChunkCollector snapshotChunkCollector = getSnapshotChunkCollector(slowFollower);
            assertThat(snapshotChunkCollector).isNotNull();
            assertThat(snapshotChunkCollector.getSnapshottedMembers()).hasSize(1);
        });

        group.allowMessagesTo(slowFollower.getLocalEndpoint(), leader.getLocalEndpoint(),
                InstallSnapshotResponse.class);

        eventually(() -> assertThat(getCommitIndex(slowFollower)).isEqualTo(getCommitIndex(leader)));
        assertThat(getSnapshotChunkCollector(slowFollower)).isNull();
    }

    @Test(timeout = 300_000)
    public void when_leaderCrashesDuringSnapshotTransfer_then_newLeaderSendsItsSnapshottedMembers() {
        int entryCount = 50;
        RaftConfig config = RaftConfig.newBuilder().setCommitCountToTakeSnapshot(entryCount)
                .setLeaderHeartbeatPeriodSecs(1).setLeaderHeartbeatTimeoutSecs(5).build();
        group = LocalRaftGroup.start(5, config);

        RaftNodeImpl leader = group.waitUntilLeaderElected();

        List<RaftNodeImpl> followers = group.getNodesExcept(leader.getLocalEndpoint());
        RaftNodeImpl slowFollower = followers.get(0);

        for (RaftNodeImpl follower : followers) {
            if (follower != slowFollower) {
                group.dropMessagesTo(follower.getLocalEndpoint(), slowFollower.getLocalEndpoint(),
                        InstallSnapshotRequest.class);
                group.dropMessagesTo(slowFollower.getLocalEndpoint(), follower.getLocalEndpoint(),
                        InstallSnapshotResponse.class);
            }
        }

        group.dropMessagesTo(leader.getLocalEndpoint(), slowFollower.getLocalEndpoint(), AppendEntriesRequest.class);
        group.dropMessagesTo(leader.getLocalEndpoint(), slowFollower.getLocalEndpoint(), InstallSnapshotRequest.class);
        group.dropMessagesTo(slowFollower.getLocalEndpoint(), leader.getLocalEndpoint(), InstallSnapshotResponse.class);

        while (getSnapshotEntry(leader).getIndex() == 0) {
            leader.replicate(applyValue("val")).join();
        }

        eventually(() -> assertThat(getSnapshotEntry(leader).getIndex()).isGreaterThan(0));

        group.allowMessagesTo(leader.getLocalEndpoint(), slowFollower.getLocalEndpoint(), InstallSnapshotRequest.class);

        eventually(() -> {
            SnapshotChunkCollector snapshotChunkCollector = getSnapshotChunkCollector(slowFollower);
            assertThat(snapshotChunkCollector).isNotNull();
            assertThat(snapshotChunkCollector.getSnapshottedMembers().size()).isEqualTo(1);
        });

        group.terminateNode(leader.getLocalEndpoint());

        for (RaftNodeImpl follower : followers) {
            eventually(
                    () -> assertThat(follower.getLeaderEndpoint()).isNotNull().isNotEqualTo(leader.getLocalEndpoint()));
        }

        RaftNodeImpl newLeader = group.getLeaderNode();
        assertThat(newLeader).isNotNull();
        newLeader.replicate(applyValue("newLeaderVal")).join();

        group.allowMessagesTo(newLeader.getLocalEndpoint(), slowFollower.getLocalEndpoint(),
                InstallSnapshotRequest.class);

        eventually(() -> assertThat(getSnapshotChunkCollector(slowFollower).getSnapshottedMembers().size())
                .isGreaterThan(1));

        for (RaftNodeImpl follower : followers) {
            if (follower != slowFollower) {
                group.allowMessagesTo(follower.getLocalEndpoint(), slowFollower.getLocalEndpoint(),
                        InstallSnapshotRequest.class);
                group.allowMessagesTo(slowFollower.getLocalEndpoint(), follower.getLocalEndpoint(),
                        InstallSnapshotResponse.class);
            }
        }

        eventually(() -> assertThat(getCommitIndex(slowFollower)).isEqualTo(getCommitIndex(newLeader)));
        assertThat(getSnapshotChunkCollector(slowFollower)).isNull();
    }

    @Test(timeout = 300_000)
    public void when_promotionCommitFallsIntoSnapshot_then_promotedMemberTurnsIntoVotingMemberByInstallingSnapshot() {
        int entryCount = 50;
        RaftConfig config = RaftConfig.newBuilder().setCommitCountToTakeSnapshot(entryCount)
                .setLeaderHeartbeatPeriodSecs(1).setLeaderHeartbeatTimeoutSecs(5).build();
        group = LocalRaftGroup.newBuilder(3).enableNewTermOperation().setConfig(config).build();
        group.start();

        RaftNodeImpl leader = group.waitUntilLeaderElected();

        RaftNodeImpl newNode = group.createNewNode();

        Ordered<RaftGroupMembers> membershipChangeResult = leader
                .changeMembership(newNode.getLocalEndpoint(), ADD_LEARNER, 0).join();

        eventually(() -> assertThat(getCommitIndex(newNode)).isEqualTo(getCommitIndex(leader)));

        group.dropMessagesTo(leader.getLocalEndpoint(), newNode.getLocalEndpoint(), AppendEntriesRequest.class);

        leader.changeMembership(newNode.getLocalEndpoint(), ADD_OR_PROMOTE_TO_FOLLOWER,
                membershipChangeResult.getCommitIndex()).join();

        while (getSnapshotEntry(leader).getIndex() == 0) {
            leader.replicate(applyValue("val")).join();
        }

        eventually(() -> {
            assertThat(getCommitIndex(newNode)).isEqualTo(getCommitIndex(leader));
            assertThat(getRole(newNode)).isEqualTo(RaftRole.FOLLOWER);
        });
    }

    @Test(timeout = 30_000)
    public void peerSnapshotCancelsPreVoteAndHeartbeatRestartsIt() {
        testPeerSnapshotDuringPreVote(true);
    }

    @Test(timeout = 30_000)
    public void peerSnapshotCancelsPreVoteAndTimeoutRestartsIt() {
        testPeerSnapshotDuringPreVote(false);
    }

    private void testPeerSnapshotDuringPreVote(boolean retryOnHeartbeat) {
        int votingMemberCount = 3;
        group = LocalRaftGroup.start(votingMemberCount + 1, votingMemberCount);
        RaftNodeImpl leader = group.waitUntilLeaderElected();
        RaftNodeImpl candidate = group.<RaftNodeImpl>getNodes().stream()
                .filter(node -> getRole(node) == FOLLOWER).findFirst().get();
        RaftNodeImpl learner = group.<RaftNodeImpl>getNodes().stream()
                .filter(node -> getRole(node) == LEARNER).findFirst().get();
        RaftNodeImpl peer = group.<RaftNodeImpl>getNodes().stream()
                .filter(node -> node != candidate && getRole(node) == FOLLOWER).findFirst().get();
        group.splitMembers(candidate.getLocalEndpoint());
        leader.replicate(applyValue("seed")).join();
        long membershipIndex = leader.changeMembership(learner.getLocalEndpoint(),
                ADD_OR_PROMOTE_TO_FOLLOWER, 0).join().getCommitIndex();
        eventually(() -> assertThat(getCommitIndex(peer)).isEqualTo(membershipIndex));
        peer.takeSnapshot().join();
        SnapshotEntry snapshot = getSnapshotEntry(peer);
        SnapshotChunk chunk = (SnapshotChunk) ((List<?>) snapshot.getOperation()).get(0);
        assertThat(snapshot.getSnapshotChunkCount()).isEqualTo(1);

        CompletableFuture.runAsync(() -> {
            candidate.toFollower(candidate.state().term());
            candidate.state().initPreCandidateState();
            int term = candidate.state().term();
            assertThat(candidate.state().preCandidateState().majority()).isEqualTo(2);

            // Deliver a peer chunk after the leader timed out; the snapshot contains a promotion.
            new InstallSnapshotRequestHandler(candidate,
                    candidate.getModelFactory().createInstallSnapshotRequestBuilder()
                            .setGroupId(candidate.getGroupId()).setSender(peer.getLocalEndpoint())
                            .setSenderLeader(false).setTerm(term).setSnapshotTerm(snapshot.getTerm())
                            .setSnapshotIndex(snapshot.getIndex()).setTotalSnapshotChunkCount(1)
                            .setSnapshotChunk(chunk).setSnapshottedMembers(List.of(peer.getLocalEndpoint()))
                            .setGroupMembersView(snapshot.getGroupMembersView()).build()).run();

            assertThat(candidate.state().commitIndex()).isEqualTo(snapshot.getIndex());
            assertThat(candidate.state().leaderElectionQuorumSize()).isEqualTo(3);
            assertThat(candidate.state().preCandidateState()).isNull();
            PreVoteResponse delayedResponse = candidate.getModelFactory().createPreVoteResponseBuilder()
                    .setGroupId(candidate.getGroupId()).setSender(peer.getLocalEndpoint())
                    .setTerm(term + 1).setGranted(true).build();
            new PreVoteResponseHandler(candidate, delayedResponse).run();
            assertThat(candidate.state().role()).isEqualTo(FOLLOWER);
            assertThat(candidate.state().term()).isEqualTo(term);

            if (retryOnHeartbeat) {
                new HeartbeatTask(candidate).run();
            } else {
                new PreVoteTimeoutTask(candidate, term).run();
            }
            assertThat(candidate.state().preCandidateState().majority()).isEqualTo(3);
            assertThat(candidate.state().preCandidateState().voteCount()).isEqualTo(1);
            new PreVoteResponseHandler(candidate, delayedResponse).run();
            assertThat(candidate.state().preCandidateState().voteCount()).isEqualTo(2);
            assertThat(candidate.state().role()).isEqualTo(FOLLOWER);
            assertThat(candidate.state().term()).isEqualTo(term);
        }, candidate.getExecutor()::execute).join();
    }

    @Test(timeout = 300_000)
    public void when_snapshotTriggeredViaNotStartedSingletonNode_then_snapshotCannotBeTaken() {
        group = LocalRaftGroup.newBuilder(1).build();
        RaftNodeImpl node = group.getNodes().get(0);

        try {
            node.takeSnapshot().join();
            fail("Cannot take snapshot manually before started");
        } catch (CompletionException e) {
            assertThat(e).hasCauseInstanceOf(IllegalStateException.class);
        }
    }

    @Test(timeout = 300_000)
    public void when_snapshotTriggeredViaSingletonNodeWhenFirstLogEntryNotCommitted_then_snapshotCannotBeTaken() {
        group = LocalRaftGroup.start(1);
        RaftNodeImpl node = group.getNodes().get(0);

        try {
            node.takeSnapshot().join();
            fail("Cannot take snapshot manually before first entry committed");
        } catch (CompletionException e) {
            assertThat(e).hasCauseInstanceOf(IllegalStateException.class);
        }
    }

    @Test(timeout = 300_000)
    public void when_snapshotTriggeredViaSingletonNodeWhenFirstLogEntryCommitted_then_snapshotCanBeTaken() {
        group = LocalRaftGroup.newBuilder(1).enableNewTermOperation().start();
        RaftNodeImpl node = group.getNodes().get(0);

        Ordered<RaftNodeReport> result = node.takeSnapshot().join();
        assertThat(result.getCommitIndex()).isEqualTo(FIRST_VALID_LOG_INDEX);
    }

    @Test(timeout = 300_000)
    public void when_snapshotTriggeredViaNotStartedNode_then_snapshotCannotBeTaken() {
        group = LocalRaftGroup.newBuilder(2).build();
        RaftNodeImpl node = group.getNodes().get(0);

        try {
            node.takeSnapshot().join();
            fail("Cannot take snapshot manually before started");
        } catch (CompletionException e) {
            assertThat(e).hasCauseInstanceOf(IllegalStateException.class);
        }
    }

    @Test(timeout = 300_000)
    public void when_snapshotTriggeredViaLeaderWhenFirstEntryNotCommitted_then_snapshotCannotBeTaken() {
        group = LocalRaftGroup.start(3);
        RaftNodeImpl node = group.waitUntilLeaderElected();

        try {
            node.takeSnapshot().join();
            fail("Cannot take snapshot manually before first entry committed");
        } catch (CompletionException e) {
            assertThat(e).hasCauseInstanceOf(IllegalStateException.class);
        }
    }

    @Test(timeout = 300_000)
    public void when_snapshotTriggeredViaLeaderWhenFirstEntryCommitted_then_snapshotCanBeTaken() {
        group = LocalRaftGroup.newBuilder(3).enableNewTermOperation().start();
        RaftNodeImpl node = group.waitUntilLeaderElected();
        eventually(() -> {
            assertThat(getCommitIndex(node)).isGreaterThan(0);
        });

        Ordered<RaftNodeReport> result = node.takeSnapshot().join();
        assertThat(result.getCommitIndex()).isEqualTo(FIRST_VALID_LOG_INDEX);
    }

    @Test(timeout = 300_000)
    public void when_snapshotTriggeredViaLeaderMultipleTimesAtSameIndex_then_singleSnapshotIsTaken() {
        group = LocalRaftGroup.newBuilder(3).enableNewTermOperation().start();
        RaftNodeImpl node = group.waitUntilLeaderElected();
        eventually(() -> {
            assertThat(getCommitIndex(node)).isGreaterThan(0);
        });

        Ordered<RaftNodeReport> result1 = node.takeSnapshot().join();
        assertThat(result1.getCommitIndex()).isEqualTo(FIRST_VALID_LOG_INDEX);
        assertThat(result1.getResult()).isNotNull();
        Ordered<RaftNodeReport> result2 = node.takeSnapshot().join();
        assertThat(result2.getCommitIndex()).isEqualTo(result1.getCommitIndex());
        assertThat(result2.getResult()).isNull();
    }

    @Test(timeout = 300_000)
    public void when_snapshotTriggeredViaFollowerMultipleTimesAtSameIndex_then_singleSnapshotIsTaken() {
        group = LocalRaftGroup.newBuilder(2).enableNewTermOperation().start();
        RaftNodeImpl leader = group.waitUntilLeaderElected();
        RaftNodeImpl node = group.getAnyNodeExcept(leader.getLeaderEndpoint());
        eventually(() -> {
            assertThat(getCommitIndex(node)).isGreaterThan(0);
        });

        Ordered<RaftNodeReport> result1 = node.takeSnapshot().join();
        assertThat(result1.getCommitIndex()).isEqualTo(FIRST_VALID_LOG_INDEX);
        Ordered<RaftNodeReport> result2 = node.takeSnapshot().join();
        assertThat(result2.getCommitIndex()).isEqualTo(result1.getCommitIndex());
    }

    @Test(timeout = 300_000)
    public void when_snapshotTriggeredViaFollowerWhenFirstEntryCommitted_then_snapshotCanBeTaken() {
        group = LocalRaftGroup.newBuilder(3).enableNewTermOperation().start();
        group.waitUntilLeaderElected();
        RaftNodeImpl node = group.getAnyNodeExcept(group.getLeaderEndpoint());
        eventually(() -> {
            assertThat(getCommitIndex(node)).isGreaterThan(0);
        });

        Ordered<RaftNodeReport> result = node.takeSnapshot().join();
        assertThat(result.getCommitIndex()).isEqualTo(FIRST_VALID_LOG_INDEX);
    }

    @Test(timeout = 300_000)
    public void when_snapshotTriggeredViaLearnerWhenFirstEntryCommitted_then_snapshotCanBeTaken() {
        group = LocalRaftGroup.newBuilder(3).enableNewTermOperation().start();
        RaftNodeImpl leader = group.waitUntilLeaderElected();

        RaftNodeImpl newNode = group.createNewNode();

        long commitIndex = leader.changeMembership(newNode.getLocalEndpoint(), ADD_LEARNER, 0).join().getCommitIndex();

        eventually(() -> {
            assertThat(getCommitIndex(newNode)).isEqualTo(getCommitIndex(leader));
        });

        Ordered<RaftNodeReport> result = newNode.takeSnapshot().join();
        assertThat(result.getCommitIndex()).isEqualTo(commitIndex);
    }

    @Test(timeout = 300_000)
    public void when_manualSnapshotIsTakenViaLeader_then_leaderCanTakeAutomaticSnapshot() {
        int entryCount = 50;
        RaftConfig config = RaftConfig.newBuilder().setCommitCountToTakeSnapshot(entryCount)
                .setMaxPendingLogEntryCount(entryCount / 10).build();
        group = LocalRaftGroup.newBuilder(3).setConfig(config).enableNewTermOperation().start();
        RaftNodeImpl leader = group.waitUntilLeaderElected();
        eventually(() -> {
            assertThat(getCommitIndex(leader)).isGreaterThan(0);
        });

        long snapshotIndex1 = leader.takeSnapshot().join().getCommitIndex();

        for (int i = 0; i < entryCount * 5; i++) {
            leader.replicate(applyValue("val")).join();
        }

        long snapshotIndex2 = getSnapshotEntry(leader).getIndex();
        assertThat(snapshotIndex2).isGreaterThan(snapshotIndex1);

        for (int i = 0; i < entryCount * 10 + entryCount / 2; i++) {
            leader.replicate(applyValue("val")).join();
        }

        long snapshotIndex3 = leader.takeSnapshot().join().getCommitIndex();

        assertThat(snapshotIndex3).isGreaterThan(snapshotIndex2);
    }

    @Test(timeout = 300_000)
    public void when_manualSnapshotIsTakenViaFollower_then_followerCanTakeAutomaticSnapshot() {
        int entryCount = 50;
        RaftConfig config = RaftConfig.newBuilder().setCommitCountToTakeSnapshot(entryCount)
                .setMaxPendingLogEntryCount(entryCount / 10).build();
        group = LocalRaftGroup.newBuilder(3).setConfig(config).enableNewTermOperation().start();
        RaftNodeImpl leader = group.waitUntilLeaderElected();
        RaftNodeImpl follower = group.getAnyNodeExcept(leader.getLocalEndpoint());
        eventually(() -> {
            assertThat(getCommitIndex(follower)).isGreaterThan(0);
        });

        long snapshotIndex1 = follower.takeSnapshot().join().getCommitIndex();

        for (int i = 0; i < entryCount * 5; i++) {
            leader.replicate(applyValue("val")).join();
        }

        eventually(() -> {
            assertThat(getCommitIndex(follower)).isEqualTo(getCommitIndex(leader));
        });

        long snapshotIndex2 = getSnapshotEntry(follower).getIndex();

        assertThat(snapshotIndex2).isGreaterThan(snapshotIndex1);

        for (int i = 0; i < entryCount * 10 + entryCount / 2; i++) {
            leader.replicate(applyValue("val")).join();
        }

        eventually(() -> {
            assertThat(getCommitIndex(follower)).isEqualTo(getCommitIndex(leader));
        });

        long snapshotIndex3 = follower.takeSnapshot().join().getCommitIndex();
        assertThat(snapshotIndex3).isGreaterThan(snapshotIndex2);
    }
}
