/*
 * Copyright (c) 2026, MicroRaft.
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

import static io.microraft.QueryPolicy.LINEARIZABLE;
import static io.microraft.impl.local.SimpleStateMachine.applyValue;
import static io.microraft.impl.local.SimpleStateMachine.queryLastValue;
import static io.microraft.test.util.AssertionUtils.eventually;
import static io.microraft.test.util.RaftTestUtils.getLeaderQuerySequenceNumber;
import static io.microraft.test.util.RaftTestUtils.getMatchIndex;
import static io.microraft.test.util.RaftTestUtils.getSnapshotEntry;
import static io.microraft.test.util.RaftTestUtils.getTerm;
import static io.microraft.test.util.RaftTestUtils.readRaftState;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import io.microraft.Ordered;
import io.microraft.RaftConfig;
import io.microraft.impl.handler.AppendEntriesFailureResponseHandler;
import io.microraft.impl.handler.AppendEntriesSuccessResponseHandler;
import io.microraft.impl.handler.InstallSnapshotResponseHandler;
import io.microraft.impl.local.LocalRaftGroup;
import io.microraft.model.message.AppendEntriesFailureResponse;
import io.microraft.model.message.AppendEntriesSuccessResponse;
import io.microraft.model.message.InstallSnapshotRequest;
import io.microraft.model.message.InstallSnapshotResponse;
import io.microraft.test.util.BaseTest;

/**
 * Follower indices and query sequence numbers belong to the leader state, which is
 * recreated every time a node becomes leader. A delayed response from an earlier
 * term can carry values that coincide with, or exceed, those of the current leader
 * state, so the leader must ignore it. Each test runs with the leader in its second
 * term so that the responses carry a term that really existed.
 */
public class StaleTermResponseTest extends BaseTest {

    private static final RaftConfig CONFIG = RaftConfig.newBuilder().setLeaderElectionTimeoutMillis(500)
            .setLeaderHeartbeatPeriodSecs(1).setLeaderHeartbeatTimeoutSecs(30).build();

    private LocalRaftGroup group;
    private RaftNodeImpl firstLeader;
    private RaftNodeImpl leader;
    private RaftNodeImpl follower;

    @Before
    public void before() {
        group = LocalRaftGroup.newBuilder(3).setConfig(CONFIG).enableNewTermOperation().start();
        firstLeader = group.waitUntilLeaderElected();
        firstLeader.replicate(applyValue("one")).join();
        List<RaftNodeImpl> others = group.getNodesExcept(firstLeader.getLocalEndpoint());
        leader = others.get(0);
        follower = others.get(1);
    }

    @After
    public void destroy() {
        if (group != null) {
            group.destroy();
        }
    }

    @Test(timeout = 300_000)
    public void when_successResponseWithStaleTermAndEqualQuerySequenceNumber_then_queryIsNotAcked() {
        transferLeadershipToLeader();
        CompletableFuture<Ordered<Object>> query = startQueryWithoutAcks();

        AppendEntriesSuccessResponse response = leader.getModelFactory().createAppendEntriesSuccessResponseBuilder()
                .setGroupId(leader.getGroupId()).setSender(follower.getLocalEndpoint()).setTerm(staleTerm())
                .setLastLogIndex(0).setQuerySequenceNumber(getLeaderQuerySequenceNumber(leader))
                .setFlowControlSequenceNumber(0).build();
        runOnLeader(new AppendEntriesSuccessResponseHandler(leader, response));

        assertThat(query).isNotDone();
    }

    @Test(timeout = 300_000)
    public void when_successResponseWithStaleTermAndHigherLastLogIndex_then_matchIndexIsNotUpdated() {
        dropResponsesTo(leader, follower);
        transferLeadershipToLeader();
        assertThat(getMatchIndex(leader, follower.getLocalEndpoint())).isZero();

        long lastLogIndex = readRaftState(leader, () -> leader.state().log().lastLogOrSnapshotIndex());
        AppendEntriesSuccessResponse response = leader.getModelFactory().createAppendEntriesSuccessResponseBuilder()
                .setGroupId(leader.getGroupId()).setSender(follower.getLocalEndpoint()).setTerm(staleTerm())
                .setLastLogIndex(lastLogIndex).setQuerySequenceNumber(0).setFlowControlSequenceNumber(0).build();
        runOnLeader(new AppendEntriesSuccessResponseHandler(leader, response));

        assertThat(getMatchIndex(leader, follower.getLocalEndpoint())).isZero();
    }

    @Test(timeout = 300_000)
    public void when_failureResponseWithStaleTermAndMatchingExpectedNextIndex_then_nextIndexIsNotUpdated() {
        dropResponsesTo(leader, follower);
        transferLeadershipToLeader();
        long nextIndex = getNextIndex();
        assertThat(nextIndex).isGreaterThan(getMatchIndex(leader, follower.getLocalEndpoint()) + 1);

        AppendEntriesFailureResponse response = leader.getModelFactory().createAppendEntriesFailureResponseBuilder()
                .setGroupId(leader.getGroupId()).setSender(follower.getLocalEndpoint()).setTerm(staleTerm())
                .setExpectedNextIndex(nextIndex).setQuerySequenceNumber(0).setFlowControlSequenceNumber(0).build();
        runOnLeader(new AppendEntriesFailureResponseHandler(leader, response));

        assertThat(getNextIndex()).isEqualTo(nextIndex);
    }

    @Test(timeout = 300_000)
    public void when_installSnapshotResponseWithStaleTermAndEqualQuerySequenceNumber_then_queryIsNotAcked() {
        transferLeadershipToLeader();
        CompletableFuture<Ordered<Object>> query = startQueryWithoutAcks();

        // The flow control sequence number does not match, so that a handled response
        // acks the query but sends no snapshot chunk back.
        InstallSnapshotResponse response = leader.getModelFactory().createInstallSnapshotResponseBuilder()
                .setGroupId(leader.getGroupId()).setSender(follower.getLocalEndpoint()).setTerm(staleTerm())
                .setSnapshotIndex(0).setRequestedSnapshotChunkIndex(0)
                .setQuerySequenceNumber(getLeaderQuerySequenceNumber(leader))
                .setFlowControlSequenceNumber(Long.MAX_VALUE).build();
        runOnLeader(new InstallSnapshotResponseHandler(leader, response));

        assertThat(query).isNotDone();
    }

    @Test(timeout = 300_000)
    public void when_installSnapshotResponseWithStaleTermArrivesAtFollower_then_snapshotChunkIsStillServed() {
        transferLeadershipToLeader();
        follower.takeSnapshot().join();
        long snapshotIndex = getSnapshotEntry(follower).getIndex();

        AtomicReference<InstallSnapshotRequest> served = new AtomicReference<>();
        group.alterMessagesTo(follower.getLocalEndpoint(), firstLeader.getLocalEndpoint(), message -> {
            if (message instanceof InstallSnapshotRequest) {
                served.compareAndSet(null, (InstallSnapshotRequest) message);
            }
            return message;
        });

        InstallSnapshotResponse response = follower.getModelFactory().createInstallSnapshotResponseBuilder()
                .setGroupId(follower.getGroupId()).setSender(firstLeader.getLocalEndpoint())
                .setTerm(getTerm(follower) - 1).setSnapshotIndex(snapshotIndex).setRequestedSnapshotChunkIndex(0)
                .setQuerySequenceNumber(0).setFlowControlSequenceNumber(0).build();
        readRaftState(follower, () -> {
            new InstallSnapshotResponseHandler(follower, response).run();
            return null;
        });

        eventually(() -> assertThat(served.get()).isNotNull());
        assertThat(served.get().isSenderLeader()).isFalse();
        assertThat(served.get().getSnapshotIndex()).isEqualTo(snapshotIndex);
        assertThat(served.get().getSnapshotChunk().getSnapshotChunkIndex()).isZero();
        assertThat(served.get().getTerm()).isEqualTo(getTerm(follower));
    }

    private void transferLeadershipToLeader() {
        firstLeader.transferLeadership(leader.getLocalEndpoint()).join();
        eventually(() -> assertThat(group.getLeaderEndpoint()).isEqualTo(leader.getLocalEndpoint()));
        leader.replicate(applyValue("two")).join();
        assertThat(staleTerm()).isPositive();
    }

    // The query stays live because no follower acks can reach the leader.
    private CompletableFuture<Ordered<Object>> startQueryWithoutAcks() {
        dropResponsesTo(leader, firstLeader);
        dropResponsesTo(leader, follower);
        CompletableFuture<Ordered<Object>> query = leader.query(queryLastValue(), LINEARIZABLE, Optional.empty(),
                Optional.empty());
        eventually(() -> assertThat(getLeaderQuerySequenceNumber(leader)).isPositive());
        assertThat(query).isNotDone();
        return query;
    }

    private void dropResponsesTo(RaftNodeImpl target, RaftNodeImpl source) {
        group.dropMessagesTo(source.getLocalEndpoint(), target.getLocalEndpoint(),
                AppendEntriesSuccessResponse.class);
        group.dropMessagesTo(source.getLocalEndpoint(), target.getLocalEndpoint(),
                AppendEntriesFailureResponse.class);
    }

    private long getNextIndex() {
        return readRaftState(leader,
                () -> leader.state().leaderState().getFollowerState(follower.getLocalEndpoint()).nextIndex());
    }

    private int staleTerm() {
        return getTerm(leader) - 1;
    }

    private void runOnLeader(Runnable handler) {
        readRaftState(leader, () -> {
            handler.run();
            return null;
        });
    }
}
