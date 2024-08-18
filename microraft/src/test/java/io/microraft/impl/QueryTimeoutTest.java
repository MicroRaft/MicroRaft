/*
 * Copyright (c) 2023, MicroRaft.
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

import static io.microraft.QueryPolicy.EVENTUAL_CONSISTENCY;
import static io.microraft.QueryPolicy.LEADER_LEASE;
import static io.microraft.QueryPolicy.LINEARIZABLE;
import static io.microraft.impl.local.SimpleStateMachine.applyValue;
import static io.microraft.impl.local.SimpleStateMachine.queryLastValue;
import static io.microraft.test.util.AssertionUtils.eventually;
import static io.microraft.test.util.RaftTestUtils.getCommitIndex;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.fail;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

import org.junit.After;
import org.junit.Test;

import io.microraft.MembershipChangeMode;
import io.microraft.Ordered;
import io.microraft.RaftConfig;
import io.microraft.exception.LaggingCommitIndexException;
import io.microraft.impl.local.LocalRaftGroup;
import io.microraft.model.message.AppendEntriesRequest;
import io.microraft.model.message.InstallSnapshotRequest;
import io.microraft.test.util.BaseTest;

// TODO(szymon): Determine if tests needed.

public class QueryTimeoutTest extends BaseTest {

    private LocalRaftGroup group;

    @After
    public void destroy() {
        if (group != null) {
            group.destroy();
        }
    }

    @Test(timeout = 300_000)
    public void when_followerQueriedWithFutureCommitIndex_then_queryExecutedWhenCommitIndexAdvances() {
        group = LocalRaftGroup.start(3);

        RaftNodeImpl leader = group.waitUntilLeaderElected();
        RaftNodeImpl follower = group.getAnyNodeExcept(leader.getLocalEndpoint());
        group.dropMessagesTo(leader.getLocalEndpoint(), follower.getLocalEndpoint(), AppendEntriesRequest.class);

        long commitIndex = leader.replicate(applyValue("value")).join().getCommitIndex();

        CompletableFuture<Ordered<Object>> f = follower.query(queryLastValue(), EVENTUAL_CONSISTENCY,
                Optional.of(commitIndex), Optional.of(Duration.ofSeconds(30)));

        group.allowAllMessagesTo(leader.getLocalEndpoint(), follower.getLocalEndpoint());
        Ordered<Object> o = f.join();

        assertThat(o.getResult()).isEqualTo("value");
        assertThat(o.getCommitIndex()).isEqualTo(commitIndex);
    }

    @Test(timeout = 300_000)
    public void when_multipleQueriesScheduledToFollowerdWithFutureCommitIndex_then_queriesExecutedWhenCommitIndexAdvances() {
        group = LocalRaftGroup.start(3);

        RaftNodeImpl leader = group.waitUntilLeaderElected();
        RaftNodeImpl follower = group.getAnyNodeExcept(leader.getLocalEndpoint());
        group.dropMessagesTo(leader.getLocalEndpoint(), follower.getLocalEndpoint(), AppendEntriesRequest.class);

        long commitIndex = leader.replicate(applyValue("value")).join().getCommitIndex();

        List<CompletableFuture<Ordered<Object>>> futures = new ArrayList<>();
        int queryCount = 5;
        for (int i = 0; i < queryCount; i++) {
            CompletableFuture<Ordered<Object>> f = follower.query(queryLastValue(), EVENTUAL_CONSISTENCY,
                    Optional.of(commitIndex), Optional.of(Duration.ofSeconds(30)));
            futures.add(f);
        }

        group.allowAllMessagesTo(leader.getLocalEndpoint(), follower.getLocalEndpoint());

        for (CompletableFuture<Ordered<Object>> f : futures) {
            Ordered<Object> o = f.join();

            assertThat(o.getResult()).isEqualTo("value");
            assertThat(o.getCommitIndex()).isEqualTo(commitIndex);
        }
    }

    @Test(timeout = 300_000)
    public void when_multipleQueriesScheduledToFollowerdWithFutureCommitIndex_then_queriesExecutedWhenCommitIndexAdvancesViaInstalledSnapshot() {
        int logSize = 10;
        RaftConfig config = RaftConfig.newBuilder().setCommitCountToTakeSnapshot(logSize).build();
        group = LocalRaftGroup.start(3, config);

        RaftNodeImpl leader = group.waitUntilLeaderElected();
        RaftNodeImpl follower = group.getAnyNodeExcept(leader.getLocalEndpoint());
        group.dropMessagesTo(leader.getLocalEndpoint(), follower.getLocalEndpoint(), AppendEntriesRequest.class);
        group.dropMessagesTo(leader.getLocalEndpoint(), follower.getLocalEndpoint(), InstallSnapshotRequest.class);

        String latestValue = null;
        long latestCommitIndex = 0;
        for (int i = 0; i < logSize; i++) {
            latestValue = "value" + i;
            latestCommitIndex = leader.replicate(applyValue(latestValue)).join().getCommitIndex();
        }

        CompletableFuture<Ordered<Object>> f = follower.query(queryLastValue(), EVENTUAL_CONSISTENCY,
                Optional.of(latestCommitIndex - 1), Optional.of(Duration.ofSeconds(30)));

        group.allowAllMessagesTo(leader.getLocalEndpoint(), follower.getLocalEndpoint());
        Ordered<Object> o = f.join();

        assertThat(o.getResult()).isEqualTo(latestValue);
        assertThat(o.getCommitIndex()).isEqualTo(latestCommitIndex);
    }

    @Test(timeout = 300_000)
    public void when_followerQueriedWithPastCommitIndex_then_queryExecutedImmediately() {
        group = LocalRaftGroup.start(3);

        RaftNodeImpl leader = group.waitUntilLeaderElected();
        RaftNodeImpl follower = group.getAnyNodeExcept(leader.getLocalEndpoint());

        long commitIndex = leader.replicate(applyValue("value")).join().getCommitIndex();

        eventually(() -> {
            assertThat(getCommitIndex(follower)).isEqualTo(commitIndex);
        });

        Ordered<Object> o = follower.query(queryLastValue(), EVENTUAL_CONSISTENCY, Optional.of(commitIndex),
                Optional.of(Duration.ofSeconds(1))).join();

        assertThat(o.getResult()).isEqualTo("value");
        assertThat(o.getCommitIndex()).isEqualTo(commitIndex);
    }

    @Test(timeout = 300_000)
    public void when_followerQueriedWithFutureCommitIndex_then_queryTimesOutIfCommitIndexDoesNotAdvance() {
        group = LocalRaftGroup.start(3);

        RaftNodeImpl leader = group.waitUntilLeaderElected();
        RaftNodeImpl follower = group.getAnyNodeExcept(leader.getLocalEndpoint());
        group.dropMessagesTo(leader.getLocalEndpoint(), follower.getLocalEndpoint(), AppendEntriesRequest.class);

        long commitIndex = leader.replicate(applyValue("value")).join().getCommitIndex();

        try {
            follower.query(queryLastValue(), EVENTUAL_CONSISTENCY, Optional.of(commitIndex),
                    Optional.of(Duration.ofSeconds(1))).join();
            fail();
        } catch (CompletionException e) {
            assertThat(e).hasCauseInstanceOf(LaggingCommitIndexException.class);
        }
    }

    @Test(timeout = 300_000)
    public void when_followerQueriedWithFutureCommitIndexAndNegativeTimeout_then_queryFailsImmediately() {
        group = LocalRaftGroup.start(3);

        RaftNodeImpl leader = group.waitUntilLeaderElected();
        RaftNodeImpl follower = group.getAnyNodeExcept(leader.getLocalEndpoint());

        long commitIndex = leader.replicate(applyValue("value")).join().getCommitIndex();

        try {
            follower.query(queryLastValue(), EVENTUAL_CONSISTENCY, Optional.of(commitIndex + 1),
                    Optional.of(Duration.ofSeconds(-1))).join();
            fail();
        } catch (CompletionException e) {
            assertThat(e).hasCauseInstanceOf(LaggingCommitIndexException.class);
        }
    }

    @Test(timeout = 300_000)
    public void when_leaderQueriedWithLeaderLeaseAndFutureCommitIndex_then_queryFailsImmediately() {
        group = LocalRaftGroup.start(3);

        RaftNodeImpl leader = group.waitUntilLeaderElected();

        long commitIndex = leader.replicate(applyValue("value")).join().getCommitIndex();

        try {
            leader.query(queryLastValue(), LEADER_LEASE, Optional.of(commitIndex + 1),
                    Optional.of(Duration.ofSeconds(1))).join();
            fail();
        } catch (CompletionException e) {
            assertThat(e).hasCauseInstanceOf(LaggingCommitIndexException.class);
        }
    }

    @Test(timeout = 300_000)
    public void when_leaderQueriedWithLinearizableAndFutureCommitIndex_then_queryFailsImmediately() {
        group = LocalRaftGroup.start(3);

        RaftNodeImpl leader = group.waitUntilLeaderElected();

        long commitIndex = leader.replicate(applyValue("value")).join().getCommitIndex();

        try {
            leader.query(queryLastValue(), LINEARIZABLE, Optional.of(commitIndex + 1),
                    Optional.of(Duration.ofSeconds(1))).join();
            fail();
        } catch (CompletionException e) {
            assertThat(e).hasCauseInstanceOf(LaggingCommitIndexException.class);
        }
    }

    @Test(timeout = 300_000)
    public void when_leaderQueriedWithEventualConsistencyAndFutureCommitIndex_then_queryFailsWithTimeout() {
        group = LocalRaftGroup.start(3);

        RaftNodeImpl leader = group.waitUntilLeaderElected();

        long commitIndex = leader.replicate(applyValue("value")).join().getCommitIndex();

        try {
            leader.query(queryLastValue(), EVENTUAL_CONSISTENCY, Optional.of(commitIndex + 1),
                    Optional.of(Duration.ofSeconds(1))).join();
            fail();
        } catch (CompletionException e) {
            assertThat(e).hasCauseInstanceOf(LaggingCommitIndexException.class);
        }
    }

    @Test(timeout = 300_000)
    public void when_followerQueriedWithFutureCommitIndex_then_queryFailsOnTerminate() {
        group = LocalRaftGroup.start(3);

        RaftNodeImpl leader = group.waitUntilLeaderElected();
        RaftNodeImpl follower = group.getAnyNodeExcept(leader.getLocalEndpoint());
        group.dropMessagesTo(leader.getLocalEndpoint(), follower.getLocalEndpoint(), AppendEntriesRequest.class);

        long commitIndex = leader.replicate(applyValue("value")).join().getCommitIndex();

        CompletableFuture<Ordered<Object>> f = follower.query(queryLastValue(), EVENTUAL_CONSISTENCY,
                Optional.of(commitIndex), Optional.of(Duration.ofSeconds(30)));

        follower.terminate().join();

        try {
            f.join();
            fail();
        } catch (CompletionException e) {
            assertThat(e).hasCauseInstanceOf(LaggingCommitIndexException.class);
        }
    }

    @Test(timeout = 300_000)
    public void when_leaderQueriedWithFutureCommitIndex_then_queryFailsWhenLeaderLeavesRaftGroup() {
        group = LocalRaftGroup.start(3);

        RaftNodeImpl leader = group.waitUntilLeaderElected();

        long commitIndex = leader.replicate(applyValue("value")).join().getCommitIndex();

        CompletableFuture<Ordered<Object>> f = leader.query(queryLastValue(), EVENTUAL_CONSISTENCY,
                Optional.of(commitIndex + 100), Optional.of(Duration.ofSeconds(300)));

        leader.changeMembership(leader.getLocalEndpoint(), MembershipChangeMode.REMOVE_MEMBER, 0).join();

        try {
            f.join();
            fail();
        } catch (CompletionException e) {
            assertThat(e).hasCauseInstanceOf(LaggingCommitIndexException.class);
        }
    }

    @Test(timeout = 300_000)
    public void when_followerWaitsForPastCommitIndex_then_barrierCompletesImmediately() {
        group = LocalRaftGroup.start(3);

        RaftNodeImpl leader = group.waitUntilLeaderElected();
        RaftNodeImpl follower = group.getAnyNodeExcept(leader.getLocalEndpoint());

        long commitIndex1 = leader.replicate(applyValue("value1")).join().getCommitIndex();
        long commitIndex2 = leader.replicate(applyValue("value2")).join().getCommitIndex();

        eventually(() -> {
            assertThat(getCommitIndex(follower)).isEqualTo(commitIndex2);
        });

        Ordered<Object> o = follower.waitFor(commitIndex1, Duration.ofSeconds(100)).join();

        assertThat(o.getResult()).isNull();
        assertThat(o.getCommitIndex()).isEqualTo(commitIndex2);
    }

    @Test(timeout = 300_000)
    public void when_followerWaitsForCurrentCommitIndex_then_barrierCompletesImmediately() {
        group = LocalRaftGroup.start(3);

        RaftNodeImpl leader = group.waitUntilLeaderElected();
        RaftNodeImpl follower = group.getAnyNodeExcept(leader.getLocalEndpoint());

        long commitIndex = leader.replicate(applyValue("value1")).join().getCommitIndex();

        eventually(() -> {
            assertThat(getCommitIndex(follower)).isEqualTo(commitIndex);
        });

        Ordered<Object> o = follower.waitFor(commitIndex, Duration.ofSeconds(100)).join();

        assertThat(o.getResult()).isNull();
        assertThat(o.getCommitIndex()).isEqualTo(commitIndex);
    }

    @Test(timeout = 300_000)
    public void when_followerWaitsForFutureCommitIndex_then_barrierFailsWithTimeout() {
        group = LocalRaftGroup.start(3);

        RaftNodeImpl leader = group.waitUntilLeaderElected();
        RaftNodeImpl follower = group.getAnyNodeExcept(leader.getLocalEndpoint());
        group.dropMessagesTo(leader.getLocalEndpoint(), follower.getLocalEndpoint(), AppendEntriesRequest.class);

        long commitIndex = leader.replicate(applyValue("value1")).join().getCommitIndex();

        try {
            follower.waitFor(commitIndex, Duration.ofSeconds(5)).join();
            fail();
        } catch (CompletionException e) {
            assertThat(e).hasCauseInstanceOf(LaggingCommitIndexException.class);
        }
    }

    @Test(timeout = 300_000)
    public void when_leaderWaitsForPastCommitIndex_then_barrierCompletesImmediately() {
        group = LocalRaftGroup.start(3);

        RaftNodeImpl leader = group.waitUntilLeaderElected();

        long commitIndex1 = leader.replicate(applyValue("value1")).join().getCommitIndex();
        long commitIndex2 = leader.replicate(applyValue("value2")).join().getCommitIndex();

        Ordered<Object> o = leader.waitFor(commitIndex1, Duration.ofSeconds(100)).join();

        assertThat(o.getResult()).isNull();
        assertThat(o.getCommitIndex()).isEqualTo(commitIndex2);
    }

    @Test(timeout = 300_000)
    public void when_leaderWaitsForCurrentCommitIndex_then_barrierCompletesImmediately() {
        group = LocalRaftGroup.start(3);

        RaftNodeImpl leader = group.waitUntilLeaderElected();

        long commitIndex = leader.replicate(applyValue("value1")).join().getCommitIndex();

        Ordered<Object> o = leader.waitFor(commitIndex, Duration.ofSeconds(100)).join();

        assertThat(o.getResult()).isNull();
        assertThat(o.getCommitIndex()).isEqualTo(commitIndex);
    }

    @Test(timeout = 300_000)
    public void when_leaderWaitsForFutureCommitIndex_then_barrierFailsWithTimeout() {
        group = LocalRaftGroup.start(3);

        RaftNodeImpl leader = group.waitUntilLeaderElected();

        long commitIndex = leader.replicate(applyValue("value1")).join().getCommitIndex();

        try {
            leader.waitFor(commitIndex + 1, Duration.ofSeconds(5)).join();
            fail();
        } catch (CompletionException e) {
            assertThat(e).hasCauseInstanceOf(LaggingCommitIndexException.class);
        }
    }

}
