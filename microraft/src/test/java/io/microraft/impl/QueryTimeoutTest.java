package io.microraft.impl;

import static io.microraft.QueryPolicy.BOUNDED_STALENESS;
import static io.microraft.QueryPolicy.EVENTUAL_CONSISTENCY;
import static io.microraft.QueryPolicy.LEADER_LEASE;
import static io.microraft.QueryPolicy.LINEARIZABLE;;
import static io.microraft.impl.local.SimpleStateMachine.applyValue;
import static io.microraft.impl.local.SimpleStateMachine.queryLastValue;
import static io.microraft.test.util.AssertionUtils.eventually;
import static io.microraft.test.util.RaftTestUtils.TEST_RAFT_CONFIG;
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

import io.microraft.Ordered;
import io.microraft.QueryPolicy;
import io.microraft.RaftConfig;
import io.microraft.exception.LaggingCommitIndexException;
import io.microraft.impl.local.LocalRaftGroup;
import io.microraft.model.message.AppendEntriesRequest;
import io.microraft.model.message.InstallSnapshotRequest;
import io.microraft.test.util.BaseTest;

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

}
