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
import io.microraft.RaftNode;
import io.microraft.exception.CannotReplicateException;
import io.microraft.exception.LaggingCommitIndexException;
import io.microraft.exception.NotLeaderException;
import io.microraft.impl.local.LocalRaftGroup;
import io.microraft.model.message.AppendEntriesFailureResponse;
import io.microraft.model.message.AppendEntriesRequest;
import io.microraft.model.message.AppendEntriesSuccessResponse;
import io.microraft.model.message.InstallSnapshotRequest;
import io.microraft.test.util.BaseTest;
import org.junit.After;
import org.junit.Test;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

import static io.microraft.MembershipChangeMode.ADD_LEARNER;
import static io.microraft.QueryPolicy.LINEARIZABLE;
import static io.microraft.RaftRole.FOLLOWER;
import static io.microraft.impl.local.SimpleStateMachine.applyValue;
import static io.microraft.impl.local.SimpleStateMachine.queryLastValue;
import static io.microraft.test.util.AssertionUtils.eventually;
import static io.microraft.test.util.RaftTestUtils.TEST_RAFT_CONFIG;
import static io.microraft.test.util.RaftTestUtils.getCommitIndex;
import static io.microraft.test.util.RaftTestUtils.getLeaderQuerySequenceNumber;
import static io.microraft.test.util.RaftTestUtils.getRole;
import static io.microraft.test.util.RaftTestUtils.getSnapshotEntry;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.fail;

public class LinearizableQueryTest extends BaseTest {

    private LocalRaftGroup group;

    @After
    public void destroy() {
        if (group != null) {
            group.destroy();
        }
    }

    @Test(timeout = 300_000)
    public void when_linearizableQueryIsIssuedWithoutCommitIndex_then_itReadsLastState() {
        startGroup(5, TEST_RAFT_CONFIG);

        RaftNodeImpl leader = group.waitUntilLeaderElected();
        leader.replicate(applyValue("value1")).join();
        long commitIndex1 = getCommitIndex(leader);

        Ordered<Object> o1 = leader.query(queryLastValue(), LINEARIZABLE, 0).join();

        assertThat(o1.getResult()).isEqualTo("value1");
        assertThat(o1.getCommitIndex()).isEqualTo(commitIndex1);
        long leaderQuerySeqNum1 = getLeaderQuerySequenceNumber(leader);
        assertThat(leaderQuerySeqNum1).isGreaterThan(0);
        assertThat(getCommitIndex(leader)).isEqualTo(commitIndex1);

        leader.replicate(applyValue("value2")).join();
        long commitIndex2 = getCommitIndex(leader);

        Ordered<Object> o2 = leader.query(queryLastValue(), LINEARIZABLE, 0).join();

        assertThat(o2.getResult()).isEqualTo("value2");
        assertThat(o2.getCommitIndex()).isEqualTo(commitIndex2);
        long leaderQuerySeqNum2 = getLeaderQuerySequenceNumber(leader);
        assertThat(leaderQuerySeqNum2).isEqualTo(leaderQuerySeqNum1 + 1);
        assertThat(getCommitIndex(leader)).isEqualTo(commitIndex2);
    }

    @Test(timeout = 300_000)
    public void when_linearizableQueryIsIssuedWithCommitIndex_then_itReadsLastState() {
        startGroup(5, TEST_RAFT_CONFIG);

        RaftNodeImpl leader = group.waitUntilLeaderElected();
        leader.replicate(applyValue("value1")).join();
        long commitIndex1 = getCommitIndex(leader);

        Ordered<Object> o1 = leader.query(queryLastValue(), LINEARIZABLE, commitIndex1).join();

        assertThat(o1.getResult()).isEqualTo("value1");
        assertThat(o1.getCommitIndex()).isEqualTo(commitIndex1);
        long leaderQuerySeqNum1 = getLeaderQuerySequenceNumber(leader);
        assertThat(leaderQuerySeqNum1).isGreaterThan(0);
        assertThat(getCommitIndex(leader)).isEqualTo(commitIndex1);

        leader.replicate(applyValue("value2")).join();
        long commitIndex2 = getCommitIndex(leader);

        Ordered<Object> o2 = leader.query(queryLastValue(), LINEARIZABLE, commitIndex2).join();

        assertThat(o2.getResult()).isEqualTo("value2");
        assertThat(o2.getCommitIndex()).isEqualTo(commitIndex2);
        long leaderQuerySeqNum2 = getLeaderQuerySequenceNumber(leader);
        assertThat(leaderQuerySeqNum2).isEqualTo(leaderQuerySeqNum1 + 1);
        assertThat(getCommitIndex(leader)).isEqualTo(commitIndex2);
    }

    @Test(timeout = 300_000)
    public void when_linearizableQueryIsIssuedWithFurtherCommitIndex_then_itReadsLastState() {
        startGroup(5, TEST_RAFT_CONFIG);

        RaftNodeImpl leader = group.waitUntilLeaderElected();
        leader.replicate(applyValue("value1")).join();
        long commitIndex = getCommitIndex(leader);

        try {
            leader.query(queryLastValue(), LINEARIZABLE, commitIndex + 1).join();
            fail();
        } catch (CompletionException e) {
            assertThat(e).hasCauseInstanceOf(LaggingCommitIndexException.class);
        }
    }

    @Test(timeout = 300_000)
    public void when_newCommitIsDoneWhileThereIsWaitingQuery_then_queryRunsAfterNewCommit() {
        startGroup(5, TEST_RAFT_CONFIG);

        RaftNodeImpl leader = group.waitUntilLeaderElected();
        leader.replicate(applyValue("value1")).join();
        List<RaftNodeImpl> followers = group.getNodesExcept(leader.getLocalEndpoint());
        group.dropMessagesTo(leader.getLocalEndpoint(), followers.get(0).getLocalEndpoint(),
                AppendEntriesRequest.class);
        group.dropMessagesTo(leader.getLocalEndpoint(), followers.get(1).getLocalEndpoint(),
                AppendEntriesRequest.class);
        group.dropMessagesTo(leader.getLocalEndpoint(), followers.get(2).getLocalEndpoint(),
                AppendEntriesRequest.class);

        CompletableFuture<Ordered<Object>> replicateFuture = leader.replicate(applyValue("value2"));
        CompletableFuture<Ordered<Object>> queryFuture = leader.query(queryLastValue(), LINEARIZABLE, 0);

        group.resetAllRulesFrom(leader.getLocalEndpoint());

        Ordered<Object> replicationResult = replicateFuture.join();
        Ordered<Object> queryResult = queryFuture.join();
        assertThat(queryResult.getResult()).isEqualTo("value2");
        assertThat(queryResult.getCommitIndex()).isEqualTo(replicationResult.getCommitIndex());
    }

    private void startGroup(int groupSize, RaftConfig config) {
        group = LocalRaftGroup.newBuilder(groupSize).setConfig(config).enableNewTermOperation().start();
    }

    @Test(timeout = 300_000)
    public void when_multipleQueriesAreIssuedBeforeHeartbeatAcksReceived_then_allQueriesExecutedAtOnce() {
        startGroup(5, TEST_RAFT_CONFIG);

        RaftNodeImpl leader = group.waitUntilLeaderElected();
        leader.replicate(applyValue("value1")).join();
        List<RaftNodeImpl> followers = group.getNodesExcept(leader.getLocalEndpoint());
        group.dropMessagesTo(leader.getLocalEndpoint(), followers.get(0).getLocalEndpoint(),
                AppendEntriesRequest.class);
        group.dropMessagesTo(leader.getLocalEndpoint(), followers.get(1).getLocalEndpoint(),
                AppendEntriesRequest.class);
        group.dropMessagesTo(leader.getLocalEndpoint(), followers.get(2).getLocalEndpoint(),
                AppendEntriesRequest.class);

        CompletableFuture<Ordered<Object>> queryFuture1 = leader.query(queryLastValue(), LINEARIZABLE, 0);
        CompletableFuture<Ordered<Object>> queryFuture2 = leader.query(queryLastValue(), LINEARIZABLE, 0);

        group.resetAllRulesFrom(leader.getLocalEndpoint());

        long commitIndex = getCommitIndex(leader);

        Ordered<Object> result1 = queryFuture1.join();
        Ordered<Object> result2 = queryFuture2.join();
        assertThat(result1.getResult()).isEqualTo("value1");
        assertThat(result2.getResult()).isEqualTo("value1");
        assertThat(result1.getCommitIndex()).isEqualTo(commitIndex);
        assertThat(result2.getCommitIndex()).isEqualTo(commitIndex);
    }

    @Test(timeout = 300_000)
    public void when_newCommitIsDoneWhileThereAreMultipleQueries_then_allQueriesRunAfterCommit() {
        startGroup(5, TEST_RAFT_CONFIG);

        RaftNodeImpl leader = group.waitUntilLeaderElected();
        leader.replicate(applyValue("value1")).join();
        List<RaftNodeImpl> followers = group.getNodesExcept(leader.getLocalEndpoint());
        group.dropMessagesTo(leader.getLocalEndpoint(), followers.get(0).getLocalEndpoint(),
                AppendEntriesRequest.class);
        group.dropMessagesTo(leader.getLocalEndpoint(), followers.get(1).getLocalEndpoint(),
                AppendEntriesRequest.class);
        group.dropMessagesTo(leader.getLocalEndpoint(), followers.get(2).getLocalEndpoint(),
                AppendEntriesRequest.class);

        CompletableFuture<Ordered<Object>> replicateFuture = leader.replicate(applyValue("value2"));
        CompletableFuture<Ordered<Object>> queryFuture1 = leader.query(queryLastValue(), LINEARIZABLE, 0);
        CompletableFuture<Ordered<Object>> queryFuture2 = leader.query(queryLastValue(), LINEARIZABLE, 0);

        group.resetAllRulesFrom(leader.getLocalEndpoint());

        long commitIndex = replicateFuture.join().getCommitIndex();
        Ordered<Object> queryResult1 = queryFuture1.join();
        Ordered<Object> queryResult2 = queryFuture2.join();
        assertThat(queryResult1.getResult()).isEqualTo("value2");
        assertThat(queryResult2.getResult()).isEqualTo("value2");
        assertThat(queryResult1.getCommitIndex()).isEqualTo(commitIndex);
        assertThat(queryResult2.getCommitIndex()).isEqualTo(commitIndex);
    }

    @Test(timeout = 300_000)
    public void when_linearizableQueryIsIssuedToFollower_then_queryFails() {
        startGroup(5, TEST_RAFT_CONFIG);

        RaftNode leader = group.waitUntilLeaderElected();
        try {
            group.getAnyNodeExcept(leader.getLocalEndpoint()).query(queryLastValue(), LINEARIZABLE, 0).join();
            fail();
        } catch (CompletionException e) {
            assertThat(e).hasCauseInstanceOf(NotLeaderException.class);
        }
    }

    @Test(timeout = 300_000)
    public void when_multipleQueryLimitIsReachedBeforeHeartbeatAcks_then_noNewQueryIsAccepted() {
        RaftConfig config = RaftConfig.newBuilder().setMaxPendingLogEntryCount(1).build();
        startGroup(5, config);

        RaftNodeImpl leader = group.waitUntilLeaderElected();
        eventually(() -> assertThat(getCommitIndex(leader)).isEqualTo(1));

        List<RaftNodeImpl> followers = group.getNodesExcept(leader.getLocalEndpoint());
        group.dropMessagesTo(leader.getLocalEndpoint(), followers.get(0).getLocalEndpoint(),
                AppendEntriesRequest.class);
        group.dropMessagesTo(leader.getLocalEndpoint(), followers.get(1).getLocalEndpoint(),
                AppendEntriesRequest.class);
        group.dropMessagesTo(leader.getLocalEndpoint(), followers.get(2).getLocalEndpoint(),
                AppendEntriesRequest.class);

        CompletableFuture<Ordered<Object>> queryFuture1 = leader.query(queryLastValue(), LINEARIZABLE, 0);
        CompletableFuture<Ordered<Object>> queryFuture2 = leader.query(queryLastValue(), LINEARIZABLE, 0);

        try {
            queryFuture2.join();
            fail();
        } catch (CompletionException e) {
            assertThat(e).hasCauseInstanceOf(CannotReplicateException.class);
        }

        group.resetAllRulesFrom(leader.getLocalEndpoint());

        queryFuture1.join();
    }

    @Test(timeout = 300_000)
    public void when_leaderDemotesToFollowerWhileThereIsOngoingQuery_then_queryFails() {
        RaftConfig config = RaftConfig.newBuilder().setLeaderHeartbeatPeriodSecs(1).setLeaderHeartbeatTimeoutSecs(5)
                .build();
        startGroup(3, config);

        RaftNodeImpl leader = group.waitUntilLeaderElected();

        group.splitMembers(leader.getLocalEndpoint());

        CompletableFuture<Ordered<Object>> queryFuture = leader.query(queryLastValue(), LINEARIZABLE, 0);

        eventually(() -> assertThat(getRole(leader)).isEqualTo(FOLLOWER));

        try {
            queryFuture.join();
            fail();
        } catch (CompletionException e) {
            assertThat(e).hasCauseInstanceOf(NotLeaderException.class);
        }
    }

    @Test(timeout = 300_000)
    public void when_followersInstallSnapshot_then_queryIsExecutedOnLeaderWithInstallSnapshotResponse() {
        RaftConfig config = RaftConfig.newBuilder()
                .setLeaderElectionTimeoutMillis(TEST_RAFT_CONFIG.getLeaderElectionTimeoutMillis())
                .setLeaderHeartbeatPeriodSecs(TEST_RAFT_CONFIG.getLeaderHeartbeatPeriodSecs())
                .setLeaderHeartbeatTimeoutSecs(TEST_RAFT_CONFIG.getLeaderHeartbeatTimeoutSecs())
                .setCommitCountToTakeSnapshot(100).build();
        startGroup(3, config);

        RaftNodeImpl leader = group.waitUntilLeaderElected();
        List<RaftNodeImpl> followers = group.getNodesExcept(leader.getLocalEndpoint());
        group.dropMessagesTo(leader.getLocalEndpoint(), followers.get(1).getLocalEndpoint(),
                AppendEntriesRequest.class);
        group.dropMessagesTo(leader.getLocalEndpoint(), followers.get(1).getLocalEndpoint(),
                InstallSnapshotRequest.class);

        int i = 0;
        while (getSnapshotEntry(leader).getIndex() == 0) {
            leader.replicate(applyValue("val" + (++i))).join();
        }

        group.dropMessagesTo(leader.getLocalEndpoint(), followers.get(0).getLocalEndpoint(),
                AppendEntriesRequest.class);
        group.dropMessagesTo(leader.getLocalEndpoint(), followers.get(0).getLocalEndpoint(),
                InstallSnapshotRequest.class);

        CompletableFuture<Ordered<Object>> f = leader.query(queryLastValue(), LINEARIZABLE, 0);

        eventually(() -> assertThat(getLeaderQuerySequenceNumber(leader)).isEqualTo(1));

        group.allowAllMessagesTo(leader.getLocalEndpoint(), followers.get(0).getLocalEndpoint());
        group.allowAllMessagesTo(leader.getLocalEndpoint(), followers.get(1).getLocalEndpoint());

        Ordered<Object> result = f.join();
        assertThat(result.getResult()).isEqualTo("val" + i);
    }

    @Test
    public void when_followerFallsBehind_then_queryIsExecutedOnLeaderWithAppendEntriesFailureResponse() {
        startGroup(2, TEST_RAFT_CONFIG);

        RaftNodeImpl leader = group.waitUntilLeaderElected();
        RaftNodeImpl follower = group.getAnyNodeExcept(leader.getLocalEndpoint());

        int count = 10;
        for (int i = 0; i < count; i++) {
            leader.replicate(applyValue("val" + (i + 1))).join();
        }

        RaftNodeImpl newFollower = group.createNewNode();

        group.dropMessagesTo(leader.getLocalEndpoint(), newFollower.getLocalEndpoint(), AppendEntriesRequest.class);
        group.dropMessagesTo(newFollower.getLocalEndpoint(), leader.getLocalEndpoint(),
                AppendEntriesSuccessResponse.class);
        group.dropMessagesTo(newFollower.getLocalEndpoint(), leader.getLocalEndpoint(),
                AppendEntriesFailureResponse.class);

        leader.changeMembership(newFollower.getLocalEndpoint(), ADD_LEARNER, 0).join();

        group.dropMessagesTo(leader.getLocalEndpoint(), follower.getLocalEndpoint(), AppendEntriesRequest.class);

        CompletableFuture<Ordered<Object>> f = leader.query(queryLastValue(), LINEARIZABLE, 0);

        group.allowMessagesTo(newFollower.getLocalEndpoint(), leader.getLocalEndpoint(),
                AppendEntriesFailureResponse.class);
        group.allowMessagesTo(leader.getLocalEndpoint(), newFollower.getLocalEndpoint(), AppendEntriesRequest.class);

        f.join();
    }

}
