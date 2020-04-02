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

import io.microraft.MembershipChangeMode;
import io.microraft.Ordered;
import io.microraft.RaftConfig;
import io.microraft.RaftEndpoint;
import io.microraft.exception.CannotReplicateException;
import io.microraft.exception.LaggingCommitIndexException;
import io.microraft.exception.NotLeaderException;
import io.microraft.impl.local.LocalRaftGroup;
import io.microraft.impl.util.BaseTest;
import io.microraft.model.message.AppendEntriesFailureResponse;
import io.microraft.model.message.AppendEntriesRequest;
import io.microraft.model.message.AppendEntriesSuccessResponse;
import io.microraft.model.message.InstallSnapshotRequest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import static io.microraft.QueryPolicy.LINEARIZABLE;
import static io.microraft.impl.local.SimpleStateMachine.apply;
import static io.microraft.impl.local.SimpleStateMachine.query;
import static io.microraft.impl.util.AssertionUtils.eventually;
import static io.microraft.impl.util.RaftTestUtils.TEST_RAFT_CONFIG;
import static io.microraft.impl.util.RaftTestUtils.getCommitIndex;
import static io.microraft.impl.util.RaftTestUtils.getLeaderQuerySeqNo;
import static io.microraft.impl.util.RaftTestUtils.getSnapshotEntry;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.fail;

/**
 * @author mdogan
 * @author metanet
 */
public class LinearizableQueryTest
        extends BaseTest {

    private LocalRaftGroup group;

    @Before
    public void init() {
        RaftConfig config = RaftConfig.newBuilder().setMaxUncommittedLogEntryCount(1).build();
        group = new LocalRaftGroup(5, config, true, null, null, null);
    }

    @After
    public void destroy() {
        if (group != null) {
            group.destroy();
        }
    }

    @Test(timeout = 300_000)
    public void when_linearizableQueryIsIssuedWithoutCommitIndex_then_itReadsLastState()
            throws Exception {
        group = newGroup();
        group.start();

        RaftNodeImpl leader = group.waitUntilLeaderElected();
        leader.replicate(apply("value1")).get();
        long commitIndex1 = getCommitIndex(leader);

        Ordered<Object> o1 = leader.query(query(), LINEARIZABLE, 0).get();

        assertThat(o1.getResult()).isEqualTo("value1");
        assertThat(o1.getCommitIndex()).isEqualTo(commitIndex1);
        long leaderQuerySeqNo1 = getLeaderQuerySeqNo(leader);
        assertThat(leaderQuerySeqNo1).isGreaterThan(0);
        assertThat(getCommitIndex(leader)).isEqualTo(commitIndex1);

        leader.replicate(apply("value2")).get();
        long commitIndex2 = getCommitIndex(leader);

        Ordered<Object> o2 = leader.query(query(), LINEARIZABLE, 0).get();

        assertThat(o2.getResult()).isEqualTo("value2");
        assertThat(o2.getCommitIndex()).isEqualTo(commitIndex2);
        long leaderQuerySeqNo2 = getLeaderQuerySeqNo(leader);
        assertThat(leaderQuerySeqNo2).isEqualTo(leaderQuerySeqNo1 + 1);
        assertThat(getCommitIndex(leader)).isEqualTo(commitIndex2);
    }

    private LocalRaftGroup newGroup() {
        return new LocalRaftGroup(5, TEST_RAFT_CONFIG, true, null, null, null);
    }

    @Test(timeout = 300_000)
    public void when_linearizableQueryIsIssuedWithCommitIndex_then_itReadsLastState()
            throws Exception {
        group = newGroup();
        group.start();

        RaftNodeImpl leader = group.waitUntilLeaderElected();
        leader.replicate(apply("value1")).get();
        long commitIndex1 = getCommitIndex(leader);

        Ordered<Object> o1 = leader.query(query(), LINEARIZABLE, commitIndex1).get();

        assertThat(o1.getResult()).isEqualTo("value1");
        assertThat(o1.getCommitIndex()).isEqualTo(commitIndex1);
        long leaderQuerySeqNo1 = getLeaderQuerySeqNo(leader);
        assertThat(leaderQuerySeqNo1).isGreaterThan(0);
        assertThat(getCommitIndex(leader)).isEqualTo(commitIndex1);

        leader.replicate(apply("value2")).get();
        long commitIndex2 = getCommitIndex(leader);

        Ordered<Object> o2 = leader.query(query(), LINEARIZABLE, commitIndex2).get();

        assertThat(o2.getResult()).isEqualTo("value2");
        assertThat(o2.getCommitIndex()).isEqualTo(commitIndex2);
        long leaderQuerySeqNo2 = getLeaderQuerySeqNo(leader);
        assertThat(leaderQuerySeqNo2).isEqualTo(leaderQuerySeqNo1 + 1);
        assertThat(getCommitIndex(leader)).isEqualTo(commitIndex2);
    }

    @Test(timeout = 300_000)
    public void when_linearizableQueryIsIssuedWithFurtherCommitIndex_then_itReadsLastState()
            throws Exception {
        group = newGroup();
        group.start();

        RaftNodeImpl leader = group.waitUntilLeaderElected();
        leader.replicate(apply("value1")).get();
        long commitIndex = getCommitIndex(leader);

        try {
            leader.query(query(), LINEARIZABLE, commitIndex + 1).get();
            fail();
        } catch (ExecutionException e) {
            assertThat(e).hasCauseInstanceOf(LaggingCommitIndexException.class);
        }
    }

    @Test(timeout = 300_000)
    public void when_newCommitIsDoneWhileThereIsWaitingQuery_then_queryRunsAfterNewCommit()
            throws Exception {
        group = newGroup();
        group.start();

        RaftNodeImpl leader = group.waitUntilLeaderElected();
        leader.replicate(apply("value1")).get();
        RaftNodeImpl[] followers = group.getNodesExcept(leader.getLocalEndpoint());
        group.dropMessagesToMember(leader.getLocalEndpoint(), followers[0].getLocalEndpoint(), AppendEntriesRequest.class);
        group.dropMessagesToMember(leader.getLocalEndpoint(), followers[1].getLocalEndpoint(), AppendEntriesRequest.class);
        group.dropMessagesToMember(leader.getLocalEndpoint(), followers[2].getLocalEndpoint(), AppendEntriesRequest.class);

        Future<Ordered<Object>> replicateFuture = leader.replicate(apply("value2"));
        Future<Ordered<Object>> queryFuture = leader.query(query(), LINEARIZABLE, 0);

        group.resetAllRulesFrom(leader.getLocalEndpoint());

        Ordered<Object> replicationResult = replicateFuture.get();
        Ordered<Object> queryResult = queryFuture.get();
        assertThat(queryResult.getResult()).isEqualTo("value2");
        assertThat(queryResult.getCommitIndex()).isEqualTo(replicationResult.getCommitIndex());
    }

    @Test(timeout = 300_000)
    public void when_multipleQueriesAreIssuedBeforeHeartbeatAcksReceived_then_allQueriesExecutedAtOnce()
            throws Exception {
        group = newGroup();
        group.start();

        RaftNodeImpl leader = group.waitUntilLeaderElected();
        leader.replicate(apply("value1")).get();
        RaftNodeImpl[] followers = group.getNodesExcept(leader.getLocalEndpoint());
        group.dropMessagesToMember(leader.getLocalEndpoint(), followers[0].getLocalEndpoint(), AppendEntriesRequest.class);
        group.dropMessagesToMember(leader.getLocalEndpoint(), followers[1].getLocalEndpoint(), AppendEntriesRequest.class);
        group.dropMessagesToMember(leader.getLocalEndpoint(), followers[2].getLocalEndpoint(), AppendEntriesRequest.class);

        Future<Ordered<Object>> queryFuture1 = leader.query(query(), LINEARIZABLE, 0);
        Future<Ordered<Object>> queryFuture2 = leader.query(query(), LINEARIZABLE, 0);

        group.resetAllRulesFrom(leader.getLocalEndpoint());

        long commitIndex = getCommitIndex(leader);

        Ordered<Object> result1 = queryFuture1.get();
        Ordered<Object> result2 = queryFuture2.get();
        assertThat(result1.getResult()).isEqualTo("value1");
        assertThat(result2.getResult()).isEqualTo("value1");
        assertThat(result1.getCommitIndex()).isEqualTo(commitIndex);
        assertThat(result2.getCommitIndex()).isEqualTo(commitIndex);
    }

    @Test(timeout = 300_000)
    public void when_newCommitIsDoneWhileThereAreMultipleQueries_then_allQueriesRunAfterCommit()
            throws Exception {
        group = newGroup();
        group.start();

        RaftNodeImpl leader = group.waitUntilLeaderElected();
        leader.replicate(apply("value1")).get();
        RaftNodeImpl[] followers = group.getNodesExcept(leader.getLocalEndpoint());
        group.dropMessagesToMember(leader.getLocalEndpoint(), followers[0].getLocalEndpoint(), AppendEntriesRequest.class);
        group.dropMessagesToMember(leader.getLocalEndpoint(), followers[1].getLocalEndpoint(), AppendEntriesRequest.class);
        group.dropMessagesToMember(leader.getLocalEndpoint(), followers[2].getLocalEndpoint(), AppendEntriesRequest.class);

        Future<Ordered<Object>> replicateFuture = leader.replicate(apply("value2"));
        Future<Ordered<Object>> queryFuture1 = leader.query(query(), LINEARIZABLE, 0);
        Future<Ordered<Object>> queryFuture2 = leader.query(query(), LINEARIZABLE, 0);

        group.resetAllRulesFrom(leader.getLocalEndpoint());

        long commitIndex = replicateFuture.get().getCommitIndex();
        Ordered<Object> queryResult1 = queryFuture1.get();
        Ordered<Object> queryResult2 = queryFuture2.get();
        assertThat(queryResult1.getResult()).isEqualTo("value2");
        assertThat(queryResult2.getResult()).isEqualTo("value2");
        assertThat(queryResult1.getCommitIndex()).isEqualTo(commitIndex);
        assertThat(queryResult2.getCommitIndex()).isEqualTo(commitIndex);
    }

    @Test(timeout = 300_000)
    public void when_linearizableQueryIsIssuedToFollower_then_queryFails()
            throws Exception {
        group = newGroup();
        group.start();

        group.waitUntilLeaderElected();
        try {
            group.getAnyFollowerNode().query(query(), LINEARIZABLE, 0).get();
            fail();
        } catch (ExecutionException e) {
            assertThat(e).hasCauseInstanceOf(NotLeaderException.class);
        }
    }

    @Test(timeout = 300_000)
    public void when_multipleQueryLimitIsReachedBeforeHeartbeatAcks_then_noNewQueryIsAccepted()
            throws Exception {
        RaftConfig config = RaftConfig.newBuilder().setMaxUncommittedLogEntryCount(1).build();
        group = new LocalRaftGroup(5, config, true, null, null, null);
        group.start();

        RaftNodeImpl leader = group.waitUntilLeaderElected();
        eventually(() -> assertThat(getCommitIndex(leader)).isEqualTo(1));

        RaftNodeImpl[] followers = group.getNodesExcept(leader.getLocalEndpoint());
        group.dropMessagesToMember(leader.getLocalEndpoint(), followers[0].getLocalEndpoint(), AppendEntriesRequest.class);
        group.dropMessagesToMember(leader.getLocalEndpoint(), followers[1].getLocalEndpoint(), AppendEntriesRequest.class);
        group.dropMessagesToMember(leader.getLocalEndpoint(), followers[2].getLocalEndpoint(), AppendEntriesRequest.class);

        Future<Ordered<Object>> queryFuture1 = leader.query(query(), LINEARIZABLE, 0);
        Future<Ordered<Object>> queryFuture2 = leader.query(query(), LINEARIZABLE, 0);

        try {
            queryFuture2.get();
            fail();
        } catch (ExecutionException e) {
            assertThat(e).hasCauseInstanceOf(CannotReplicateException.class);
        }

        group.resetAllRulesFrom(leader.getLocalEndpoint());

        queryFuture1.get();
    }

    @Test(timeout = 300_000)
    public void when_leaderDemotesToFollowerWhileThereIsOngoingQuery_then_queryFails()
            throws Exception {
        group = newGroup();
        group.start();

        RaftNodeImpl oldLeader = group.waitUntilLeaderElected();

        int[] majorityNodeIndices = group.createMajoritySplitIndexes(false);
        group.split(majorityNodeIndices);

        Future<Ordered<Object>> queryFuture = oldLeader.query(query(), LINEARIZABLE, 0);

        eventually(() -> {
            for (int ix : majorityNodeIndices) {
                RaftEndpoint newLeader = group.getNode(ix).getLeaderEndpoint();
                assertThat(newLeader).isNotNull();
                assertThat(newLeader).isNotEqualTo(oldLeader.getLocalEndpoint());
            }
        });

        RaftNodeImpl newLeader = group.getNode(group.getNode(majorityNodeIndices[0]).getLeaderEndpoint());
        newLeader.replicate(apply("value1")).get();

        group.merge();
        group.waitUntilLeaderElected();

        Object newLeaderEndpoint = newLeader.getLocalEndpoint();
        assertThat(newLeaderEndpoint).isEqualTo(oldLeader.getLeaderEndpoint());
        try {
            queryFuture.get();
            fail();
        } catch (ExecutionException e) {
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
        group = new LocalRaftGroup(3, config, true, null, null, null);
        group.start();

        RaftNodeImpl leader = group.waitUntilLeaderElected();
        RaftNodeImpl[] followers = group.getNodesExcept(leader.getLocalEndpoint());
        group.dropMessagesToMember(leader.getLocalEndpoint(), followers[1].getLocalEndpoint(), AppendEntriesRequest.class);
        group.dropMessagesToMember(leader.getLocalEndpoint(), followers[1].getLocalEndpoint(), InstallSnapshotRequest.class);

        int i = 0;
        while (getSnapshotEntry(leader).getIndex() == 0) {
            leader.replicate(apply("val" + (++i))).join();
        }

        group.dropMessagesToMember(leader.getLocalEndpoint(), followers[0].getLocalEndpoint(), AppendEntriesRequest.class);
        group.dropMessagesToMember(leader.getLocalEndpoint(), followers[0].getLocalEndpoint(), InstallSnapshotRequest.class);

        CompletableFuture<Ordered<Object>> f = leader.query(query(), LINEARIZABLE, 0);

        eventually(() -> assertThat(getLeaderQuerySeqNo(leader)).isEqualTo(1));

        group.allowAllMessagesToMember(leader.getLocalEndpoint(), followers[0].getLocalEndpoint());
        group.allowAllMessagesToMember(leader.getLocalEndpoint(), followers[1].getLocalEndpoint());

        Ordered<Object> result = f.join();
        assertThat(result.getResult()).isEqualTo("val" + i);
    }

    @Test
    public void when_followerFallsBehind_then_queryIsExecutedOnLeaderWithAppendEntriesFailureResponse() {
        group = new LocalRaftGroup(2, TEST_RAFT_CONFIG, true, null, null, null);
        group.start();

        RaftNodeImpl leader = group.waitUntilLeaderElected();
        RaftNodeImpl[] followers = group.getNodesExcept(leader.getLocalEndpoint());
        RaftNodeImpl follower = followers[0];

        int count = 10;
        for (int i = 0; i < count; i++) {
            leader.replicate(apply("val" + (i + 1))).join();
        }

        RaftNodeImpl newFollower = group.createNewRaftNode();

        group.dropMessagesToMember(leader.getLocalEndpoint(), newFollower.getLocalEndpoint(), AppendEntriesRequest.class);
        group.dropMessagesToMember(newFollower.getLocalEndpoint(), leader.getLocalEndpoint(), AppendEntriesSuccessResponse.class);
        group.dropMessagesToMember(newFollower.getLocalEndpoint(), leader.getLocalEndpoint(), AppendEntriesFailureResponse.class);

        leader.changeMembership(newFollower.getLocalEndpoint(), MembershipChangeMode.ADD, 0).join();

        group.dropMessagesToMember(leader.getLocalEndpoint(), follower.getLocalEndpoint(), AppendEntriesRequest.class);

        CompletableFuture<Ordered<Object>> f = leader.query(query(), LINEARIZABLE, 0);

        group.allowMessagesToMember(newFollower.getLocalEndpoint(), leader.getLocalEndpoint(),
                                    AppendEntriesFailureResponse.class);
        group.allowMessagesToMember(leader.getLocalEndpoint(), newFollower.getLocalEndpoint(), AppendEntriesRequest.class);

        f.join();
    }

}
