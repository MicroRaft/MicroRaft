package com.hazelcast.raft.impl;

import com.hazelcast.raft.RaftConfig;
import com.hazelcast.raft.RaftEndpoint;
import com.hazelcast.raft.exception.CannotReplicateException;
import com.hazelcast.raft.exception.LeaderDemotedException;
import com.hazelcast.raft.exception.NotLeaderException;
import com.hazelcast.raft.impl.local.LocalRaftGroup;
import com.hazelcast.raft.impl.msg.AppendEntriesRequest;
import com.hazelcast.raft.impl.util.RaftUtil;
import org.junit.After;
import org.junit.Test;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static com.hazelcast.raft.QueryPolicy.LINEARIZABLE;
import static com.hazelcast.raft.RaftConfig.DEFAULT_RAFT_CONFIG;
import static com.hazelcast.raft.impl.local.RaftDummyService.apply;
import static com.hazelcast.raft.impl.local.RaftDummyService.query;
import static com.hazelcast.raft.impl.util.AssertUtil.eventually;
import static com.hazelcast.raft.impl.util.RaftUtil.getCommitIndex;
import static com.hazelcast.raft.impl.util.RaftUtil.getLeaderMember;
import static com.hazelcast.raft.impl.util.RaftUtil.getLeaderQueryRound;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.fail;

/**
 * @author mdogan
 * @author metanet
 */
public class LinearizableQueryTest {

    private LocalRaftGroup group = RaftUtil
            .newGroup(5, RaftConfig.builder().setUncommittedLogEntryCountToRejectNewAppends(1).build(), true);

    @After
    public void destroy() {
        if (group != null) {
            group.destroy();
        }
    }

    @Test(timeout = 300_000)
    public void when_linearizableQueryIsIssued_then_itReadsLastState()
            throws Exception {
        group = newGroup();
        group.start();

        RaftNodeImpl leader = group.waitUntilLeaderElected();
        leader.replicate(apply("value1")).get();
        long commitIndex1 = getCommitIndex(leader);

        Object o1 = leader.query(query(), LINEARIZABLE).get();

        assertThat(o1).isEqualTo("value1");
        long leaderQueryRound1 = getLeaderQueryRound(leader);
        assertThat(leaderQueryRound1).isGreaterThan(0);
        assertThat(getCommitIndex(leader)).isEqualTo(commitIndex1);

        leader.replicate(apply("value2")).get();
        long commitIndex2 = getCommitIndex(leader);

        Object o2 = leader.query(query(), LINEARIZABLE).get();

        assertThat(o2).isEqualTo("value2");
        long leaderQueryRound2 = getLeaderQueryRound(leader);
        assertThat(leaderQueryRound2).isEqualTo(leaderQueryRound1 + 1);
        assertThat(getCommitIndex(leader)).isEqualTo(commitIndex2);
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

        CompletableFuture<Object> replicateFuture = leader.replicate(apply("value2"));
        CompletableFuture<Object> queryFuture = leader.query(query(), LINEARIZABLE);

        group.resetAllRulesFrom(leader.getLocalEndpoint());

        replicateFuture.get();
        Object o = queryFuture.get();
        assertThat(o).isEqualTo("value2");
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

        CompletableFuture<Object> queryFuture1 = leader.query(query(), LINEARIZABLE);
        CompletableFuture<Object> queryFuture2 = leader.query(query(), LINEARIZABLE);

        group.resetAllRulesFrom(leader.getLocalEndpoint());

        assertThat(queryFuture1.get()).isEqualTo("value1");
        assertThat(queryFuture2.get()).isEqualTo("value1");
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

        CompletableFuture<Object> replicateFuture = leader.replicate(apply("value2"));
        CompletableFuture<Object> queryFuture1 = leader.query(query(), LINEARIZABLE);
        CompletableFuture<Object> queryFuture2 = leader.query(query(), LINEARIZABLE);

        group.resetAllRulesFrom(leader.getLocalEndpoint());

        replicateFuture.get();
        assertThat(queryFuture1.get()).isEqualTo("value2");
        assertThat(queryFuture2.get()).isEqualTo("value2");
    }

    @Test(timeout = 300_000)
    public void when_linearizableQueryIsIssuedToFollower_then_queryFails()
            throws Exception {
        group = newGroup();
        group.start();

        group.waitUntilLeaderElected();
        try {
            group.getAnyFollowerNode().query(query(), LINEARIZABLE).get();
            fail();
        } catch (ExecutionException e) {
            assertThat(e).hasCauseInstanceOf(NotLeaderException.class);
        }
    }

    @Test(timeout = 300_000)
    public void when_multipleQueryLimitIsReachedBeforeHeartbeatAcks_then_noNewQueryIsAccepted()
            throws Exception {
        group = RaftUtil.newGroup(5, RaftConfig.builder().setUncommittedLogEntryCountToRejectNewAppends(1).build(), true);
        group.start();

        RaftNodeImpl leader = group.waitUntilLeaderElected();
        eventually(() -> assertThat(getCommitIndex(leader)).isEqualTo(1));

        RaftNodeImpl[] followers = group.getNodesExcept(leader.getLocalEndpoint());
        group.dropMessagesToMember(leader.getLocalEndpoint(), followers[0].getLocalEndpoint(), AppendEntriesRequest.class);
        group.dropMessagesToMember(leader.getLocalEndpoint(), followers[1].getLocalEndpoint(), AppendEntriesRequest.class);
        group.dropMessagesToMember(leader.getLocalEndpoint(), followers[2].getLocalEndpoint(), AppendEntriesRequest.class);

        CompletableFuture<Object> queryFuture1 = leader.query(query(), LINEARIZABLE);
        CompletableFuture<Object> queryFuture2 = leader.query(query(), LINEARIZABLE);

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

        final int[] split = group.createMajoritySplitIndexes(false);
        group.split(split);

        eventually(() -> {
            for (int ix : split) {
                RaftEndpoint newLeader = getLeaderMember(group.getNode(ix));
                assertThat(newLeader).isNotNull();
                assertThat(newLeader).isNotEqualTo(oldLeader.getLocalEndpoint());
            }
        });

        RaftNodeImpl newLeader = group.getNode(getLeaderMember(group.getNode(split[0])));
        newLeader.replicate(apply("value1")).get();

        CompletableFuture<Object> queryFuture = oldLeader.query(query(), LINEARIZABLE);

        group.merge();
        group.waitUntilLeaderElected();

        Object newLeaderEndpoint = newLeader.getLocalEndpoint();
        assertThat(newLeaderEndpoint).isEqualTo(oldLeader.getLeaderEndpoint());
        try {
            queryFuture.get();
            fail();
        } catch (ExecutionException e) {
            assertThat(e).hasCauseInstanceOf(LeaderDemotedException.class);
        }
    }

    private LocalRaftGroup newGroup() {
        return RaftUtil.newGroup(5, DEFAULT_RAFT_CONFIG, true);
    }

}
