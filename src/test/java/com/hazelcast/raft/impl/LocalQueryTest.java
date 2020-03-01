package com.hazelcast.raft.impl;

import com.hazelcast.raft.QueryPolicy;
import com.hazelcast.raft.RaftEndpoint;
import com.hazelcast.raft.exception.NotLeaderException;
import com.hazelcast.raft.impl.local.LocalRaftGroup;
import com.hazelcast.raft.impl.msg.AppendEntriesRequest;
import org.junit.After;
import org.junit.Test;

import java.util.concurrent.ExecutionException;
import java.util.function.Consumer;

import static com.hazelcast.raft.QueryPolicy.LEADER_LOCAL;
import static com.hazelcast.raft.impl.local.RaftDummyService.apply;
import static com.hazelcast.raft.impl.local.RaftDummyService.query;
import static com.hazelcast.raft.impl.util.AssertUtil.eventually;
import static com.hazelcast.raft.impl.util.RaftUtil.getCommitIndex;
import static com.hazelcast.raft.impl.util.RaftUtil.getLeaderMember;
import static com.hazelcast.raft.impl.util.RaftUtil.newGroup;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * @author mdogan
 * @author metanet
 */
public class LocalQueryTest {

    private LocalRaftGroup group;

    @After
    public void destroy() {
        if (group != null) {
            group.destroy();
        }
    }

    @Test(timeout = 300_000)
    public void when_queryFromLeader_withoutAnyCommit_thenReturnDefaultValue()
            throws Exception {
        group = newGroup(3);
        group.start();

        RaftNodeImpl leader = group.waitUntilLeaderElected();

        Object o = leader.query(query(), LEADER_LOCAL).get();
        assertThat(o).isNull();
    }

    @Test(timeout = 300_000)
    public void when_queryFromFollower_withoutAnyCommit_thenReturnDefaultValue()
            throws Exception {
        group = newGroup(3);
        group.start();

        group.waitUntilLeaderElected();
        RaftNodeImpl follower = group.getAnyFollowerNode();

        Object o = follower.query(query(), QueryPolicy.ANY_LOCAL).get();
        assertThat(o).isNull();
    }

    @Test(timeout = 300_000)
    public void when_queryFromLeader_onStableCluster_thenReadLatestValue()
            throws Exception {
        group = newGroup(3);
        group.start();

        RaftNodeImpl leader = group.waitUntilLeaderElected();

        int count = 3;
        for (int i = 1; i <= count; i++) {
            leader.replicate(apply("value" + i)).get();
        }

        Object result = leader.query(query(), LEADER_LOCAL).get();
        assertThat(result).isEqualTo("value" + count);
    }

    @Test(timeout = 300_000)
    public void when_queryFromFollower_withLeaderLocalPolicy_thenFail()
            throws Exception {
        group = newGroup(3);
        group.start();

        RaftNodeImpl leader = group.waitUntilLeaderElected();
        leader.replicate(apply("value")).get();

        RaftNodeImpl follower = group.getAnyFollowerNode();

        try {
            follower.query(query(), LEADER_LOCAL).get();
        } catch (ExecutionException e) {
            assertThat(e).hasCauseInstanceOf(NotLeaderException.class);
        }
    }

    @Test(timeout = 300_000)
    public void when_queryFromFollower_onStableCluster_thenReadLatestValue()
            throws Exception {
        group = newGroup(3);
        group.start();

        RaftNodeImpl leader = group.waitUntilLeaderElected();

        int count = 3;
        for (int i = 1; i <= count; i++) {
            leader.replicate(apply("value" + i)).get();
        }

        RaftNodeImpl[] followers = group.getNodesExcept(leader.getLocalEndpoint());
        Object result0 = followers[0].query(query(), QueryPolicy.ANY_LOCAL).get();
        Object result1 = followers[1].query(query(), QueryPolicy.ANY_LOCAL).get();

        String latestValue = "value" + count;
        assertThat(latestValue).satisfiesAnyOf((Consumer<String>) s -> assertThat(s).isEqualTo(result0),
                (Consumer<String>) s -> assertThat(s).isEqualTo(result1));
    }

    @Test(timeout = 300_000)
    public void when_queryFromSlowFollower_thenReadStaleValue()
            throws Exception {
        group = newGroup(3);
        group.start();

        RaftNodeImpl leader = group.waitUntilLeaderElected();
        RaftNodeImpl slowFollower = group.getAnyFollowerNode();

        Object firstValue = "value1";
        leader.replicate(apply(firstValue)).get();
        long leaderCommitIndex = getCommitIndex(leader);

        eventually(() -> assertThat(getCommitIndex(slowFollower)).isEqualTo(leaderCommitIndex));

        group.dropMessagesToMember(leader.getLocalEndpoint(), slowFollower.getLocalEndpoint(), AppendEntriesRequest.class);

        leader.replicate(apply("value2")).get();

        Object result = slowFollower.query(query(), QueryPolicy.ANY_LOCAL).get();
        assertThat(result).isEqualTo(firstValue);
    }

    @Test(timeout = 300_000)
    public void when_queryFromSlowFollower_thenEventuallyReadLatestValue()
            throws Exception {
        group = newGroup(3);
        group.start();

        RaftNodeImpl leader = group.waitUntilLeaderElected();
        leader.replicate(apply("value1")).get();

        RaftNodeImpl slowFollower = group.getAnyFollowerNode();
        group.dropMessagesToMember(leader.getLocalEndpoint(), slowFollower.getLocalEndpoint(), AppendEntriesRequest.class);

        Object lastValue = "value2";
        leader.replicate(apply(lastValue)).get();

        group.allowAllMessagesToMember(leader.getLocalEndpoint(), slowFollower.getLocalEndpoint());

        eventually(() -> {
            Object result = slowFollower.query(query(), QueryPolicy.ANY_LOCAL).get();
            assertThat(result).isEqualTo(lastValue);
        });
    }

    @Test(timeout = 300_000)
    public void when_queryFromSplitLeader_thenReadStaleValue()
            throws Exception {
        group = newGroup(3);
        group.start();

        RaftNodeImpl leader = group.waitUntilLeaderElected();

        Object firstValue = "value1";
        leader.replicate(apply(firstValue)).get();
        long leaderCommitIndex = getCommitIndex(leader);

        eventually(() -> {
            for (RaftNodeImpl node : group.getNodes()) {
                assertThat(getCommitIndex(node)).isEqualTo(leaderCommitIndex);
            }
        });

        RaftNodeImpl followerNode = group.getAnyFollowerNode();
        group.splitMembers(leader.getLocalEndpoint());

        eventually(() -> {
            RaftEndpoint leaderEndpoint = getLeaderMember(followerNode);
            assertThat(leaderEndpoint).isNotNull().isNotEqualTo(leader.getLocalEndpoint());
        });

        RaftNodeImpl newLeader = group.getNode(getLeaderMember(followerNode));
        Object lastValue = "value2";
        newLeader.replicate(apply(lastValue)).get();

        Object result1 = newLeader.query(query(), QueryPolicy.ANY_LOCAL).get();
        assertThat(result1).isEqualTo(lastValue);

        Object result2 = leader.query(query(), QueryPolicy.ANY_LOCAL).get();
        assertThat(result2).isEqualTo(firstValue);
    }

    @Test(timeout = 300_000)
    public void when_queryFromSplitLeader_thenEventuallyReadLatestValue()
            throws Exception {
        group = newGroup(3);
        group.start();

        RaftNodeImpl leader = group.waitUntilLeaderElected();

        Object firstValue = "value1";
        leader.replicate(apply(firstValue)).get();
        long leaderCommitIndex = getCommitIndex(leader);

        eventually(() -> {
            for (RaftNodeImpl node : group.getNodes()) {
                assertThat(getCommitIndex(node)).isEqualTo(leaderCommitIndex);
            }
        });

        RaftNodeImpl followerNode = group.getAnyFollowerNode();
        group.splitMembers(leader.getLocalEndpoint());

        eventually(() -> {
            RaftEndpoint leaderEndpoint = getLeaderMember(followerNode);
            assertThat(leaderEndpoint).isNotNull().isNotEqualTo(leader.getLocalEndpoint());
        });

        RaftNodeImpl newLeader = group.getNode(getLeaderMember(followerNode));
        Object lastValue = "value2";
        newLeader.replicate(apply(lastValue)).get();

        group.merge();

        eventually(() -> {
            Object result = leader.query(query(), QueryPolicy.ANY_LOCAL).get();
            assertThat(result).isEqualTo(lastValue);
        });
    }
}
