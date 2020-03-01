package com.hazelcast.raft.impl;

import com.hazelcast.raft.RaftEndpoint;
import com.hazelcast.raft.RaftNodeStatus;
import com.hazelcast.raft.exception.CannotReplicateException;
import com.hazelcast.raft.exception.RaftGroupTerminatedException;
import com.hazelcast.raft.impl.local.LocalRaftGroup;
import com.hazelcast.raft.impl.msg.AppendEntriesRequest;
import org.junit.After;
import org.junit.Test;

import java.util.concurrent.ExecutionException;

import static com.hazelcast.raft.impl.local.RaftDummyService.apply;
import static com.hazelcast.raft.impl.util.AssertUtil.eventually;
import static com.hazelcast.raft.impl.util.RaftUtil.getLeaderMember;
import static com.hazelcast.raft.impl.util.RaftUtil.getStatus;
import static com.hazelcast.raft.impl.util.RaftUtil.newGroup;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.fail;

/**
 * @author mdogan
 * @author metanet
 */
public class TerminateRaftGroupTest {

    private LocalRaftGroup group;

    @After
    public void destroy() {
        if (group != null) {
            group.destroy();
        }
    }

    @Test(timeout = 300_000)
    public void when_terminateOpIsAppendedButNotCommitted_then_cannotAppendNewEntry()
            throws Exception {
        group = newGroup(2);
        group.start();

        RaftNodeImpl leader = group.waitUntilLeaderElected();
        RaftNodeImpl follower = group.getAnyFollowerNode();

        group.dropAllMessagesToMember(leader.getLocalEndpoint(), follower.getLocalEndpoint());

        leader.terminateGroup();

        try {
            leader.replicate(apply("val")).get();
            fail();
        } catch (ExecutionException e) {
            assertThat(e).hasCauseInstanceOf(CannotReplicateException.class);
        }
    }

    @Test(timeout = 300_000)
    public void when_terminateOpIsAppended_then_statusIsTerminating() {
        group = newGroup(2);
        group.start();

        RaftNodeImpl leader = group.waitUntilLeaderElected();
        RaftNodeImpl follower = group.getAnyFollowerNode();

        group.dropAllMessagesToMember(follower.getLocalEndpoint(), leader.getLocalEndpoint());

        leader.terminateGroup();

        eventually(() -> {
            assertThat(getStatus(leader)).isEqualTo(RaftNodeStatus.TERMINATING);
            assertThat(getStatus(follower)).isEqualTo(RaftNodeStatus.TERMINATING);
        });
    }

    @Test(timeout = 300_000)
    public void when_terminateOpIsCommitted_then_raftNodeIsTerminated()
            throws Exception {
        group = newGroup(2);
        group.start();

        RaftNodeImpl leader = group.waitUntilLeaderElected();
        RaftNodeImpl follower = group.getAnyFollowerNode();

        leader.terminateGroup().get();

        eventually(() -> {
            assertThat(leader.state().commitIndex()).isEqualTo(1);
            assertThat(follower.state().commitIndex()).isEqualTo(1);
            assertThat(leader.getStatus()).isEqualTo(RaftNodeStatus.TERMINATED);
            assertThat(follower.getStatus()).isEqualTo(RaftNodeStatus.TERMINATED);
        });

        try {
            leader.replicate(apply("val")).get();
            fail();
        } catch (ExecutionException e) {
            assertThat(e).hasCauseInstanceOf(RaftGroupTerminatedException.class);
        }

        try {
            follower.replicate(apply("val")).get();
            fail();
        } catch (ExecutionException e) {
            assertThat(e).hasCauseInstanceOf(RaftGroupTerminatedException.class);
        }
    }

    @Test(timeout = 300_000)
    public void when_terminateOpIsTruncated_then_statusIsActive()
            throws Exception {
        group = newGroup(3);
        group.start();

        RaftNodeImpl leader = group.waitUntilLeaderElected();
        RaftNodeImpl[] followers = group.getNodesExcept(leader.getLocalEndpoint());

        group.dropMessagesToAll(leader.getLocalEndpoint(), AppendEntriesRequest.class);

        leader.terminateGroup();

        group.splitMembers(leader.getLocalEndpoint());

        eventually(() -> {
            for (RaftNodeImpl raftNode : followers) {
                RaftEndpoint leaderEndpoint = getLeaderMember(raftNode);
                assertThat(leaderEndpoint).isNotNull().isNotEqualTo(leader.getLocalEndpoint());
            }
        });

        RaftNodeImpl newLeader = group.getNode(getLeaderMember(followers[0]));

        for (int i = 0; i < 10; i++) {
            newLeader.replicate(apply("val" + i)).get();
        }

        group.merge();

        eventually(() -> {
            for (RaftNodeImpl raftNode : group.getNodes()) {
                assertThat(getStatus(raftNode)).isEqualTo(RaftNodeStatus.ACTIVE);
            }
        });
    }

}
