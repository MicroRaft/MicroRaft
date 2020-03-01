package com.hazelcast.raft.impl;

import com.hazelcast.raft.MembershipChangeMode;
import com.hazelcast.raft.RaftConfig;
import com.hazelcast.raft.RaftEndpoint;
import com.hazelcast.raft.exception.CannotReplicateException;
import com.hazelcast.raft.exception.NotLeaderException;
import com.hazelcast.raft.impl.local.LocalRaftGroup;
import com.hazelcast.raft.impl.local.Nop;
import com.hazelcast.raft.impl.msg.AppendEntriesRequest;
import org.junit.After;
import org.junit.Test;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeoutException;

import static com.hazelcast.raft.RaftRole.FOLLOWER;
import static com.hazelcast.raft.impl.local.LocalRaftEndpoint.newEndpoint;
import static com.hazelcast.raft.impl.util.RaftUtil.getRole;
import static com.hazelcast.raft.impl.util.RaftUtil.getTerm;
import static com.hazelcast.raft.impl.util.RaftUtil.newGroup;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * @author mdogan
 * @author metanet
 */
public class LeadershipTransferTest {

    private LocalRaftGroup group;

    @After
    public void destroy() {
        if (group != null) {
            group.destroy();
        }
    }

    @Test(timeout = 300_000)
    public void when_leaderTransfersLeadershipToItself_then_leadershipTransferSucceeds()
            throws Exception {
        group = newGroup(3);
        group.start();
        RaftNodeImpl leader = group.waitUntilLeaderElected();

        leader.transferLeadership(leader.getLocalEndpoint()).get();
    }

    @Test(timeout = 300_000)
    public void when_leaderTransfersLeadershipToNull_then_leadershipTransferFails()
            throws Exception {
        group = newGroup(3);
        group.start();
        RaftNodeImpl leader = group.waitUntilLeaderElected();

        Future future = leader.transferLeadership(null);

        try {
            future.get();
        } catch (ExecutionException e) {
            assertThat(e).hasCauseInstanceOf(IllegalArgumentException.class);
        }
    }

    @Test(timeout = 300_000)
    public void when_leaderTransfersLeadershipToNonGroupMemberEndpoint_then_leadershipTransferFails()
            throws Exception {
        group = newGroup(3);
        group.start();
        RaftNodeImpl leader = group.waitUntilLeaderElected();

        RaftEndpoint invalidEndpoint = newEndpoint();
        Future future = leader.transferLeadership(invalidEndpoint);

        try {
            future.get();
        } catch (ExecutionException e) {
            assertThat(e).hasCauseInstanceOf(IllegalArgumentException.class);
        }
    }

    @Test(timeout = 300_000)
    public void when_leadershipTransferTriggeredOnFollower_then_leadershipTransferFails()
            throws Exception {
        group = newGroup(3);
        group.start();
        RaftNodeImpl leader = group.waitUntilLeaderElected();

        RaftNodeImpl follower = group.getNodesExcept(leader.getLocalEndpoint())[0];
        Future f = follower.transferLeadership(leader.getLocalEndpoint());

        try {
            f.get();
        } catch (ExecutionException e) {
            assertThat(e).hasCauseInstanceOf(IllegalStateException.class);
        }
    }

    @Test(timeout = 300_000)
    public void when_leadershipTransferTriggeredDuringMembershipChange_then_leadershipTransferFails()
            throws Exception {
        RaftConfig config = RaftConfig.builder().setLeaderHeartbeatPeriodMs(SECONDS.toMillis(30)).build();
        group = newGroup(3, config);
        group.start();
        RaftNodeImpl leader = group.waitUntilLeaderElected();

        leader.replicate(new Nop()).get();
        RaftNodeImpl[] followers = group.getNodesExcept(leader.getLocalEndpoint());
        group.dropMessagesToMember(leader.getLocalEndpoint(), followers[0].getLocalEndpoint(), AppendEntriesRequest.class);
        group.dropMessagesToMember(leader.getLocalEndpoint(), followers[1].getLocalEndpoint(), AppendEntriesRequest.class);

        leader.changeMembership(followers[0].getLocalEndpoint(), MembershipChangeMode.REMOVE, 0);
        Future f = leader.transferLeadership(followers[0].getLocalEndpoint());

        try {
            f.get();
        } catch (ExecutionException e) {
            assertThat(e).hasCauseInstanceOf(IllegalStateException.class);
        }
    }

    @Test(timeout = 300_000)
    public void when_leadershipTransferTriggeredDuringNoOperationCommitted_then_leadershipTransferSucceeds()
            throws Exception {
        group = newGroup(3);
        group.start();
        RaftNodeImpl leader = group.waitUntilLeaderElected();
        int term1 = getTerm(leader);

        RaftNodeImpl follower = group.getNodesExcept(leader.getLocalEndpoint())[0];
        Future f = leader.transferLeadership(follower.getLocalEndpoint());

        f.get();

        RaftNodeImpl newLeader = group.waitUntilLeaderElected();
        int term2 = getTerm(newLeader);
        assertThat(newLeader).isNotSameAs(leader);
        assertThat(term2).isGreaterThan(term1);
        assertThat(getRole(leader)).isEqualTo(FOLLOWER);
    }

    @Test(timeout = 300_000)
    public void when_leadershipTransferTriggeredDuringOperationsCommitted_then_leadershipTransferSucceeds()
            throws Exception {
        group = newGroup(3);
        group.start();
        RaftNodeImpl leader = group.waitUntilLeaderElected();
        int term1 = getTerm(leader);

        leader.replicate(new Nop()).get();

        RaftNodeImpl follower = group.getNodesExcept(leader.getLocalEndpoint())[0];
        Future f = leader.transferLeadership(follower.getLocalEndpoint());

        f.get();

        RaftNodeImpl newLeader = group.waitUntilLeaderElected();
        int term2 = getTerm(newLeader);
        assertThat(newLeader).isNotSameAs(leader);
        assertThat(term2).isGreaterThan(term1);
        assertThat(getRole(leader)).isEqualTo(FOLLOWER);
    }

    @Test(timeout = 300_000)
    public void when_targetEndpointCannotCatchesUpTheLeaderInTime_then_leadershipTransferFails()
            throws Exception {
        group = newGroup(3);
        group.start();
        RaftNodeImpl leader = group.waitUntilLeaderElected();

        RaftNodeImpl follower = group.getNodesExcept(leader.getLocalEndpoint())[0];
        group.dropMessagesToMember(leader.getLocalEndpoint(), follower.getLocalEndpoint(), AppendEntriesRequest.class);

        leader.replicate(new Nop()).get();

        Future f = leader.transferLeadership(follower.getLocalEndpoint());

        try {
            f.get();
        } catch (ExecutionException e) {
            assertThat(e).hasCauseInstanceOf(TimeoutException.class);
        }
    }

    @Test(timeout = 300_000)
    public void when_sameLeadershipTransferTriggeredMultipleTimes_then_leadershipTransferSucceeds()
            throws Exception {
        group = newGroup(3);
        group.start();
        RaftNodeImpl leader = group.waitUntilLeaderElected();

        RaftNodeImpl follower = group.getNodesExcept(leader.getLocalEndpoint())[0];
        group.dropMessagesToMember(leader.getLocalEndpoint(), follower.getLocalEndpoint(), AppendEntriesRequest.class);

        leader.replicate(new Nop()).get();

        Future f1 = leader.transferLeadership(follower.getLocalEndpoint());
        Future f2 = leader.transferLeadership(follower.getLocalEndpoint());

        group.allowAllMessagesToMember(leader.getLocalEndpoint(), follower.getLocalEndpoint());

        f1.get();
        f2.get();
    }

    @Test(timeout = 300_000)
    public void when_secondLeadershipTransfersTriggeredForDifferentEndpoint_then_secondLeadershipTransferFails()
            throws Exception {
        group = newGroup(3);
        group.start();
        RaftNodeImpl leader = group.waitUntilLeaderElected();

        RaftNodeImpl[] followers = group.getNodesExcept(leader.getLocalEndpoint());
        RaftNodeImpl follower1 = followers[0];
        RaftNodeImpl follower2 = followers[1];
        group.dropMessagesToMember(leader.getLocalEndpoint(), follower1.getLocalEndpoint(), AppendEntriesRequest.class);

        leader.replicate(new Nop()).get();

        leader.transferLeadership(follower1.getLocalEndpoint());
        Future f = leader.transferLeadership(follower2.getLocalEndpoint());

        try {
            f.get();
        } catch (ExecutionException e) {
            assertThat(e).hasCauseInstanceOf(IllegalStateException.class);
        }
    }

    @Test(timeout = 300_000)
    public void when_newOperationIsReplicatedDuringLeadershipTransfer_then_replicateFails()
            throws Exception {
        group = newGroup(3);
        group.start();
        RaftNodeImpl leader = group.waitUntilLeaderElected();

        RaftNodeImpl follower = group.getNodesExcept(leader.getLocalEndpoint())[0];
        group.dropMessagesToMember(leader.getLocalEndpoint(), follower.getLocalEndpoint(), AppendEntriesRequest.class);

        leader.replicate(new Nop()).get();
        leader.transferLeadership(follower.getLocalEndpoint());

        Future f = leader.replicate(new Nop());

        try {
            f.get();
        } catch (ExecutionException e) {
            assertThat(e).hasCauseInstanceOf(CannotReplicateException.class);
        }
    }

    @Test(timeout = 300_000)
    public void when_leadershipTransferCompleted_then_oldLeaderCannotReplicate()
            throws Exception {
        group = newGroup(3);
        group.start();
        RaftNodeImpl leader = group.waitUntilLeaderElected();

        RaftNodeImpl follower = group.getNodesExcept(leader.getLocalEndpoint())[0];

        leader.transferLeadership(follower.getLocalEndpoint()).get();

        RaftNodeImpl newLeader = group.waitUntilLeaderElected();
        assertThat(newLeader).isNotSameAs(leader);

        Future f = leader.replicate(new Nop());

        try {
            f.get();
        } catch (ExecutionException e) {
            assertThat(e).hasCauseInstanceOf(NotLeaderException.class);
        }
    }

    @Test(timeout = 300_000)
    public void when_leadershipTransferCompleted_then_newLeaderCanCommit()
            throws Exception {
        group = newGroup(3);
        group.start();
        RaftNodeImpl leader = group.waitUntilLeaderElected();

        RaftNodeImpl follower = group.getNodesExcept(leader.getLocalEndpoint())[0];

        leader.transferLeadership(follower.getLocalEndpoint()).get();

        RaftNodeImpl newLeader = group.waitUntilLeaderElected();
        assertThat(newLeader).isNotSameAs(leader);

        Future f = newLeader.replicate(new Nop());
        f.get();
    }

    @Test(timeout = 300_000)
    public void when_thereAreInflightOperationsDuringLeadershipTransfer_then_inflightOperationsAreCommitted()
            throws Exception {
        group = newGroup(3);
        group.start();
        RaftNodeImpl leader = group.waitUntilLeaderElected();

        RaftNodeImpl[] followers = group.getNodesExcept(leader.getLocalEndpoint());

        group.dropMessagesToMember(leader.getLocalEndpoint(), followers[0].getLocalEndpoint(), AppendEntriesRequest.class);
        group.dropMessagesToMember(leader.getLocalEndpoint(), followers[1].getLocalEndpoint(), AppendEntriesRequest.class);

        Future f1 = leader.replicate(new Nop());
        Future f2 = leader.transferLeadership(followers[0].getLocalEndpoint());
        group.allowAllMessagesToMember(leader.getLocalEndpoint(), followers[0].getLocalEndpoint());
        group.allowAllMessagesToMember(leader.getLocalEndpoint(), followers[1].getLocalEndpoint());

        f2.get();

        RaftNodeImpl newLeader = group.waitUntilLeaderElected();
        assertThat(newLeader).isNotSameAs(leader);

        f1.get();
    }

}
