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
import io.microraft.RaftEndpoint;
import io.microraft.exception.CannotReplicateException;
import io.microraft.exception.NotLeaderException;
import io.microraft.exception.RaftException;
import io.microraft.impl.local.LocalRaftEndpoint;
import io.microraft.impl.local.LocalRaftGroup;
import io.microraft.model.message.AppendEntriesRequest;
import io.microraft.test.util.BaseTest;
import org.junit.After;
import org.junit.Test;

import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeoutException;

import static io.microraft.MembershipChangeMode.REMOVE;
import static io.microraft.RaftRole.FOLLOWER;
import static io.microraft.impl.local.SimpleStateMachine.apply;
import static io.microraft.test.util.RaftTestUtils.getRole;
import static io.microraft.test.util.RaftTestUtils.getTerm;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * @author mdogan
 * @author metanet
 */
public class LeadershipTransferTest
        extends BaseTest {

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
        group = LocalRaftGroup.start(3);
        RaftNodeImpl leader = group.waitUntilLeaderElected();

        leader.transferLeadership(leader.getLocalEndpoint()).get();
    }

    @Test(timeout = 300_000)
    public void when_leaderTransfersLeadershipToNull_then_leadershipTransferFails()
            throws Exception {
        group = LocalRaftGroup.start(3);
        RaftNodeImpl leader = group.waitUntilLeaderElected();

        try {
            leader.transferLeadership(null).get();
        } catch (ExecutionException e) {
            assertThat(e).hasCauseInstanceOf(IllegalArgumentException.class);
        }
    }

    @Test(timeout = 300_000)
    public void when_leaderTransfersLeadershipToNonGroupMemberEndpoint_then_leadershipTransferFails()
            throws Exception {
        group = LocalRaftGroup.start(3);
        RaftNodeImpl leader = group.waitUntilLeaderElected();
        RaftEndpoint invalidEndpoint = LocalRaftEndpoint.newEndpoint();

        try {
            leader.transferLeadership(invalidEndpoint).get();
        } catch (ExecutionException e) {
            assertThat(e).hasCauseInstanceOf(IllegalArgumentException.class);
        }
    }

    @Test(timeout = 300_000)
    public void when_leadershipTransferTriggeredOnFollower_then_leadershipTransferFails()
            throws Exception {
        group = LocalRaftGroup.start(3);
        RaftNodeImpl leader = group.waitUntilLeaderElected();
        RaftNodeImpl follower = group.getAnyFollower();

        try {
            follower.transferLeadership(leader.getLocalEndpoint()).get();
        } catch (ExecutionException e) {
            assertThat(e).hasCauseInstanceOf(RaftException.class);
        }
    }

    @Test(timeout = 300_000)
    public void when_leadershipTransferTriggeredDuringMembershipChange_then_leadershipTransferFails()
            throws Exception {
        RaftConfig config = RaftConfig.newBuilder().setLeaderHeartbeatPeriodSecs(30).setLeaderHeartbeatTimeoutSecs(30).build();
        group = LocalRaftGroup.start(3, config);
        RaftNodeImpl leader = group.waitUntilLeaderElected();

        leader.replicate(apply("val")).get();

        List<RaftNodeImpl> followers = group.getNodesExcept(leader.getLocalEndpoint());
        group.dropMessagesTo(leader.getLocalEndpoint(), followers.get(0).getLocalEndpoint(), AppendEntriesRequest.class);
        group.dropMessagesTo(leader.getLocalEndpoint(), followers.get(1).getLocalEndpoint(), AppendEntriesRequest.class);

        leader.changeMembership(followers.get(0).getLocalEndpoint(), REMOVE, 0);

        try {
            leader.transferLeadership(followers.get(0).getLocalEndpoint()).get();
        } catch (ExecutionException e) {
            assertThat(e).hasCauseInstanceOf(IllegalStateException.class);
        }
    }

    @Test(timeout = 300_000)
    public void when_leadershipTransferTriggeredDuringNoOperationCommitted_then_leadershipTransferSucceeds()
            throws Exception {
        group = LocalRaftGroup.start(3);
        RaftNodeImpl leader = group.waitUntilLeaderElected();
        int term1 = getTerm(leader);

        RaftNodeImpl follower = group.getAnyFollower();

        leader.transferLeadership(follower.getLocalEndpoint()).get();

        RaftNodeImpl newLeader = group.waitUntilLeaderElected();
        int term2 = getTerm(newLeader);
        assertThat(newLeader).isNotSameAs(leader);
        assertThat(term2).isGreaterThan(term1);
        assertThat(getRole(leader)).isEqualTo(FOLLOWER);
    }

    @Test(timeout = 300_000)
    public void when_leadershipTransferTriggeredDuringOperationsCommitted_then_leadershipTransferSucceeds()
            throws Exception {
        group = LocalRaftGroup.start(3);
        RaftNodeImpl leader = group.waitUntilLeaderElected();
        int term1 = getTerm(leader);

        leader.replicate(apply("val")).get();

        RaftNodeImpl follower = group.getAnyFollower();
        leader.transferLeadership(follower.getLocalEndpoint()).get();

        RaftNodeImpl newLeader = group.waitUntilLeaderElected();
        int term2 = getTerm(newLeader);
        assertThat(newLeader).isNotSameAs(leader);
        assertThat(term2).isGreaterThan(term1);
        assertThat(getRole(leader)).isEqualTo(FOLLOWER);
    }

    @Test(timeout = 300_000)
    public void when_targetEndpointCannotCatchesUpTheLeaderInTime_then_leadershipTransferFails()
            throws Exception {
        group = LocalRaftGroup.start(3);
        RaftNodeImpl leader = group.waitUntilLeaderElected();

        RaftNodeImpl follower = group.getAnyFollower();
        group.dropMessagesTo(leader.getLocalEndpoint(), follower.getLocalEndpoint(), AppendEntriesRequest.class);

        leader.replicate(apply("val")).get();

        try {
            leader.transferLeadership(follower.getLocalEndpoint()).get();
        } catch (ExecutionException e) {
            assertThat(e).hasCauseInstanceOf(TimeoutException.class);
        }
    }

    @Test(timeout = 300_000)
    public void when_sameLeadershipTransferTriggeredMultipleTimes_then_leadershipTransferSucceeds()
            throws Exception {
        group = LocalRaftGroup.start(3);
        RaftNodeImpl leader = group.waitUntilLeaderElected();
        RaftNodeImpl follower = group.getAnyFollower();
        group.dropMessagesTo(leader.getLocalEndpoint(), follower.getLocalEndpoint(), AppendEntriesRequest.class);

        leader.replicate(apply("val")).get();

        Future<Ordered<Object>> f1 = leader.transferLeadership(follower.getLocalEndpoint());
        Future<Ordered<Object>> f2 = leader.transferLeadership(follower.getLocalEndpoint());

        group.allowAllMessagesTo(leader.getLocalEndpoint(), follower.getLocalEndpoint());

        f1.get();
        f2.get();
    }

    @Test(timeout = 300_000)
    public void when_secondLeadershipTransfersTriggeredForDifferentEndpoint_then_secondLeadershipTransferFails()
            throws Exception {
        group = LocalRaftGroup.start(3);
        RaftNodeImpl leader = group.waitUntilLeaderElected();

        List<RaftNodeImpl> followers = group.getNodesExcept(leader.getLocalEndpoint());
        RaftNodeImpl follower1 = followers.get(0);
        RaftNodeImpl follower2 = followers.get(1);
        group.dropMessagesTo(leader.getLocalEndpoint(), follower1.getLocalEndpoint(), AppendEntriesRequest.class);

        leader.replicate(apply("val")).get();

        leader.transferLeadership(follower1.getLocalEndpoint());

        try {
            leader.transferLeadership(follower2.getLocalEndpoint()).get();
        } catch (ExecutionException e) {
            assertThat(e).hasCauseInstanceOf(IllegalStateException.class);
        }
    }

    @Test(timeout = 300_000)
    public void when_newOperationIsReplicatedDuringLeadershipTransfer_then_replicateFails()
            throws Exception {
        group = LocalRaftGroup.start(3);
        RaftNodeImpl leader = group.waitUntilLeaderElected();
        RaftNodeImpl follower = group.getAnyFollower();
        group.dropMessagesTo(leader.getLocalEndpoint(), follower.getLocalEndpoint(), AppendEntriesRequest.class);

        leader.replicate(apply("val")).get();
        leader.transferLeadership(follower.getLocalEndpoint());

        try {
            leader.replicate(apply("val")).get();
        } catch (ExecutionException e) {
            assertThat(e).hasCauseInstanceOf(CannotReplicateException.class);
        }
    }

    @Test(timeout = 300_000)
    public void when_leadershipTransferCompleted_then_oldLeaderCannotReplicate()
            throws Exception {
        group = LocalRaftGroup.start(3);
        RaftNodeImpl leader = group.waitUntilLeaderElected();
        RaftNodeImpl follower = group.getAnyFollower();

        leader.transferLeadership(follower.getLocalEndpoint()).get();

        RaftNodeImpl newLeader = group.waitUntilLeaderElected();
        assertThat(newLeader).isNotSameAs(leader);

        try {
            leader.replicate(apply("val")).get();
        } catch (ExecutionException e) {
            assertThat(e).hasCauseInstanceOf(NotLeaderException.class);
        }
    }

    @Test(timeout = 300_000)
    public void when_leadershipTransferCompleted_then_newLeaderCanCommit()
            throws Exception {
        group = LocalRaftGroup.start(3);
        RaftNodeImpl leader = group.waitUntilLeaderElected();
        RaftNodeImpl follower = group.getAnyFollower();

        leader.transferLeadership(follower.getLocalEndpoint()).get();

        RaftNodeImpl newLeader = group.waitUntilLeaderElected();
        assertThat(newLeader).isNotSameAs(leader);

        newLeader.replicate(apply("val")).get();
    }

    @Test(timeout = 300_000)
    public void when_thereAreInflightOperationsDuringLeadershipTransfer_then_inflightOperationsAreCommitted()
            throws Exception {
        group = LocalRaftGroup.start(3);
        RaftNodeImpl leader = group.waitUntilLeaderElected();

        List<RaftNodeImpl> followers = group.getNodesExcept(leader.getLocalEndpoint());

        group.dropMessagesTo(leader.getLocalEndpoint(), followers.get(0).getLocalEndpoint(), AppendEntriesRequest.class);
        group.dropMessagesTo(leader.getLocalEndpoint(), followers.get(1).getLocalEndpoint(), AppendEntriesRequest.class);

        Future<Ordered<Object>> f1 = leader.replicate(apply("val"));
        Future<Ordered<Object>> f2 = leader.transferLeadership(followers.get(0).getLocalEndpoint());
        group.allowAllMessagesTo(leader.getLocalEndpoint(), followers.get(0).getLocalEndpoint());
        group.allowAllMessagesTo(leader.getLocalEndpoint(), followers.get(1).getLocalEndpoint());

        f2.get();

        RaftNodeImpl newLeader = group.waitUntilLeaderElected();
        assertThat(newLeader).isNotSameAs(leader);

        f1.get();
    }

}
