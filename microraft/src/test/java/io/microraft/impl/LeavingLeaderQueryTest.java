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

import static io.microraft.MembershipChangeMode.REMOVE_MEMBER;
import static io.microraft.QueryPolicy.EVENTUAL_CONSISTENCY;
import static io.microraft.QueryPolicy.LEADER_LEASE;
import static io.microraft.QueryPolicy.LINEARIZABLE;
import static io.microraft.RaftRole.LEADER;
import static io.microraft.impl.local.SimpleStateMachine.applyValue;
import static io.microraft.impl.local.SimpleStateMachine.queryLastValue;
import static io.microraft.test.util.RaftTestUtils.getEffectiveGroupMembers;
import static io.microraft.test.util.RaftTestUtils.getRole;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import org.junit.After;
import org.junit.Test;

import io.microraft.Ordered;
import io.microraft.QueryPolicy;
import io.microraft.RaftConfig;
import io.microraft.exception.NotLeaderException;
import io.microraft.impl.local.LocalRaftGroup;
import io.microraft.model.message.AppendEntriesRequest;
import io.microraft.report.RaftGroupMembers;
import io.microraft.test.util.BaseTest;

public class LeavingLeaderQueryTest extends BaseTest {

    private static final int ASSERT_TIMEOUT_SECONDS = 20;
    private static final int HEARTBEAT_TIMEOUT_SECONDS = 60;

    private LocalRaftGroup group;

    @After
    public void destroy() {
        if (group != null) {
            group.destroy();
        }
    }

    @Test(timeout = 300_000)
    public void whenLeaderIsLeavingThenLinearizableQueryIsRejected() throws Exception {
        assertNewQueryRejected(3, LINEARIZABLE);
    }

    @Test(timeout = 300_000)
    public void whenLeaderIsLeavingThenLeaseQueryIsRejected() throws Exception {
        assertNewQueryRejected(3, LEADER_LEASE);
    }

    @Test(timeout = 300_000)
    public void whenRemovalLeavesSingleVoterThenLinearizableQueryIsRejected() throws Exception {
        assertNewQueryRejected(2, LINEARIZABLE);
    }

    @Test(timeout = 300_000)
    public void whenRemovalLeavesSingleVoterThenLeaseQueryIsRejected() throws Exception {
        assertNewQueryRejected(2, LEADER_LEASE);
    }

    private void assertNewQueryRejected(int groupSize, QueryPolicy policy) throws Exception {
        RaftNodeImpl leader = startGroup(groupSize);
        blockAppendRequests(leader);
        CompletableFuture<Ordered<RaftGroupMembers>> removal = leader.changeMembership(
                leader.getLocalEndpoint(), REMOVE_MEMBER, 0);
        CompletableFuture<Ordered<Object>> query = leader.query(queryLastValue(), policy,
                Optional.empty(), Optional.empty());

        // Drain both API tasks without waiting for replication or a heartbeat timeout.
        leader.getReport().get(ASSERT_TIMEOUT_SECONDS, SECONDS);
        assertRejected(query);
        assertThat(getEffectiveGroupMembers(leader).isVotingMember(leader.getLocalEndpoint())).isFalse();
        assertThat(getRole(leader)).isEqualTo(LEADER);
        assertThat(removal).isNotDone();
        assertThat(leader.query(queryLastValue(), EVENTUAL_CONSISTENCY, Optional.empty(), Optional.empty())
                .get(ASSERT_TIMEOUT_SECONDS, SECONDS).getResult()).isEqualTo("value");

        group.resetAllRulesFrom(leader.getLocalEndpoint());
        assertThat(removal.get(ASSERT_TIMEOUT_SECONDS, SECONDS).getResult().getMembers())
                .doesNotContain(leader.getLocalEndpoint());
    }

    @Test(timeout = 300_000)
    public void whenLeaderStartsLeavingThenPendingLinearizableQueryIsRejected() throws Exception {
        RaftNodeImpl leader = startGroup(3);
        blockAppendRequests(leader);
        CompletableFuture<Ordered<Object>> query = leader.query(queryLastValue(), LINEARIZABLE,
                Optional.empty(), Optional.empty());
        leader.getReport().get(ASSERT_TIMEOUT_SECONDS, SECONDS);
        assertThat(query).isNotDone();

        CompletableFuture<Ordered<RaftGroupMembers>> removal = leader.changeMembership(
                leader.getLocalEndpoint(), REMOVE_MEMBER, 0);
        leader.getReport().get(ASSERT_TIMEOUT_SECONDS, SECONDS);
        assertRejected(query);
        assertThat(getRole(leader)).isEqualTo(LEADER);
        assertThat(removal).isNotDone();

        // Late acknowledgements must not revive the rejected query or block removal.
        group.resetAllRulesFrom(leader.getLocalEndpoint());
        removal.get(ASSERT_TIMEOUT_SECONDS, SECONDS);
        assertRejected(query);
    }

    @Test(timeout = 300_000)
    public void whenAnotherMemberIsLeavingThenLeaderQueriesRemainAvailable() throws Exception {
        RaftNodeImpl leader = startGroup(3);
        List<RaftNodeImpl> followers = group.getNodesExcept(leader.getLocalEndpoint());
        for (RaftNodeImpl follower : followers) {
            group.alterMessagesTo(leader.getLocalEndpoint(), follower.getLocalEndpoint(), message -> {
                if (!(message instanceof AppendEntriesRequest)) {
                    return message;
                }
                // Deliver heartbeats and query rounds, but hold back the removal entry.
                AppendEntriesRequest request = (AppendEntriesRequest) message;
                return leader.getModelFactory().createAppendEntriesRequestBuilder()
                        .setGroupId(request.getGroupId()).setSender(request.getSender()).setTerm(request.getTerm())
                        .setPreviousLogTerm(request.getPreviousLogTerm())
                        .setPreviousLogIndex(request.getPreviousLogIndex())
                        .setCommitIndex(request.getCommitIndex()).setLogEntries(List.of())
                        .setQuerySequenceNumber(request.getQuerySequenceNumber())
                        .setFlowControlSequenceNumber(request.getFlowControlSequenceNumber()).build();
            });
        }
        CompletableFuture<Ordered<RaftGroupMembers>> removal = leader.changeMembership(
                followers.get(0).getLocalEndpoint(), REMOVE_MEMBER, 0);
        for (QueryPolicy policy : List.of(LINEARIZABLE, LEADER_LEASE)) {
            assertThat(leader.query(queryLastValue(), policy, Optional.empty(), Optional.empty())
                    .get(ASSERT_TIMEOUT_SECONDS, SECONDS).getResult()).isEqualTo("value");
        }
        assertThat(removal).isNotDone();
        group.resetAllRulesFrom(leader.getLocalEndpoint());
        removal.get(ASSERT_TIMEOUT_SECONDS, SECONDS);
    }

    private RaftNodeImpl startGroup(int size) throws Exception {
        RaftConfig config = RaftConfig.newBuilder().setLeaderHeartbeatPeriodSecs(1)
                .setLeaderHeartbeatTimeoutSecs(HEARTBEAT_TIMEOUT_SECONDS).build();
        group = LocalRaftGroup.newBuilder(size).setConfig(config).enableNewTermOperation().start();
        RaftNodeImpl leader = group.waitUntilLeaderElected();
        leader.replicate(applyValue("value")).get(ASSERT_TIMEOUT_SECONDS, SECONDS);
        return leader;
    }

    private void blockAppendRequests(RaftNodeImpl leader) {
        for (RaftNodeImpl follower : group.<RaftNodeImpl>getNodesExcept(leader.getLocalEndpoint())) {
            group.dropMessagesTo(leader.getLocalEndpoint(), follower.getLocalEndpoint(), AppendEntriesRequest.class);
        }
    }

    private static void assertRejected(CompletableFuture<?> query) {
        assertThat(query).isCompletedExceptionally();
        assertThatThrownBy(query::join).hasCauseInstanceOf(NotLeaderException.class)
                .satisfies(error -> assertThat(((NotLeaderException) error.getCause()).getLeader()).isNull());
    }
}
