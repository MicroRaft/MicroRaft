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

import static io.microraft.RaftRole.LEADER;
import static io.microraft.impl.local.SimpleStateMachine.applyValue;
import static io.microraft.test.util.AssertionUtils.eventually;
import static io.microraft.test.util.RaftTestUtils.getCommitIndex;
import static io.microraft.test.util.RaftTestUtils.getLastLogOrSnapshotEntry;
import static io.microraft.test.util.RaftTestUtils.getRole;
import static io.microraft.test.util.RaftTestUtils.getTerm;
import static io.microraft.test.util.RaftTestUtils.readRaftState;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;

import org.junit.After;
import org.junit.Test;

import io.microraft.Ordered;
import io.microraft.RaftConfig;
import io.microraft.RaftEndpoint;
import io.microraft.impl.local.LocalRaftGroup;
import io.microraft.model.message.AppendEntriesRequest;
import io.microraft.model.message.AppendEntriesSuccessResponse;
import io.microraft.model.message.PreVoteRequest;
import io.microraft.model.message.VoteRequest;
import io.microraft.test.util.BaseTest;

public class StaleTermResponseTest extends BaseTest {

    private static final RaftConfig CONFIG = RaftConfig.newBuilder().setLeaderElectionTimeoutMillis(500)
            .setLeaderHeartbeatPeriodSecs(1).setLeaderHeartbeatTimeoutSecs(3).build();

    private LocalRaftGroup group;
    private RaftNodeImpl nodeA;
    private RaftNodeImpl nodeB;
    private RaftNodeImpl nodeC;

    @After
    public void destroy() {
        if (group != null) {
            group.destroy();
        }
    }

    @Test(timeout = 300_000)
    public void when_delayedSuccessResponseFromEarlierTermArrives_then_leaderDoesNotCommitEntryFollowerLacks() {
        group = LocalRaftGroup.newBuilder(3).setConfig(CONFIG).start();
        nodeA = group.waitUntilLeaderElected();
        nodeA.replicate(applyValue("1a")).join();
        long base = getCommitIndex(nodeA);

        // A appends three entries in its term but replicates none of them.
        List<RaftNodeImpl> others = group.getNodesExcept(nodeA.getLocalEndpoint());
        others.forEach(other -> group.dropMessagesTo(nodeA.getLocalEndpoint(), other.getLocalEndpoint(),
                AppendEntriesRequest.class));
        Stream.of("1b", "1c", "1d").forEach(value -> nodeA.replicate(applyValue(value)));
        eventually(() -> assertThat(lastIndex(nodeA)).isEqualTo(base + 3));

        // B and C elect one of themselves, call it B, which appends an entry it can't replicate. A only hears their
        // vote requests, which keep its term current.
        silence(nodeA);
        group.dropMessagesTo(others.get(0).getLocalEndpoint(), others.get(1).getLocalEndpoint(),
                AppendEntriesRequest.class);
        group.dropMessagesTo(others.get(1).getLocalEndpoint(), others.get(0).getLocalEndpoint(),
                AppendEntriesRequest.class);
        eventually(() -> assertThat(others.stream().filter(node -> getRole(node) == LEADER)).hasSize(1));
        nodeB = others.stream().filter(node -> getRole(node) == LEADER).findFirst().orElseThrow();
        nodeC = others.stream().filter(node -> node != nodeB).findFirst().orElseThrow();
        nodeB.replicate(applyValue("2"));
        eventually(() -> assertThat(lastIndex(nodeB)).isEqualTo(base + 1));
        int termOfB = getTerm(nodeB);

        // A wins a newer term with C's vote and copies 1b..1d to C. C's ack for them is held back. B only hears
        // vote requests.
        resetAll();
        silence(nodeB);
        AtomicReference<AppendEntriesSuccessResponse> delayed = new AtomicReference<>();
        group.alterMessagesTo(nodeC.getLocalEndpoint(), nodeA.getLocalEndpoint(), message -> {
            if (message instanceof AppendEntriesSuccessResponse) {
                AppendEntriesSuccessResponse response = (AppendEntriesSuccessResponse) message;
                if (response.getLastLogIndex() == base + 3 && delayed.compareAndSet(null, response)) {
                    return null;
                }
            }
            return message;
        });
        eventually(() -> {
            assertThat(getRole(nodeA)).isEqualTo(LEADER);
            assertThat(getTerm(nodeA)).isGreaterThan(termOfB);
        });
        eventually(() -> assertThat(delayed.get()).isNotNull());
        AppendEntriesSuccessResponse staleResponse = delayed.get();

        // A is cut off. B wins with C's vote, since its last entry has a newer term, and commits "4". This
        // truncates 1b..1d from C, and then from A once A reconnects.
        resetAll();
        isolate(nodeA);
        eventually(() -> {
            assertThat(getRole(nodeB)).isEqualTo(LEADER);
            assertThat(getTerm(nodeB)).isGreaterThan(staleResponse.getTerm());
        });
        nodeB.replicate(applyValue("4")).join();
        resetAll();
        eventually(() -> assertThat(getCommitIndex(nodeA)).isEqualTo(base + 2));
        assertThat(lastIndex(nodeA)).isEqualTo(base + 2);

        // A wins a newer term again, and appends "5" at base + 3 without replicating it.
        nodeB.transferLeadership(nodeA.getLocalEndpoint());
        eventually(() -> assertThat(group.getLeaderEndpoint()).isEqualTo(nodeA.getLocalEndpoint()));
        Stream.of(nodeB, nodeC).forEach(other -> group.dropMessagesTo(nodeA.getLocalEndpoint(),
                other.getLocalEndpoint(), AppendEntriesRequest.class));
        CompletableFuture<Ordered<Object>> five = nodeA.replicate(applyValue("5"));
        eventually(() -> assertThat(lastIndex(nodeA)).isEqualTo(base + 3));

        // C's ack from the earlier term arrives. It covers base + 3, where C held 1d, not "5".
        nodeA.handle(staleResponse);
        readRaftState(nodeA, () -> null);
        long commitIndexOfA = getCommitIndex(nodeA);
        boolean fiveAcked = five.isDone() && !five.isCompletedExceptionally();

        // B and C, which never saw "5", elect a leader that commits "6" at the same index.
        isolate(nodeA);
        eventually(() -> assertThat(Stream.of(nodeB, nodeC).filter(node -> getRole(node) == LEADER)).hasSize(1));
        RaftNodeImpl newLeader = Stream.of(nodeB, nodeC).filter(node -> getRole(node) == LEADER).findFirst()
                .orElseThrow();
        long sixIndex = newLeader.replicate(applyValue("6")).join().getCommitIndex();

        assertThat(commitIndexOfA).as(
                "%s committed \"5\" at index %s on a response from term %s (client acked: %s),"
                        + " and %s then committed \"6\" at index %s",
                nodeA.getLocalEndpoint().getId(), base + 3, staleResponse.getTerm(), fiveAcked,
                newLeader.getLocalEndpoint().getId(), sixIndex).isLessThan(base + 3);
    }

    private void isolate(RaftNodeImpl node) {
        RaftEndpoint endpoint = node.getLocalEndpoint();
        List<RaftNodeImpl> nodes = group.getNodesExcept(endpoint);
        nodes.stream().map(RaftNodeImpl::getLocalEndpoint).forEach(other -> {
            group.dropAllMessagesTo(endpoint, other);
            group.dropAllMessagesTo(other, endpoint);
        });
    }

    private void silence(RaftNodeImpl node) {
        RaftEndpoint endpoint = node.getLocalEndpoint();
        List<RaftNodeImpl> nodes = group.getNodesExcept(endpoint);
        nodes.stream().map(RaftNodeImpl::getLocalEndpoint).forEach(other -> {
            group.dropMessagesTo(endpoint, other, AppendEntriesRequest.class);
            group.dropMessagesTo(endpoint, other, PreVoteRequest.class);
            group.dropMessagesTo(endpoint, other, VoteRequest.class);
            group.dropMessagesTo(other, endpoint, AppendEntriesRequest.class);
        });
    }

    private void resetAll() {
        Stream.of(nodeA, nodeB, nodeC).forEach(node -> group.resetAllRulesFrom(node.getLocalEndpoint()));
    }

    private static long lastIndex(RaftNodeImpl node) {
        return getLastLogOrSnapshotEntry(node).getIndex();
    }
}
