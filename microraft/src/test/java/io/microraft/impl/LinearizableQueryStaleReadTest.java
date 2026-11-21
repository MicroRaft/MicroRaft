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

import static io.microraft.QueryPolicy.LINEARIZABLE;
import static io.microraft.RaftRole.LEADER;
import static io.microraft.impl.local.SimpleStateMachine.applyValue;
import static io.microraft.impl.local.SimpleStateMachine.queryLastValue;
import static io.microraft.test.util.AssertionUtils.eventually;
import static io.microraft.test.util.RaftTestUtils.getLastLogOrSnapshotEntry;
import static io.microraft.test.util.RaftTestUtils.getLeaderQuerySequenceNumber;
import static io.microraft.test.util.RaftTestUtils.getRole;
import static io.microraft.test.util.RaftTestUtils.getTerm;
import static io.microraft.test.util.RaftTestUtils.readRaftState;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;

import org.junit.After;
import org.junit.Test;

import io.microraft.Ordered;
import io.microraft.RaftConfig;
import io.microraft.impl.local.LocalRaftGroup;
import io.microraft.model.log.BaseLogEntry;
import io.microraft.model.message.AppendEntriesSuccessResponse;
import io.microraft.model.message.TriggerLeaderElectionRequest;
import io.microraft.test.util.BaseTest;

public class LinearizableQueryStaleReadTest extends BaseTest {

    private static final RaftConfig CONFIG = RaftConfig.newBuilder().setLeaderElectionTimeoutMillis(500)
            .setLeaderHeartbeatPeriodSecs(1).setLeaderHeartbeatTimeoutSecs(30).build();

    private LocalRaftGroup group;

    @After
    public void destroy() {
        if (group != null) {
            group.destroy();
        }
    }

    @Test(timeout = 300_000)
    public void when_queryArrivesAfterNewLeaderCommittedWrite_then_oldLeaderDoesNotReturnStaleValue() {
        group = LocalRaftGroup.newBuilder(3).setConfig(CONFIG).start();
        RaftNodeImpl oldLeader = group.waitUntilLeaderElected();
        oldLeader.replicate(applyValue("value1")).join();
        List<RaftNodeImpl> followers = group.getNodesExcept(oldLeader.getLocalEndpoint());
        RaftNodeImpl newLeader = followers.get(0);

        // The followers' acks for the first query round are delayed.
        List<AppendEntriesSuccessResponse> delayedAcks = new CopyOnWriteArrayList<>();
        for (RaftNodeImpl follower : followers) {
            group.alterMessagesTo(follower.getLocalEndpoint(), oldLeader.getLocalEndpoint(), message -> {
                if (message instanceof AppendEntriesSuccessResponse
                        && ((AppendEntriesSuccessResponse) message).getQuerySequenceNumber() > 0) {
                    delayedAcks.add((AppendEntriesSuccessResponse) message);
                    return null;
                }
                return message;
            });
        }
        CompletableFuture<Ordered<Object>> firstQuery = oldLeader.query(queryLastValue(), LINEARIZABLE,
                Optional.empty(), Optional.empty());
        long round = getLeaderQuerySequenceNumber(oldLeader);
        eventually(() -> assertThat(delayedAcks).anyMatch(ack -> ack.getQuerySequenceNumber() == round));

        // The old leader is cut off before it notices. One follower's election timeout fires, the trigger below
        // stands in for it, and it wins with the other follower's vote.
        group.getNodesExcept(oldLeader.getLocalEndpoint()).forEach(other -> {
            group.dropAllMessagesTo(oldLeader.getLocalEndpoint(), other.getLocalEndpoint());
            group.dropAllMessagesTo(other.getLocalEndpoint(), oldLeader.getLocalEndpoint());
        });
        BaseLogEntry lastEntry = getLastLogOrSnapshotEntry(newLeader);
        TriggerLeaderElectionRequest trigger = newLeader.getModelFactory().createTriggerLeaderElectionRequestBuilder()
                .setGroupId(newLeader.getGroupId()).setSender(oldLeader.getLocalEndpoint())
                .setTerm(getTerm(newLeader)).setLastLogTerm(lastEntry.getTerm())
                .setLastLogIndex(lastEntry.getIndex()).build();
        newLeader.handle(trigger);
        eventually(() -> assertThat(getRole(newLeader)).isEqualTo(LEADER));
        newLeader.replicate(applyValue("value2")).join();

        // "value2" is committed. A client now reads from the old leader, which still believes it leads.
        assertThat(getRole(oldLeader)).isEqualTo(LEADER);
        CompletableFuture<Ordered<Object>> secondQuery = oldLeader.query(queryLastValue(), LINEARIZABLE,
                Optional.empty(), Optional.empty());

        // The acks that were sent before the second query arrived finally reach the old leader.
        delayedAcks.stream().filter(ack -> ack.getQuerySequenceNumber() == round).forEach(oldLeader::handle);
        readRaftState(oldLeader, () -> null);
        eventually(() -> assertThat(firstQuery).isDone());

        Object secondResult = secondQuery.isDone() ? secondQuery.join().getResult() : "pending";
        assertThat(secondResult)
                .as("linearizable read issued on %s after %s committed \"value2\"",
                        oldLeader.getLocalEndpoint().getId(), newLeader.getLocalEndpoint().getId())
                .isNotEqualTo("value1");
    }
}
