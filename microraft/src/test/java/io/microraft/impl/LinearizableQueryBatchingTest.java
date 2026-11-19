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
import static io.microraft.impl.local.SimpleStateMachine.applyValue;
import static io.microraft.impl.local.SimpleStateMachine.queryLastValue;
import static io.microraft.test.util.AssertionUtils.eventually;
import static io.microraft.test.util.RaftTestUtils.getLeaderQuerySequenceNumber;
import static io.microraft.test.util.RaftTestUtils.readRaftState;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.After;
import org.junit.Test;

import io.microraft.Ordered;
import io.microraft.RaftConfig;
import io.microraft.impl.handler.AppendEntriesSuccessResponseHandler;
import io.microraft.impl.local.LocalRaftGroup;
import io.microraft.model.message.AppendEntriesSuccessResponse;
import io.microraft.model.message.RaftMessage;
import io.microraft.test.util.BaseTest;

/**
 * A linearizable query may only be answered after a quorum confirms the leader
 * in a heartbeat round that started after the query arrived. Acks sent before
 * the query existed say nothing about the leader at the time of the query.
 */
public class LinearizableQueryBatchingTest extends BaseTest {

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
    public void when_queryArrivesAfterAcksOfCurrentRoundWereSent_then_thoseAcksDoNotConfirmIt() {
        group = LocalRaftGroup.newBuilder(5).setConfig(CONFIG).enableNewTermOperation().start();
        RaftNodeImpl leader = group.waitUntilLeaderElected();
        leader.replicate(applyValue("value1")).join();

        // Hold back every success response, keeping the ones that ack a query round.
        AtomicBoolean holding = new AtomicBoolean(true);
        List<AppendEntriesSuccessResponse> held = new CopyOnWriteArrayList<>();
        for (RaftNodeImpl follower : group.<RaftNodeImpl>getNodesExcept(leader.getLocalEndpoint())) {
            group.alterMessagesTo(follower.getLocalEndpoint(), leader.getLocalEndpoint(), message -> {
                if (holding.get() && message instanceof AppendEntriesSuccessResponse) {
                    AppendEntriesSuccessResponse response = (AppendEntriesSuccessResponse) message;
                    if (response.getQuerySequenceNumber() > 0) {
                        held.add(response);
                    }
                    return null;
                }
                return message;
            });
        }

        CompletableFuture<Ordered<Object>> query1 = leader.query(queryLastValue(), LINEARIZABLE, Optional.empty(),
                Optional.empty());
        long round = getLeaderQuerySequenceNumber(leader);
        eventually(() -> assertThat(held.stream().filter(response -> response.getQuerySequenceNumber() == round)
                .map(RaftMessage::getSender).distinct().count()).isGreaterThanOrEqualTo(2));
        List<AppendEntriesSuccessResponse> sentBeforeQuery2 = new ArrayList<>(held);

        CompletableFuture<Ordered<Object>> query2 = leader.query(queryLastValue(), LINEARIZABLE, Optional.empty(),
                Optional.empty());
        eventually(() -> assertThat(readRaftState(leader, () -> leader.state().leaderState().queryState()
                .waitingQueriesCount())).isEqualTo(1));

        readRaftState(leader, () -> {
            for (AppendEntriesSuccessResponse response : sentBeforeQuery2) {
                if (response.getQuerySequenceNumber() == round) {
                    new AppendEntriesSuccessResponseHandler(leader, response).run();
                }
            }
            return null;
        });

        assertThat(query1).isDone();
        assertThat(query2).isNotDone();

        holding.set(false);
        assertThat(query2.join().getResult()).isEqualTo("value1");
    }
}
