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
import static io.microraft.test.util.RaftTestUtils.getCommitIndex;
import static io.microraft.test.util.RaftTestUtils.getLastApplied;
import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletionException;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.junit.After;
import org.junit.Test;

import io.microraft.RaftConfig;
import io.microraft.impl.local.LocalRaftGroup;
import io.microraft.model.message.AppendEntriesRequest;
import io.microraft.model.message.InstallSnapshotRequest;
import io.microraft.test.util.BaseTest;

public class LaggingStateMachineStaleReadTest extends BaseTest {

    private static final int WRITES = 12;

    private LocalRaftGroup group;

    @After
    public void destroy() {
        if (group != null) {
            group.destroy();
        }
    }

    @Test(timeout = 300_000)
    public void when_leaderStateMachineLagsBehindCommitIndex_then_linearizableQueryDoesNotReturnStaleValue() {
        RaftConfig config = RaftConfig.newBuilder().setLeaderElectionTimeoutMillis(500)
                .setLeaderHeartbeatPeriodSecs(1).setLeaderHeartbeatTimeoutSecs(30).setCommitCountToTakeSnapshot(5)
                .build();
        group = LocalRaftGroup.newBuilder(3).setConfig(config).enableNewTermOperation().start();
        RaftNodeImpl oldLeader = group.waitUntilLeaderElected();
        List<RaftNodeImpl> followers = group.getNodesExcept(oldLeader.getLocalEndpoint());
        RaftNodeImpl lagging = followers.get(0);
        RaftNodeImpl disconnected = followers.get(1);

        // Every commit needs the lagging follower's ack, so its log keeps up with the leader and it is never caught
        // up by a snapshot transfer. Its snapshots fail, so its state machine stops at the first snapshot index.
        group.dropMessagesTo(oldLeader.getLocalEndpoint(), disconnected.getLocalEndpoint(),
                AppendEntriesRequest.class);
        group.dropMessagesTo(oldLeader.getLocalEndpoint(), disconnected.getLocalEndpoint(),
                InstallSnapshotRequest.class);
        group.getStateMachine(lagging.getLocalEndpoint()).failSnapshots();
        for (int i = 1; i <= WRITES; i++) {
            oldLeader.replicate(applyValue("value" + i)).join();
        }
        long commitIndex = getCommitIndex(oldLeader);
        eventually(() -> assertThat(getCommitIndex(lagging)).isEqualTo(commitIndex));
        long lastApplied = getLastApplied(lagging);
        assertThat(lastApplied).isLessThan(commitIndex);

        // The lagging follower becomes leader and serves a linearizable read. The test state machine asserts that a
        // query runs at the index it has applied; a state machine without that check would return a stale value.
        oldLeader.transferLeadership(lagging.getLocalEndpoint()).join();
        eventually(() -> assertThat(group.getLeaderEndpoint()).isEqualTo(lagging.getLocalEndpoint()));
        Object outcome = lagging.query(queryLastValue(), LINEARIZABLE, Optional.empty(),
                Optional.of(Duration.ofSeconds(30))).handle((ordered, failure) -> failure == null
                        ? ordered.getResult()
                        : String.valueOf(failure instanceof CompletionException ? failure.getCause() : failure))
                .join();

        List<String> staleValues = IntStream.range(1, WRITES).mapToObj(i -> "value" + i).collect(Collectors.toList());
        String description = String.format(
                "linearizable read on %s, whose state machine stopped at index %s, after \"value%s\" was committed at"
                        + " index %s",
                lagging.getLocalEndpoint().getId(), lastApplied, WRITES, commitIndex);
        assertThat(outcome).as(description).isNotIn(staleValues.toArray());
        assertThat(String.valueOf(outcome)).as(description).doesNotStartWith(AssertionError.class.getName());
    }
}
