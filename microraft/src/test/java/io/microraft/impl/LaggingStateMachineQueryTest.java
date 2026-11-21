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
import static io.microraft.QueryPolicy.LINEARIZABLE;
import static io.microraft.impl.local.SimpleStateMachine.applyValue;
import static io.microraft.impl.local.SimpleStateMachine.queryLastValue;
import static io.microraft.test.util.AssertionUtils.eventually;
import static io.microraft.test.util.RaftTestUtils.getCommitIndex;
import static io.microraft.test.util.RaftTestUtils.getLastApplied;
import static io.microraft.test.util.RaftTestUtils.readRaftState;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletionException;

import org.junit.After;
import org.junit.Test;

import io.microraft.RaftConfig;
import io.microraft.exception.LaggingStateMachineException;
import io.microraft.impl.local.LocalRaftGroup;
import io.microraft.model.message.AppendEntriesRequest;
import io.microraft.model.message.InstallSnapshotRequest;
import io.microraft.test.util.BaseTest;

public class LaggingStateMachineQueryTest extends BaseTest {

    private static final RaftConfig CONFIG = RaftConfig.newBuilder().setLeaderElectionTimeoutMillis(500)
            .setLeaderHeartbeatPeriodSecs(1).setLeaderHeartbeatTimeoutSecs(30).setCommitCountToTakeSnapshot(5)
            .build();
    private static final int WRITES = 12;
    private static final int OPERATION_TIMEOUT_SECONDS = 5;

    private LocalRaftGroup group;

    @After
    public void destroy() {
        if (group != null) {
            group.destroy();
        }
    }

    @Test(timeout = 300_000)
    public void when_leaderStateMachineLagsBehindCommitIndex_then_linearizableQueryFails() {
        group = LocalRaftGroup.newBuilder(3).setConfig(CONFIG).enableNewTermOperation().start();
        RaftNodeImpl oldLeader = group.waitUntilLeaderElected();
        List<RaftNodeImpl> followers = group.getNodesExcept(oldLeader.getLocalEndpoint());
        RaftNodeImpl lagging = followers.get(0);
        RaftNodeImpl disconnected = followers.get(1);

        // Every commit needs the lagging follower's ack, so its log keeps up with
        // the leader and it is never caught up by a snapshot transfer. Its
        // snapshots fail, so its state machine stops at the first snapshot index.
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
        assertThat(getLastApplied(lagging)).isLessThan(commitIndex);

        oldLeader.transferLeadership(lagging.getLocalEndpoint()).join();
        eventually(() -> assertThat(group.getLeaderEndpoint()).isEqualTo(lagging.getLocalEndpoint()));

        assertThatThrownBy(() -> lagging.query(queryLastValue(), LINEARIZABLE, Optional.empty(),
                Optional.of(Duration.ofSeconds(30))).join()).isInstanceOf(CompletionException.class)
                .hasCauseInstanceOf(LaggingStateMachineException.class);
    }

    @Test(timeout = 30_000)
    public void laggingLeaderRejectsLinearizableReadWhileShrinkingToOneVoter() throws Exception {
        group = LocalRaftGroup.newBuilder(2).setConfig(CONFIG).enableNewTermOperation().build();
        group.start();
        RaftNodeImpl oldLeader = group.waitUntilLeaderElected();
        RaftNodeImpl lagging = group.getAnyNodeExcept(oldLeader.getLocalEndpoint());
        group.getStateMachine(lagging.getLocalEndpoint()).failSnapshots();
        for (int i = 1; i <= WRITES; i++) {
            oldLeader.replicate(applyValue("value" + i)).get(OPERATION_TIMEOUT_SECONDS, SECONDS);
        }
        long writeIndex = getCommitIndex(oldLeader);
        eventually(() -> assertThat(getCommitIndex(lagging)).isEqualTo(writeIndex));
        oldLeader.transferLeadership(lagging.getLocalEndpoint()).get(OPERATION_TIMEOUT_SECONDS, SECONDS);
        eventually(() -> assertThat(group.getLeaderEndpoint()).isEqualTo(lagging.getLocalEndpoint()));
        eventually(() -> assertThat(getCommitIndex(lagging)).isGreaterThan(writeIndex));

        // Appending the removal reduces the effective quorum before the state machine can apply it.
        lagging.changeMembership(oldLeader.getLocalEndpoint(), REMOVE_MEMBER, 0);
        int quorumSize = readRaftState(lagging, () -> lagging.state().logReplicationQuorumSize());
        assertThat(quorumSize).isOne();
        assertThat(getLastApplied(lagging)).isLessThan(writeIndex);

        Object outcome = lagging.query(queryLastValue(), LINEARIZABLE, Optional.empty(), Optional.empty())
                .handle((result, failure) -> failure != null ? failure : result.getResult())
                .get(OPERATION_TIMEOUT_SECONDS, SECONDS);
        assertThat(outcome).as("linearizable read after an acknowledged write at index %s", writeIndex)
                .isInstanceOf(LaggingStateMachineException.class);
    }
}
