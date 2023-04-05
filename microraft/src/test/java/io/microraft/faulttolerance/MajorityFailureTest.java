/*
 * Copyright (c) 2020, MicroRaft.
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

package io.microraft.faulttolerance;

import static io.microraft.impl.local.LocalRaftGroup.IN_MEMORY_RAFT_STATE_STORE_FACTORY;
import static io.microraft.test.util.AssertionUtils.eventually;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

import org.junit.After;
import org.junit.Test;

import io.microraft.Ordered;
import io.microraft.QueryPolicy;
import io.microraft.RaftConfig;
import io.microraft.RaftNode;
import io.microraft.RaftRole;
import io.microraft.exception.IndeterminateStateException;
import io.microraft.exception.NotLeaderException;
import io.microraft.impl.local.LocalRaftGroup;
import io.microraft.impl.local.SimpleStateMachine;
import io.microraft.persistence.RaftStore;
import io.microraft.persistence.RestoredRaftState;
import io.microraft.report.RaftNodeReport;
import io.microraft.test.util.RaftTestUtils;

/*

   TO RUN THIS TEST ON YOUR MACHINE:

   $ gh repo clone MicroRaft/MicroRaft
   $ cd MicroRaft && ./mvnw clean test -Dtest=io.microraft.faulttolerance.MajorityFailureTest -DfailIfNoTests=false -Ptutorial

   YOU CAN SEE THIS CLASS AT:

   https://github.com/MicroRaft/MicroRaft/blob/master/microraft/src/test/java/io/microraft/faulttolerance/MajorityFailureTest.java

 */
public class MajorityFailureTest {

    private LocalRaftGroup group;

    @After
    public void tearDown() {
        if (group != null) {
            group.destroy();
        }
    }

    @Test
    public void testMajorityFailure() {
        RaftConfig config = RaftConfig.newBuilder().setLeaderHeartbeatTimeoutSecs(5).setLeaderHeartbeatPeriodSecs(1)
                .build();
        group = LocalRaftGroup.newBuilder(3).setConfig(config).setRaftStoreFactory(IN_MEMORY_RAFT_STATE_STORE_FACTORY)
                .start();
        RaftNode leader = group.waitUntilLeaderElected();
        List<RaftNode> followers = group.getNodesExcept(leader.getLocalEndpoint());

        String value = "value";
        long commitIndex1 = leader.replicate(SimpleStateMachine.applyValue(value)).join().getCommitIndex();
        int term1 = leader.getTerm().getTerm();

        for (RaftNode follower : followers) {
            group.terminateNode(follower.getLocalEndpoint());
        }

        CompletableFuture<Ordered<Object>> future = leader.replicate(SimpleStateMachine.applyValue(value));

        // we wait until the leader notices that it has lost the majority.
        // on each heartbeat period, the leader checks if it has received
        // append entries rpc responses in the last "heartbeat timeout seconds"
        // duration. in this case, it demotes itself to the follower role so
        // that clients talk to the other Raft nodes to find a functioning
        // leader.
        // we don't actually need to wait until the demotion of the leader
        // before restoring crashed Raft nodes. we just do it here for the
        // sake of example.
        eventually(() -> {
            RaftNodeReport report = leader.getReport().join().getResult();
            assertThat(report.getRole()).isEqualTo(RaftRole.FOLLOWER);
            assertThat(future.isCompletedExceptionally());
            try {
                future.join();
            } catch (CompletionException e) {
                assertThat(e).hasCauseInstanceOf(IndeterminateStateException.class);
            }
        });

        try {
            // we cannot replicate a new operation because the leader has
            // demoted itself to the follower role so it will directly
            // reject our new operation.
            leader.replicate(SimpleStateMachine.applyValue(value)).join();
        } catch (CompletionException e) {
            assertThat(e).hasCauseInstanceOf(NotLeaderException.class);
        }

        // Our Raft group is unavailable for operations involving the majority
        // or a leader. However, we can still perform a local query with the
        // QueryPolicy.EVENTUAL_CONSISTENCY policy.
        Ordered<String> queryResult = leader.<String>query(SimpleStateMachine.queryLastValue(),
                QueryPolicy.EVENTUAL_CONSISTENCY, Optional.empty(), Optional.empty()).join();
        assertThat(queryResult.getCommitIndex()).isEqualTo(commitIndex1);
        assertThat(queryResult.getResult()).isEqualTo(value);

        // in order to restore availability of the Raft group, we don't need to
        // restore all crashed Raft nodes. It is sufficient to restore
        // the majority back. hence, we only restore 1 Raft node here so that
        // we will have 2 alive Raft nodes.
        RestoredRaftState restoredState = RaftTestUtils.getRestoredState(followers.get(0));
        RaftStore raftStore = RaftTestUtils.getRaftStore(followers.get(0));
        group.restoreNode(restoredState, raftStore);

        // now there will be a new leader election round. either the old leader
        // or the restored node could become the new leader.
        RaftNode newLeader = group.waitUntilLeaderElected();

        long commitIndex2 = newLeader.replicate(SimpleStateMachine.applyValue(value)).join().getCommitIndex();
        assertThat(commitIndex2).isGreaterThan(commitIndex1);
        int term2 = newLeader.getTerm().getTerm();
        assertThat(term2).isGreaterThan(term1);
    }

}
