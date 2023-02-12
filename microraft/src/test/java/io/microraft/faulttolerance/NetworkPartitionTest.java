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

import static io.microraft.test.util.AssertionUtils.eventually;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

import java.util.List;
import java.util.concurrent.CompletionException;

import org.junit.After;
import org.junit.Test;

import io.microraft.QueryPolicy;
import io.microraft.RaftConfig;
import io.microraft.RaftNode;
import io.microraft.RaftRole;
import io.microraft.exception.NotLeaderException;
import io.microraft.impl.local.LocalRaftGroup;
import io.microraft.impl.local.SimpleStateMachine;
import io.microraft.report.RaftNodeReport;
import io.microraft.test.util.BaseTest;

/*

   TO RUN THIS TEST ON YOUR MACHINE:

   $ gh repo clone MicroRaft/MicroRaft
   $ cd MicroRaft && ./mvnw clean test -Dtest=io.microraft.faulttolerance.NetworkPartitionTest -DfailIfNoTests=false -Ptutorial

   YOU CAN SEE THIS CLASS AT:

   https://github.com/MicroRaft/MicroRaft/blob/master/microraft/src/test/java/io/microraft/faulttolerance/NetworkPartitionTest.java

 */
public class NetworkPartitionTest extends BaseTest {

    private LocalRaftGroup group;

    @After
    public void tearDown() {
        if (group != null) {
            group.destroy();
        }
    }

    @Test
    public void testNetworkPartition() {
        RaftConfig config = RaftConfig.newBuilder().setLeaderHeartbeatPeriodSecs(1).setLeaderHeartbeatTimeoutSecs(5)
                .build();
        group = LocalRaftGroup.newBuilder(3).setConfig(config).start();
        RaftNode firstLeader = group.waitUntilLeaderElected();
        List<RaftNode> followers = group.getNodesExcept(firstLeader.getLocalEndpoint());

        // we are splitting the leader from the followers.
        group.splitMembers(firstLeader.getLocalEndpoint());

        // the leader eventually loses majority since it is not hearing
        // back from the followers
        eventually(() -> {
            RaftNodeReport leaderReport = firstLeader.getReport().join().getResult();
            assertThat(leaderReport.getRole()).isEqualTo(RaftRole.FOLLOWER);
        });

        String value1 = "value1";
        try {
            // we cannot commit an operation via the initial leader because it lost its
            // leadership now.
            firstLeader.replicate(SimpleStateMachine.applyValue(value1)).join();
            fail(firstLeader.getLocalEndpoint().getId() + " cannot replicate a new operation after losing leadership");
        } catch (CompletionException e) {
            assertThat(e).hasCauseInstanceOf(NotLeaderException.class);
        }

        // the followers elect a new leader among themselves
        eventually(() -> {
            for (RaftNode node : followers) {
                RaftNodeReport report = node.getReport().join().getResult();
                assertThat(report.getTerm().getLeaderEndpoint()).isNotNull()
                        .isNotEqualTo(firstLeader.getLocalEndpoint());
            }
        });

        // we can commit an operation via the new leader.
        String value2 = "value2";
        RaftNode secondLeader = group.getNode(followers.get(0).getTerm().getLeaderEndpoint());
        secondLeader.replicate(SimpleStateMachine.applyValue(value2)).join();

        // let's resolve the network partition
        group.merge();

        // our old leader will notice the new leader and get the new log entry in a
        // second
        eventually(() -> {
            assertThat(firstLeader.getTerm().getLeaderEndpoint()).isEqualTo(secondLeader.getLocalEndpoint());

            String value = firstLeader
                    .<String>query(SimpleStateMachine.queryLastValue(), QueryPolicy.EVENTUAL_CONSISTENCY, 0).join()
                    .getResult();
            assertThat(value).isEqualTo(value2);
        });
    }

}
