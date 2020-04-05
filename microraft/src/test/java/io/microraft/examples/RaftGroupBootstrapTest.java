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

package io.microraft.examples;

import io.microraft.RaftConfig;
import io.microraft.RaftEndpoint;
import io.microraft.RaftNode;
import io.microraft.impl.local.LocalRaftEndpoint;
import io.microraft.impl.local.LocalRaftNodeRuntime;
import io.microraft.impl.local.SimpleStateMachine;
import io.microraft.impl.util.BaseTest;
import io.microraft.integration.StateMachine;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

/*

   TO RUN THIS CODE SAMPLE ON YOUR MACHINE:

   $ git clone https://github.com/metanet/MicroRaft.git
   $ cd MicroRaft && ./mvnw clean test -Dtest=io.microraft.examples.RaftGroupBootstrapTest -DfailIfNoTests=false -Pcode-sample

   YOU CAN SEE THIS CLASS AT:

   https://github.com/metanet/MicroRaft/blob/master/microraft/src/test/java/io/microraft/examples/RaftGroupBootstrapTest.java

 */
public class RaftGroupBootstrapTest
        extends BaseTest {

    @Test
    public void testRaftGroupBootstrap()
            throws Exception {
        List<RaftEndpoint> initialMembers = Arrays
                .asList(LocalRaftEndpoint.newEndpoint(), LocalRaftEndpoint.newEndpoint(), LocalRaftEndpoint.newEndpoint());
        RaftConfig config = RaftConfig.newBuilder().setLeaderHeartbeatPeriodSecs(30).setLeaderHeartbeatPeriodSecs(5).build();

        List<LocalRaftNodeRuntime> runtimes = new ArrayList<>();
        List<RaftNode> raftNodes = new ArrayList<>();

        for (RaftEndpoint endpoint : initialMembers) {
            LocalRaftNodeRuntime runtime = new LocalRaftNodeRuntime(endpoint);
            runtimes.add(runtime);
            StateMachine stateMachine = new SimpleStateMachine();
            RaftNode raftNode = RaftNode.newBuilder().setGroupId("default").setLocalEndpoint(endpoint)
                                        .setInitialGroupMembers(initialMembers).setConfig(config).setRuntime(runtime)
                                        .setStateMachine(stateMachine).build();
            raftNodes.add(raftNode);
            raftNode.start();
        }

        enableDiscovery(runtimes, raftNodes);

        Thread.sleep(TimeUnit.SECONDS.toMillis(10));

        for (RaftNode raftNode : raftNodes) {
            raftNode.terminate();
        }
    }

    private void enableDiscovery(List<LocalRaftNodeRuntime> runtimes, List<RaftNode> raftNodes) {
        for (int i = 0; i < raftNodes.size(); i++) {
            for (int j = 0; j < raftNodes.size(); j++) {
                if (i != j) {
                    runtimes.get(i).discoverNode(raftNodes.get(j));
                }
            }
        }
    }

}
