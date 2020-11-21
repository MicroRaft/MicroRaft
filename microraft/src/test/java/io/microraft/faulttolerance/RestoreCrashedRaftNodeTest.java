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

import io.microraft.Ordered;
import io.microraft.QueryPolicy;
import io.microraft.RaftNode;
import io.microraft.impl.RaftNodeImpl;
import io.microraft.impl.local.LocalRaftGroup;
import io.microraft.impl.local.SimpleStateMachine;
import io.microraft.persistence.RaftStore;
import io.microraft.persistence.RestoredRaftState;
import io.microraft.test.util.RaftTestUtils;
import org.junit.After;
import org.junit.Test;

import static io.microraft.impl.local.LocalRaftGroup.IN_MEMORY_RAFT_STATE_STORE_FACTORY;
import static io.microraft.test.util.AssertionUtils.eventually;
import static org.assertj.core.api.Assertions.assertThat;

/*

   TO RUN THIS TEST ON YOUR MACHINE:

   $ git clone https://github.com/MicroRaft/MicroRaft.git
   $ cd MicroRaft && ./mvnw clean test -Dtest=io.microraft.faulttolerance.RestoreCrashedRaftNodeTest -DfailIfNoTests=false -Ptutorial

   YOU CAN SEE THIS CLASS AT:

   https://github.com/MicroRaft/MicroRaft/blob/master/microraft/src/test/java/io/microraft/faulttolerance/RestoreCrashedRaftNodeTest.java

 */
public class RestoreCrashedRaftNodeTest {

    private LocalRaftGroup group;

    @After
    public void tearDown() {
        if (group != null) {
            group.destroy();
        }
    }

    @Test
    public void testRestoreCrashedRaftNode() {
        group = LocalRaftGroup.newBuilder(3).setRaftStoreFactory(IN_MEMORY_RAFT_STATE_STORE_FACTORY).start();
        RaftNode leader = group.waitUntilLeaderElected();
        RaftNode crashedFollower = group.getAnyNodeExcept(leader.getLocalEndpoint());

        String value = "value";
        Ordered<Object> replicateResult = leader.replicate(SimpleStateMachine.applyValue(value)).join();
        System.out.println(
                "replicate result: " + replicateResult.getResult() + ", commit index: " + replicateResult.getCommitIndex());

        eventually(() -> {
            Object queryOperation = SimpleStateMachine.queryLastValue();
            Ordered<String> queryResult = crashedFollower.<String>query(queryOperation, QueryPolicy.ANY_LOCAL, 0).join();
            assertThat(queryResult.getResult()).isEqualTo(value);
            System.out.println(
                    "monotonic local query successful on follower. query result: " + queryResult.getResult() + ", commit index: "
                            + queryResult.getCommitIndex());
        });

        RestoredRaftState restoredState = RaftTestUtils.getRestoredState(crashedFollower);
        RaftStore raftStore = RaftTestUtils.getRaftStore(crashedFollower);
        group.terminateNode(crashedFollower.getLocalEndpoint());
        RaftNodeImpl restoredFollower = group.restoreNode(restoredState, raftStore);

        eventually(() -> {
            Object queryOperation = SimpleStateMachine.queryLastValue();
            Ordered<String> queryResult = restoredFollower.<String>query(queryOperation, QueryPolicy.ANY_LOCAL, 0).join();
            assertThat(queryResult.getResult()).isEqualTo(value);
            System.out.println("monotonic local query successful on restarted follower. query result: " + queryResult.getResult()
                                       + ", commit index: " + queryResult.getCommitIndex());
        });
    }

}
