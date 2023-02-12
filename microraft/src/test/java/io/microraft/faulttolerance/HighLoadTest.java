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

import static org.assertj.core.api.Assertions.assertThat;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

import org.junit.After;
import org.junit.Test;

import io.microraft.Ordered;
import io.microraft.RaftConfig;
import io.microraft.RaftNode;
import io.microraft.exception.CannotReplicateException;
import io.microraft.impl.local.LocalRaftGroup;
import io.microraft.impl.local.SimpleStateMachine;

/*

   TO RUN THIS TEST ON YOUR MACHINE:

   $ gh repo clone MicroRaft/MicroRaft
   $ cd MicroRaft && ./mvnw clean test -Dtest=io.microraft.faulttolerance.HighLoadTest -DfailIfNoTests=false -Ptutorial

   YOU CAN SEE THIS CLASS AT:

   https://github.com/MicroRaft/MicroRaft/blob/master/microraft/src/test/java/io/microraft/faulttolerance/HighLoadTest.java

 */
public class HighLoadTest {

    private LocalRaftGroup group;

    @After
    public void tearDown() {
        if (group != null) {
            group.destroy();
        }
    }

    @Test
    public void testHighLoad() throws InterruptedException {
        RaftConfig config = RaftConfig.newBuilder().setMaxPendingLogEntryCount(10).build();
        group = LocalRaftGroup.newBuilder(3).setConfig(config).start();
        RaftNode leader = group.waitUntilLeaderElected();

        // we are slowing down the followers
        // by making their Raft thread sleep for 3 seconds
        for (RaftNode follower : group.getNodesExcept(leader.getLocalEndpoint())) {
            group.slowDownNode(follower.getLocalEndpoint(), 3);
        }

        while (true) {
            // we are filling up the request buffer of the leader.
            // since the followers are slowed down, the leader won't be able to
            // keep up with the incoming request rate and after some time it
            // will start to fail new requests with CannotReplicateException
            CompletableFuture<Ordered<Object>> future = leader.replicate(SimpleStateMachine.applyValue("val"));
            Thread.sleep(10);

            if (future.isCompletedExceptionally()) {
                try {
                    future.join();
                } catch (CompletionException e) {
                    assertThat(e).hasCauseInstanceOf(CannotReplicateException.class);
                    return;
                }
            }
        }
    }

}
