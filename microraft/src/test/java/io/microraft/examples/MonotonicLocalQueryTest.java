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

import io.microraft.Ordered;
import io.microraft.QueryPolicy;
import io.microraft.RaftNode;
import io.microraft.exception.LaggingCommitIndexException;
import io.microraft.impl.local.LocalRaftGroup;
import io.microraft.impl.local.SimpleStateMachine;
import io.microraft.impl.util.BaseTest;
import org.junit.After;
import org.junit.Test;

import java.util.concurrent.CompletionException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

/*

   TO RUN THIS CODE SAMPLE ON YOUR MACHINE:

   $ git clone https://github.com/metanet/MicroRaft.git
   $ cd MicroRaft && ./mvnw clean test -Dtest=io.microraft.examples.MonotonicLocalQueryTest -DfailIfNoTests=false -Pcode-sample

   YOU CAN SEE THIS CLASS AT:

   https://github.com/metanet/MicroRaft/blob/master/microraft/src/test/java/io/microraft/examples/MonotonicLocalQueryTest.java

 */
public class MonotonicLocalQueryTest
        extends BaseTest {

    private LocalRaftGroup group;

    @After
    public void tearDown() {
        if (group != null) {
            group.destroy();
        }
    }

    @Test
    public void testMonotonicLocalQuery() {
        group = LocalRaftGroup.start(3);
        RaftNode leader = group.waitUntilLeaderElected();
        RaftNode disconnectedFollower = group.getAnyFollower();

        String value1 = "value1";
        Ordered<Object> replicateResult1 = leader.replicate(SimpleStateMachine.apply(value1)).join();
        System.out.println(
                "replicate1 result: " + replicateResult1.getResult() + ", commit index: " + replicateResult1.getCommitIndex());

        group.dropAllMessagesTo(leader.getLocalEndpoint(), disconnectedFollower.getLocalEndpoint());

        String value2 = "value2";
        Ordered<Object> replicateResult2 = leader.replicate(SimpleStateMachine.apply(value2)).join();
        System.out.println(
                "replicate2 result: " + replicateResult2.getResult() + ", commit index: " + replicateResult2.getCommitIndex());

        Ordered<Object> queryResult1 = leader
                .query(SimpleStateMachine.queryLast(), QueryPolicy.ANY_LOCAL, replicateResult2.getCommitIndex()).join();
        System.out.println("query1 result: " + queryResult1.getResult() + ", commit index: " + queryResult1.getCommitIndex());

        assertThat(queryResult1.getResult()).isEqualTo(value2);
        assertThat(queryResult1.getCommitIndex()).isEqualTo(replicateResult2.getCommitIndex());

        try {
            disconnectedFollower.query(SimpleStateMachine.queryLast(), QueryPolicy.ANY_LOCAL, queryResult1.getCommitIndex())
                                .join();
            fail("non-monotonic query cannot succeed.");
        } catch (CompletionException e) {
            assertThat(e).hasCauseInstanceOf(LaggingCommitIndexException.class);
            System.out.println(
                    "Disconnected follower could not preserve monotonicity for local query at commit index: " + queryResult1
                            .getCommitIndex());
        }
    }

}
