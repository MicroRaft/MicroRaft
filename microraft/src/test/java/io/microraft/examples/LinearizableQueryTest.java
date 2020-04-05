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
import io.microraft.impl.local.LocalRaftGroup;
import io.microraft.impl.local.SimpleStateMachine;
import io.microraft.impl.util.BaseTest;
import org.junit.After;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

/*

   TO RUN THIS CODE SAMPLE ON YOUR MACHINE:

   $ git clone https://github.com/metanet/MicroRaft.git
   $ cd MicroRaft && ./mvnw clean test -Dtest=io.microraft.examples.LinearizableQueryTest -DfailIfNoTests=false -Pcode-sample

   YOU CAN SEE THIS CLASS AT:

   https://github.com/metanet/MicroRaft/blob/master/microraft/src/test/java/io/microraft/examples/LinearizableQueryTest.java

 */
public class LinearizableQueryTest
        extends BaseTest {

    private LocalRaftGroup group;

    @After
    public void tearDown() {
        if (group != null) {
            group.destroy();
        }
    }

    @Test
    public void testLinearizableQuery() {
        group = LocalRaftGroup.start(3);
        RaftNode leader = group.waitUntilLeaderElected();

        String value = "value";
        Object applyOperation = SimpleStateMachine.apply(value);
        Ordered<Object> replicateResult = leader.replicate(applyOperation).join();
        System.out.println(
                "operation result: " + replicateResult.getResult() + ", commit index: " + replicateResult.getCommitIndex());

        Object queryOperation = SimpleStateMachine.queryLast();
        Ordered<Object> queryResult = leader.query(queryOperation, QueryPolicy.LINEARIZABLE, 0).join();
        System.out.println("query result: " + queryResult.getResult() + ", commit index: " + queryResult.getCommitIndex());

        assertThat(queryResult.getResult()).isEqualTo(value);
        assertThat(queryResult.getCommitIndex()).isEqualTo(replicateResult.getCommitIndex());
    }

}
