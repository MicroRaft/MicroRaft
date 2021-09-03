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

package io.microraft.tutorial;

import io.microraft.Ordered;
import io.microraft.QueryPolicy;
import io.microraft.RaftNode;
import io.microraft.statemachine.StateMachine;
import io.microraft.tutorial.atomicregister.OperableAtomicRegister;
import io.microraft.tutorial.atomicregister.OperableAtomicRegister.AtomicRegisterOperation;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

/*

   TO RUN THIS TEST ON YOUR MACHINE:

   $ gh repo clone MicroRaft/MicroRaft
   $ cd MicroRaft && ./mvnw clean test -Dtest=io.microraft.tutorial.LinearizableQueryTest -DfailIfNoTests=false -Ptutorial

   YOU CAN SEE THIS CLASS AT:

   https://github.com/MicroRaft/MicroRaft/blob/master/microraft-tutorial/src/test/java/io/microraft/tutorial/LinearizableQueryTest.java

 */
public class LinearizableQueryTest
        extends BaseLocalTest {

    @Override
    protected StateMachine createStateMachine() {
        return new OperableAtomicRegister();
    }

    @Test
    public void testLinearizableQuery() {
        RaftNode leader = waitUntilLeaderElected();

        String value = "value";
        AtomicRegisterOperation operation1 = OperableAtomicRegister.newSetOperation(value);
        Ordered<String> result1 = leader.<String>replicate(operation1).join();

        System.out.println("set operation commit index: " + result1.getCommitIndex());

        AtomicRegisterOperation operation2 = OperableAtomicRegister.newGetOperation();
        Ordered<String> queryResult = leader.<String>query(operation2, QueryPolicy.LINEARIZABLE, 0).join();

        System.out
                .println("get operation result: " + queryResult.getResult() + ", commit index: " + queryResult.getCommitIndex());

        assertThat(queryResult.getResult()).isEqualTo(value);
        assertThat(queryResult.getCommitIndex()).isEqualTo(result1.getCommitIndex());
    }

}
