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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

import java.util.Optional;
import java.util.concurrent.CompletionException;

import org.junit.Test;

import io.microraft.Ordered;
import io.microraft.QueryPolicy;
import io.microraft.RaftNode;
import io.microraft.exception.LaggingCommitIndexException;
import io.microraft.statemachine.StateMachine;
import io.microraft.tutorial.atomicregister.OperableAtomicRegister;

/*

   TO RUN THIS TEST ON YOUR MACHINE:

   $ gh repo clone MicroRaft/MicroRaft
   $ cd MicroRaft && ./mvnw clean test -Dtest=io.microraft.tutorial.MonotonicLocalQueryTest -DfailIfNoTests=false -Ptutorial

   YOU CAN SEE THIS CLASS AT:

   https://github.com/MicroRaft/MicroRaft/blob/master/microraft-tutorial/src/test/java/io/microraft/tutorial/MonotonicLocalQueryTest.java

 */
public class MonotonicLocalQueryTest extends BaseLocalTest {

    @Override
    protected StateMachine createStateMachine() {
        return new OperableAtomicRegister();
    }

    @Test
    public void testMonotonicLocalQuery() {
        RaftNode leader = waitUntilLeaderElected();
        RaftNode follower = getAnyNodeExcept(leader.getLocalEndpoint());

        leader.replicate(OperableAtomicRegister.newSetOperation("value1")).join();

        disconnect(leader.getLocalEndpoint(), follower.getLocalEndpoint());

        leader.replicate(OperableAtomicRegister.newSetOperation("value2")).join();

        Ordered<String> queryResult = leader.<String>query(OperableAtomicRegister.newGetOperation(),
                QueryPolicy.LINEARIZABLE, Optional.empty(), Optional.empty()).join();

        try {
            follower.query(OperableAtomicRegister.newGetOperation(), QueryPolicy.EVENTUAL_CONSISTENCY,
                    Optional.of(queryResult.getCommitIndex()), Optional.empty()).join();
            fail("non-monotonic query cannot succeed.");
        } catch (CompletionException e) {
            assertThat(e).hasCauseInstanceOf(LaggingCommitIndexException.class);
        }
    }

}
