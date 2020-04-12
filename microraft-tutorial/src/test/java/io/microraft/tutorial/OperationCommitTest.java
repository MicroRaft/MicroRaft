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
import io.microraft.RaftNode;
import io.microraft.statemachine.StateMachine;
import io.microraft.tutorial.atomicregister.OperableAtomicRegister;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

/*

   TO RUN THIS TEST ON YOUR MACHINE:

   $ git clone https://github.com/metanet/MicroRaft.git
   $ cd MicroRaft && ./mvnw clean test -Dtest=io.microraft.tutorial.OperationCommitTest -DfailIfNoTests=false -Ptutorial

   YOU CAN SEE THIS CLASS AT:

   https://github.com/metanet/MicroRaft/blob/master/microraft-tutorial/src/test/java/io/microraft/tutorial/OperationCommitTest.java

 */
public class OperationCommitTest
        extends BaseLocalTest {

    @Override
    protected StateMachine createStateMachine() {
        return new OperableAtomicRegister();
    }

    @Test
    public void testCommitOperation() {
        RaftNode leader = waitUntilLeaderElected();

        String value1 = "value1";
        Ordered<String> result1 = leader.<String>replicate(OperableAtomicRegister.newSetOperation(value1)).join();

        assertThat(result1.getCommitIndex()).isGreaterThan(0);
        assertThat(result1.getResult()).isNull();

        System.out.println("1st operation commit index: " + result1.getCommitIndex() + ", result: " + result1.getResult());

        String value2 = "value2";
        Ordered<String> result2 = leader.<String>replicate(OperableAtomicRegister.newSetOperation(value2)).join();

        assertThat(result2.getCommitIndex()).isGreaterThan(result1.getCommitIndex());
        assertThat(result2.getResult()).isEqualTo(value1);

        System.out.println("2nd operation commit index: " + result2.getCommitIndex() + ", result: " + result2.getResult());

        String value3 = "value3";
        Ordered<Boolean> result3 = leader.<Boolean>replicate(OperableAtomicRegister.newCasOperation(value2, value3)).join();

        assertThat(result3.getCommitIndex()).isGreaterThan(result2.getCommitIndex());
        assertThat(result3.getResult()).isTrue();

        System.out.println("3rd operation commit index: " + result2.getCommitIndex() + ", result: " + result3.getResult());

        String value4 = "value4";
        Ordered<Boolean> result4 = leader.<Boolean>replicate(OperableAtomicRegister.newCasOperation(value2, value4)).join();

        assertThat(result4.getCommitIndex()).isGreaterThan(result3.getCommitIndex());
        assertThat(result4.getResult()).isFalse();

        System.out.println("4th operation commit index: " + result4.getCommitIndex() + ", result: " + result4.getResult());

        Ordered<String> result5 = leader.<String>replicate(OperableAtomicRegister.newGetOperation()).join();

        assertThat(result5.getCommitIndex()).isGreaterThan(result4.getCommitIndex());
        assertThat(result5.getResult()).isEqualTo(value3);

        System.out.println("5th operation commit index: " + result5.getCommitIndex() + ", result: " + result5.getResult());
    }

}
