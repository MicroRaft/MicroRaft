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

import io.microraft.RaftNode;
import io.microraft.statemachine.StateMachine;
import io.microraft.tutorial.atomicregister.AtomicRegister;
import org.junit.Test;

/*

   TO RUN THIS TEST ON YOUR MACHINE:

   $ git clone https://github.com/MicroRaft/MicroRaft.git
   $ cd MicroRaft && ./mvnw clean test -Dtest=io.microraft.tutorial.LeaderElectionTest -DfailIfNoTests=false -Ptutorial

   YOU CAN SEE THIS CLASS AT:

   https://github.com/MicroRaft/MicroRaft/blob/master/microraft-tutorial/src/test/java/io/microraft/tutorial/LeaderElectionTest.java

 */
public class LeaderElectionTest
        extends BaseLocalTest {

    @Override
    protected StateMachine createStateMachine() {
        return new AtomicRegister();
    }

    @Test
    public void testLeaderElection() {
        RaftNode leader = waitUntilLeaderElected();

        System.out.println(leader.getLocalEndpoint().getId() + " is the leader!");
    }

}
