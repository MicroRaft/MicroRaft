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

import io.microraft.MembershipChangeMode;
import io.microraft.Ordered;
import io.microraft.QueryPolicy;
import io.microraft.RaftEndpoint;
import io.microraft.RaftNode;
import io.microraft.report.RaftGroupMembers;
import io.microraft.statemachine.StateMachine;
import io.microraft.tutorial.atomicregister.OperableAtomicRegister;
import io.microraft.tutorial.atomicregister.SnapshotableAtomicRegister;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

/*

   TO RUN THIS TEST ON YOUR MACHINE:

   $ gh repo clone MicroRaft/MicroRaft
   $ cd MicroRaft && ./mvnw clean test -Dtest=io.microraft.tutorial.ChangeRaftGroupMemberListTest -DfailIfNoTests=false -Ptutorial

   YOU CAN SEE THIS CLASS AT:

   https://github.com/MicroRaft/MicroRaft/blob/master/microraft-tutorial/src/test/java/io/microraft/tutorial/ChangeRaftGroupMemberListTest.java

 */
public class ChangeRaftGroupMemberListTest extends BaseLocalTest {

    @Override
    protected StateMachine createStateMachine() {
        return new SnapshotableAtomicRegister();
    }

    @Test
    public void testChangeMemberListOfRaftGroup() {
        RaftNode leader = waitUntilLeaderElected();

        String value1 = "value1";
        leader.replicate(OperableAtomicRegister.newSetOperation(value1)).join();

        // when we start a new Raft node, we should first add it to the Raft
        // group, then start its RaftNode instance.
        RaftEndpoint endpoint4 = LocalRaftEndpoint.newEndpoint();
        // group members commit index of the initial Raft group members is 0.
        RaftGroupMembers newMemberList1 = leader
                .changeMembership(endpoint4, MembershipChangeMode.ADD_OR_PROMOTE_TO_FOLLOWER, 0).join().getResult();
        System.out.println("New member list: " + newMemberList1.getMembers() + ", majority: "
                + newMemberList1.getMajorityQuorumSize() + ", commit index: " + newMemberList1.getLogIndex());

        // endpoint4 is now part of the member list. Let's start its Raft node
        RaftNode raftNode4 = createRaftNode(endpoint4);
        raftNode4.start();

        String value2 = "value2";
        leader.replicate(OperableAtomicRegister.newSetOperation(value2)).join();

        RaftEndpoint endpoint5 = LocalRaftEndpoint.newEndpoint();
        // we need the commit index of the current Raft group member list for
        // adding endpoint5. We can get it either from
        // newMemberList1.getCommitIndex() or leader.getCommittedMembers().
        RaftGroupMembers newMemberList2 = leader.changeMembership(endpoint5,
                MembershipChangeMode.ADD_OR_PROMOTE_TO_FOLLOWER, newMemberList1.getLogIndex()).join().getResult();
        System.out.println("New member list: " + newMemberList2.getMembers() + ", majority: "
                + newMemberList2.getMajorityQuorumSize() + ", commit index: " + newMemberList2.getLogIndex());

        // endpoint5 is also part of the member list now. Let's start its Raft node
        RaftNode raftNode5 = createRaftNode(endpoint5);
        raftNode5.start();

        String value3 = "value3";
        leader.replicate(OperableAtomicRegister.newSetOperation(value3)).join();

        // wait until the new Raft nodes replicate the log entries.
        eventually(() -> {
            assertThat(query(raftNode4).getResult()).isEqualTo(value3);
            assertThat(query(raftNode5).getResult()).isEqualTo(value3);
        });

        System.out.println(endpoint4.getId() + "'s local query result: " + query(raftNode4).getResult());
        System.out.println(endpoint5.getId() + "'s local query result: " + query(raftNode5).getResult());
    }

    private Ordered<String> query(RaftNode raftNode) {
        return raftNode.<String>query(OperableAtomicRegister.newGetOperation(), QueryPolicy.EVENTUAL_CONSISTENCY, 0)
                .join();
    }

}
