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

import io.microraft.MembershipChangeMode;
import io.microraft.Ordered;
import io.microraft.QueryPolicy;
import io.microraft.RaftEndpoint;
import io.microraft.RaftNode;
import io.microraft.impl.local.LocalRaftEndpoint;
import io.microraft.impl.local.LocalRaftNodeRuntime;
import io.microraft.impl.local.SimpleStateMachine;
import io.microraft.impl.util.BaseTest;
import io.microraft.integration.StateMachine;
import io.microraft.report.RaftGroupMembers;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static io.microraft.impl.util.AssertionUtils.eventually;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assume.assumeTrue;

/*

   TO RUN THIS CODE SAMPLE ON YOUR MACHINE:

   $ git clone https://github.com/metanet/MicroRaft.git
   $ cd MicroRaft && ./mvnw clean test -Dtest=io.microraft.examples.ChangeRaftGroupMemberListTest -DfailIfNoTests=false -Pcode-sample

   YOU CAN SEE THIS CLASS AT:

   https://github.com/metanet/MicroRaft/blob/master/microraft/src/test/java/io/microraft/examples/ChangeRaftGroupMemberListTest.java

 */
public class ChangeRaftGroupMemberListTest
        extends BaseTest {

    private List<RaftEndpoint> initialMembers = Arrays
            .asList(LocalRaftEndpoint.newEndpoint(), LocalRaftEndpoint.newEndpoint(), LocalRaftEndpoint.newEndpoint());
    private List<LocalRaftNodeRuntime> runtimes = new ArrayList<>();
    private List<RaftNode> raftNodes = new ArrayList<>();
    private RaftNode leader;

    @Before
    public void init() {
        for (RaftEndpoint endpoint : initialMembers) {
            RaftNode raftNode = createRaftNode(endpoint);
            raftNode.start();
        }

        // wait until a leader is elected
        eventually(() -> assertThat(raftNodes.get(0).getTerm().getLeaderEndpoint()).isNotNull());

        // find the leader
        RaftEndpoint leaderEndpoint = raftNodes.get(0).getTerm().getLeaderEndpoint();
        leader = raftNodes.stream().filter(n -> n.getLocalEndpoint().equals(leaderEndpoint)).findFirst()
                          .orElseThrow(IllegalStateException::new);
    }

    @After
    public void tearDown() {
        for (RaftNode raftNode : raftNodes) {
            raftNode.terminate();
        }
    }

    @Test
    public void testChangeMemberListOfRaftGroup() {
        assumeTrue(leader != null);

        String value1 = "value1";
        leader.replicate(SimpleStateMachine.apply(value1)).join();

        RaftEndpoint endpoint4 = LocalRaftEndpoint.newEndpoint();
        // group members commit index of the initial Raft group members is 0.
        RaftGroupMembers newMemberList1 = leader.changeMembership(endpoint4, MembershipChangeMode.ADD, 0).join().getResult();
        System.out.println("New member list: " + newMemberList1.getMembers() + ", majority: " + newMemberList1.getMajority()
                                   + ", commit index: " + newMemberList1.getLogIndex());
        // endpoint4 is now part of the member list. Let's start its Raft node
        RaftNode raftNode4 = createRaftNode(endpoint4);
        raftNode4.start();

        String value2 = "value2";
        leader.replicate(SimpleStateMachine.apply(value2)).join();

        RaftEndpoint endpoint5 = LocalRaftEndpoint.newEndpoint();
        // we need to use newMemberList1's commit index for adding endpoint5
        RaftGroupMembers newMemberList2 = leader
                .changeMembership(endpoint5, MembershipChangeMode.ADD, newMemberList1.getLogIndex()).join().getResult();
        System.out.println("New member list: " + newMemberList2.getMembers() + ", majority: " + newMemberList2.getMajority()
                                   + ", commit index: " + newMemberList2.getLogIndex());
        // endpoint5 is also part of the member list now. Let's start its Raft node
        RaftNode raftNode5 = createRaftNode(endpoint5);
        raftNode5.start();

        String value3 = "value3";
        leader.replicate(SimpleStateMachine.apply(value3)).join();

        // wait until the new Raft nodes catch up with the leader
        eventually(() -> {
            assertThat(query(raftNode4).getResult()).isEqualTo(value3);
            assertThat(query(raftNode5).getResult()).isEqualTo(value3);
        });

        System.out.println(endpoint4.getId() + "'s local query result: " + query(raftNode4).getResult());
        System.out.println(endpoint5.getId() + "'s local query result: " + query(raftNode5).getResult());
    }

    private RaftNode createRaftNode(RaftEndpoint endpoint) {
        LocalRaftNodeRuntime runtime = new LocalRaftNodeRuntime(endpoint);
        runtimes.add(runtime);
        StateMachine stateMachine = new SimpleStateMachine();
        RaftNode raftNode = RaftNode.newBuilder().setGroupId("default").setLocalEndpoint(endpoint)
                                    .setInitialGroupMembers(initialMembers).setRuntime(runtime).setStateMachine(stateMachine)
                                    .build();
        raftNodes.add(raftNode);
        enableDiscovery(raftNode, runtime);

        return raftNode;
    }

    private void enableDiscovery(RaftNode raftNode, LocalRaftNodeRuntime runtime) {
        for (int i = 0; i < raftNodes.size(); i++) {
            RaftNode otherNode = raftNodes.get(i);
            if (otherNode != raftNode) {
                runtimes.get(i).discoverNode(raftNode);
                runtime.discoverNode(otherNode);
            }
        }
    }

    private Ordered<String> query(RaftNode raftNode) {
        return raftNode.<String>query(SimpleStateMachine.queryLast(), QueryPolicy.ANY_LOCAL, 0).join();
    }

}
