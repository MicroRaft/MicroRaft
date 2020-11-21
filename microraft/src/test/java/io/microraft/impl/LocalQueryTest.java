/*
 * Original work Copyright (c) 2008-2020, Hazelcast, Inc.
 * Modified work Copyright 2020, MicroRaft.
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

package io.microraft.impl;

import io.microraft.MembershipChangeMode;
import io.microraft.Ordered;
import io.microraft.RaftEndpoint;
import io.microraft.RaftNode;
import io.microraft.exception.LaggingCommitIndexException;
import io.microraft.exception.NotLeaderException;
import io.microraft.impl.local.LocalRaftGroup;
import io.microraft.model.message.AppendEntriesRequest;
import io.microraft.test.util.BaseTest;
import org.junit.After;
import org.junit.Test;

import java.util.concurrent.CompletionException;

import static io.microraft.QueryPolicy.ANY_LOCAL;
import static io.microraft.QueryPolicy.LEADER_LOCAL;
import static io.microraft.impl.local.SimpleStateMachine.applyValue;
import static io.microraft.impl.local.SimpleStateMachine.queryLastValue;
import static io.microraft.test.util.AssertionUtils.eventually;
import static io.microraft.test.util.RaftTestUtils.TEST_RAFT_CONFIG;
import static io.microraft.test.util.RaftTestUtils.getCommitIndex;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.fail;

public class LocalQueryTest
        extends BaseTest {

    private LocalRaftGroup group;

    @After
    public void destroy() {
        if (group != null) {
            group.destroy();
        }
    }

    @Test(timeout = 300_000) public void when_queryFromLeader_withoutAnyCommit_then_returnDefaultValue() {
        group = LocalRaftGroup.start(3);

        RaftNodeImpl leader = group.waitUntilLeaderElected();

        Ordered<Object> o = leader.query(queryLastValue(), LEADER_LOCAL, 0).join();
        assertThat(o.getResult()).isNull();
        assertThat(o.getCommitIndex()).isEqualTo(0);
    }

    @Test(timeout = 300_000) public void when_queryFromLeaderWithCommitIndex_withoutAnyCommit_then_fail() {
        group = LocalRaftGroup.start(3);

        RaftNodeImpl leader = group.waitUntilLeaderElected();

        try {
            leader.query(queryLastValue(), LEADER_LOCAL, getCommitIndex(leader) + 1).join();
            fail();
        } catch (CompletionException e) {
            assertThat(e).hasCauseInstanceOf(LaggingCommitIndexException.class);
        }
    }

    @Test(timeout = 300_000) public void when_queryFromFollower_withoutAnyCommit_then_returnDefaultValue() {
        group = LocalRaftGroup.start(3);

        RaftNode leader = group.waitUntilLeaderElected();
        RaftNodeImpl follower = group.getAnyNodeExcept(leader.getLocalEndpoint());

        Ordered<Object> o = follower.query(queryLastValue(), ANY_LOCAL, 0).join();
        assertThat(o.getResult()).isNull();
        assertThat(o.getCommitIndex()).isEqualTo(0);
    }

    @Test(timeout = 300_000) public void when_queryFromFollowerWithCommitIndex_withoutAnyCommit_then_returnDefaultValue() {
        group = LocalRaftGroup.start(3);

        RaftNode leader = group.waitUntilLeaderElected();
        RaftNodeImpl follower = group.getAnyNodeExcept(leader.getLocalEndpoint());

        try {
            follower.query(queryLastValue(), ANY_LOCAL, getCommitIndex(follower) + 1).join();
            fail();
        } catch (CompletionException e) {
            assertThat(e).hasCauseInstanceOf(LaggingCommitIndexException.class);
        }
    }

    @Test(timeout = 300_000) public void when_queryFromLeaderWithoutCommitIndex_onStableRaftGroup_then_readLatestValue() {
        group = LocalRaftGroup.start(3);

        RaftNodeImpl leader = group.waitUntilLeaderElected();

        int count = 3;
        for (int i = 1; i <= count; i++) {
            leader.replicate(applyValue("value" + i)).join();
        }

        long commitIndex = getCommitIndex(leader);
        Ordered<Object> result = leader.query(queryLastValue(), LEADER_LOCAL, 0).join();
        assertThat(result.getResult()).isEqualTo("value" + count);
        assertThat(result.getCommitIndex()).isEqualTo(commitIndex);
    }

    @Test(timeout = 300_000) public void when_queryFromLeaderWithCommitIndex_onStableRaftGroup_then_readLatestValue() {
        group = LocalRaftGroup.start(3);

        RaftNodeImpl leader = group.waitUntilLeaderElected();

        int count = 3;
        for (int i = 1; i <= count; i++) {
            leader.replicate(applyValue("value" + i)).join();
        }

        long commitIndex = getCommitIndex(leader);
        Ordered<Object> result = leader.query(queryLastValue(), LEADER_LOCAL, commitIndex).join();
        assertThat(result.getResult()).isEqualTo("value" + count);
        assertThat(result.getCommitIndex()).isEqualTo(commitIndex);
    }

    @Test(timeout = 300_000) public void when_queryFromLeaderWithFurtherCommitIndex_onStableRaftGroup_then_fail() {
        group = LocalRaftGroup.start(3);

        RaftNodeImpl leader = group.waitUntilLeaderElected();

        int count = 3;
        for (int i = 1; i <= count; i++) {
            leader.replicate(applyValue("value" + i)).join();
        }

        long commitIndex = getCommitIndex(leader);
        try {
            leader.query(queryLastValue(), LEADER_LOCAL, commitIndex + 1).join();
            fail();
        } catch (CompletionException e) {
            assertThat(e).hasCauseInstanceOf(LaggingCommitIndexException.class);
        }
    }

    @Test(timeout = 300_000) public void when_queryFromFollower_withLeaderLocalPolicy_then_fail() {
        group = LocalRaftGroup.start(3);

        RaftNodeImpl leader = group.waitUntilLeaderElected();
        leader.replicate(applyValue("value")).join();

        try {
            group.getAnyNodeExcept(leader.getLocalEndpoint()).query(queryLastValue(), LEADER_LOCAL, 0).join();
            fail();
        } catch (CompletionException e) {
            assertThat(e).hasCauseInstanceOf(NotLeaderException.class);
        }
    }

    @Test(timeout = 300_000) public void when_queryFromFollowerWithoutCommitIndex_onStableRaftGroup_then_readLatestValue() {
        group = LocalRaftGroup.start(3);

        RaftNodeImpl leader = group.waitUntilLeaderElected();

        int count = 3;
        for (int i = 1; i <= count; i++) {
            leader.replicate(applyValue("value" + i)).join();
        }

        String latestValue = "value" + count;

        eventually(() -> {
            long commitIndex = getCommitIndex(leader);
            for (RaftNodeImpl follower : group.<RaftNodeImpl>getNodesExcept(leader.getLocalEndpoint())) {
                assertThat(getCommitIndex(follower)).isEqualTo(commitIndex);
                Ordered<Object> result = follower.query(queryLastValue(), ANY_LOCAL, 0).join();
                assertThat(result.getResult()).isEqualTo(latestValue);
                assertThat(result.getCommitIndex()).isEqualTo(commitIndex);
            }
        });
    }

    @Test(timeout = 300_000) public void when_queryFromFollowerWithCommitIndex_onStableRaftGroup_then_readLatestValue() {
        group = LocalRaftGroup.start(3);

        RaftNodeImpl leader = group.waitUntilLeaderElected();

        int count = 3;
        for (int i = 1; i <= count; i++) {
            leader.replicate(applyValue("value" + i)).join();
        }

        String latestValue = "value" + count;

        eventually(() -> {
            long commitIndex = getCommitIndex(leader);
            for (RaftNodeImpl follower : group.<RaftNodeImpl>getNodesExcept(leader.getLocalEndpoint())) {
                assertThat(getCommitIndex(follower)).isEqualTo(commitIndex);
                Ordered<Object> result = follower.query(queryLastValue(), ANY_LOCAL, commitIndex).join();
                assertThat(result.getResult()).isEqualTo(latestValue);
                assertThat(result.getCommitIndex()).isEqualTo(commitIndex);
            }
        });
    }

    @Test(timeout = 300_000) public void when_queryFromSlowFollower_then_readStaleValue() {
        group = LocalRaftGroup.start(3);

        RaftNodeImpl leader = group.waitUntilLeaderElected();
        RaftNodeImpl slowFollower = group.getAnyNodeExcept(leader.getLocalEndpoint());

        Object firstValue = "value1";
        leader.replicate(applyValue(firstValue)).join();
        long leaderCommitIndex = getCommitIndex(leader);

        eventually(() -> assertThat(getCommitIndex(slowFollower)).isEqualTo(leaderCommitIndex));

        group.dropMessagesTo(leader.getLocalEndpoint(), slowFollower.getLocalEndpoint(), AppendEntriesRequest.class);

        leader.replicate(applyValue("value2")).join();

        Ordered<Object> result = slowFollower.query(queryLastValue(), ANY_LOCAL, 0).join();
        assertThat(result.getResult()).isEqualTo(firstValue);
        assertThat(result.getCommitIndex()).isEqualTo(leaderCommitIndex);
    }

    @Test(timeout = 300_000) public void when_queryFromSlowFollower_then_eventuallyReadLatestValue() {
        group = LocalRaftGroup.start(3);

        RaftNodeImpl leader = group.waitUntilLeaderElected();
        leader.replicate(applyValue("value1")).join();

        RaftNodeImpl slowFollower = group.getAnyNodeExcept(leader.getLocalEndpoint());
        group.dropMessagesTo(leader.getLocalEndpoint(), slowFollower.getLocalEndpoint(), AppendEntriesRequest.class);

        Object lastValue = "value2";
        leader.replicate(applyValue(lastValue)).join();

        group.allowAllMessagesTo(leader.getLocalEndpoint(), slowFollower.getLocalEndpoint());

        eventually(() -> {
            long commitIndex = getCommitIndex(leader);
            Ordered<Object> result = slowFollower.query(queryLastValue(), ANY_LOCAL, 0).join();
            assertThat(result.getResult()).isEqualTo(lastValue);
            assertThat(result.getCommitIndex()).isEqualTo(commitIndex);
        });
    }

    @Test(timeout = 300_000) public void when_queryFromSplitLeaderWithAnyLocal_then_readStaleValue() {
        group = LocalRaftGroup.start(3, TEST_RAFT_CONFIG);

        RaftNodeImpl leader = group.waitUntilLeaderElected();

        Object firstValue = "value1";
        leader.replicate(applyValue(firstValue)).join();
        long firstCommitIndex = getCommitIndex(leader);

        eventually(() -> {
            for (RaftNodeImpl node : group.getNodes()) {
                assertThat(getCommitIndex(node)).isEqualTo(firstCommitIndex);
            }
        });

        RaftNodeImpl followerNode = group.getAnyNodeExcept(leader.getLocalEndpoint());
        group.splitMembers(leader.getLocalEndpoint());

        eventually(() -> {
            RaftEndpoint leaderEndpoint = followerNode.getLeaderEndpoint();
            assertThat(leaderEndpoint).isNotNull().isNotEqualTo(leader.getLocalEndpoint());
        });

        RaftNodeImpl newLeader = group.getNode(followerNode.getLeaderEndpoint());
        Object lastValue = "value2";
        newLeader.replicate(applyValue(lastValue)).join();
        long lastCommitIndex = getCommitIndex(newLeader);

        Ordered<Object> result1 = newLeader.query(queryLastValue(), ANY_LOCAL, 0).join();
        assertThat(result1.getResult()).isEqualTo(lastValue);
        assertThat(result1.getCommitIndex()).isEqualTo(lastCommitIndex);

        Ordered<Object> result2 = leader.query(queryLastValue(), ANY_LOCAL, 0).join();
        assertThat(result2.getResult()).isEqualTo(firstValue);
        assertThat(result2.getCommitIndex()).isEqualTo(firstCommitIndex);
    }

    @Test(timeout = 300_000) public void when_queryFromSplitLeaderWithLeaderLocal_then__readFailsAfterLeaderDemotesToFollower() {
        group = LocalRaftGroup.start(3, TEST_RAFT_CONFIG);

        RaftNodeImpl leader = group.waitUntilLeaderElected();

        Object firstValue = "value1";
        leader.replicate(applyValue(firstValue)).join();
        long firstCommitIndex = getCommitIndex(leader);

        eventually(() -> {
            for (RaftNodeImpl node : group.getNodes()) {
                assertThat(getCommitIndex(node)).isEqualTo(firstCommitIndex);
            }
        });

        group.splitMembers(leader.getLocalEndpoint());

        eventually(() -> {
            try {
                leader.query(queryLastValue(), LEADER_LOCAL, 0).join();
                fail();
            } catch (CompletionException e) {
                assertThat(e).hasCauseInstanceOf(NotLeaderException.class);
            }
        });
    }

    @Test(timeout = 300_000) public void when_queryFromSplitLeader_then_eventuallyReadLatestValue() {
        group = LocalRaftGroup.start(3, TEST_RAFT_CONFIG);

        RaftNodeImpl leader = group.waitUntilLeaderElected();

        Object firstValue = "value1";
        leader.replicate(applyValue(firstValue)).join();
        long leaderCommitIndex = getCommitIndex(leader);

        eventually(() -> {
            for (RaftNodeImpl node : group.getNodes()) {
                assertThat(getCommitIndex(node)).isEqualTo(leaderCommitIndex);
            }
        });

        RaftNodeImpl followerNode = group.getAnyNodeExcept(leader.getLocalEndpoint());
        group.splitMembers(leader.getLocalEndpoint());

        eventually(() -> {
            RaftEndpoint leaderEndpoint = followerNode.getLeaderEndpoint();
            assertThat(leaderEndpoint).isNotNull().isNotEqualTo(leader.getLocalEndpoint());
        });

        RaftNodeImpl newLeader = group.getNode(followerNode.getLeaderEndpoint());
        Object lastValue = "value2";
        newLeader.replicate(applyValue(lastValue)).join();
        long lastCommitIndex = getCommitIndex(newLeader);

        group.merge();

        eventually(() -> {
            Ordered<Object> result = leader.query(queryLastValue(), ANY_LOCAL, 0).join();
            assertThat(result.getResult()).isEqualTo(lastValue);
            assertThat(result.getCommitIndex()).isEqualTo(lastCommitIndex);
        });
    }

    @Test(timeout = 300_000) public void when_queryFromLearner_then_readLatestValue() {
        group = LocalRaftGroup.start(3, TEST_RAFT_CONFIG);

        RaftNodeImpl leader = group.waitUntilLeaderElected();

        Object value = "value";
        leader.replicate(applyValue(value)).join();

        RaftNodeImpl newFollower = group.createNewNode();

        leader.changeMembership(newFollower.getLocalEndpoint(), MembershipChangeMode.ADD_LEARNER, 0).join();

        eventually(() -> assertThat(getCommitIndex(newFollower)).isEqualTo(getCommitIndex(leader)));

        Ordered<Object> result = newFollower.query(queryLastValue(), ANY_LOCAL, 0).join();

        assertThat(result.getCommitIndex()).isEqualTo(getCommitIndex(leader));
        assertThat(result.getResult()).isEqualTo(value);
    }

    @Test(timeout = 300_000) public void when_queryFromLearnerWithStaleCommitIndex_then_readStaleValue() {
        group = LocalRaftGroup.start(3, TEST_RAFT_CONFIG);

        RaftNodeImpl leader = group.waitUntilLeaderElected();

        Object value1 = "value1";
        leader.replicate(applyValue(value1)).join();

        RaftNodeImpl newFollower = group.createNewNode();

        long commitIndex1 = leader.changeMembership(newFollower.getLocalEndpoint(), MembershipChangeMode.ADD_LEARNER, 0)
                                  .join()
                                  .getCommitIndex();

        eventually(() -> assertThat(getCommitIndex(newFollower)).isEqualTo(commitIndex1));

        group.dropMessagesTo(leader.getLocalEndpoint(), newFollower.getLocalEndpoint(), AppendEntriesRequest.class);

        Object value2 = "value2";
        leader.replicate(applyValue(value2)).join();

        Ordered<Object> result = newFollower.query(queryLastValue(), ANY_LOCAL, commitIndex1).join();
        assertThat(result.getResult()).isEqualTo(value1);
        assertThat(result.getCommitIndex()).isEqualTo(commitIndex1);
    }

    @Test(timeout = 300_000) public void when_queryFromLearnerWithInvalidCommitIndex_then_queryFails() {
        group = LocalRaftGroup.start(3, TEST_RAFT_CONFIG);

        RaftNodeImpl leader = group.waitUntilLeaderElected();

        Object value = "value";
        leader.replicate(applyValue(value)).join();

        RaftNodeImpl newFollower = group.createNewNode();

        leader.changeMembership(newFollower.getLocalEndpoint(), MembershipChangeMode.ADD_LEARNER, 0).join();

        eventually(() -> assertThat(getCommitIndex(newFollower)).isEqualTo(getCommitIndex(leader)));

        try {
            newFollower.query(queryLastValue(), ANY_LOCAL, getCommitIndex(leader) + 1).join();
            fail();
        } catch (CompletionException e) {
            assertThat(e).hasCauseInstanceOf(LaggingCommitIndexException.class);
        }
    }

}
