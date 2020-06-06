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

package io.microraft.impl.state;

import io.microraft.RaftEndpoint;
import io.microraft.impl.local.LocalRaftEndpoint;
import io.microraft.impl.log.RaftLog;
import io.microraft.model.impl.log.DefaultLogEntryOrBuilder;
import io.microraft.test.util.RaftTestUtils;
import org.junit.Before;
import org.junit.Test;

import java.util.Collection;
import java.util.HashSet;

import static io.microraft.RaftRole.CANDIDATE;
import static io.microraft.RaftRole.FOLLOWER;
import static io.microraft.RaftRole.LEADER;
import static java.util.Arrays.asList;
import static org.assertj.core.api.Assertions.assertThat;

public class RaftStateTest {

    private RaftState state;
    private LocalRaftEndpoint localMember = LocalRaftEndpoint.newEndpoint();
    private Collection<RaftEndpoint> members;

    @Before
    public void setup() {
        members = new HashSet<>(asList(localMember, localMember, LocalRaftEndpoint.newEndpoint(), LocalRaftEndpoint.newEndpoint(),
                                       LocalRaftEndpoint.newEndpoint()));
        state = RaftState.create("default", localMember, members, 100);
    }

    @Test
    public void test_initialState() {
        assertThat(state.memberCount()).isEqualTo(members.size());

        assertThat(state.members()).isEqualTo(members);

        Collection<RaftEndpoint> remoteMembers = new HashSet<>(members);
        remoteMembers.remove(localMember);
        assertThat(state.remoteMembers()).isEqualTo(remoteMembers);

        assertThat(state.term()).isEqualTo(0);
        assertThat(state.role()).isEqualTo(FOLLOWER);
        assertThat(state.leader()).isNull();
        assertThat(state.commitIndex()).isEqualTo(0);
        assertThat(state.lastApplied()).isEqualTo(0);
        assertThat(state.majority()).isEqualTo(3);
        assertThat(state.votedEndpoint()).isNull();
        assertThat(state.leaderState()).isNull();
        assertThat(state.candidateState()).isNull();

        RaftLog log = state.log();
        assertThat(log.lastLogOrSnapshotTerm()).isEqualTo(0);
        assertThat(log.lastLogOrSnapshotIndex()).isEqualTo(0);
    }

    @Test
    public void test_Leader() {
        state.leader(localMember);
        assertThat(state.leader()).isEqualTo(localMember);
    }

    @Test
    public void test_commitIndex() {
        int ix = 123;
        state.commitIndex(ix);
        assertThat(state.commitIndex()).isEqualTo(ix);
    }

    @Test
    public void test_lastApplied() {
        int last = 123;
        state.lastApplied(last);
        assertThat(state.lastApplied()).isEqualTo(last);
    }

    @Test
    public void persistVote() {
        int term = 13;
        state.toFollower(term);
        state.grantVote(term, localMember);

        assertThat(state.term()).isEqualTo(term);
        assertThat(state.votedEndpoint()).isEqualTo(localMember);
    }

    @Test
    public void toFollower_fromCandidate() {
        state.toCandidate();

        int term = 23;
        state.toFollower(term);

        assertThat(state.term()).isEqualTo(term);
        assertThat(state.role()).isEqualTo(FOLLOWER);
        assertThat(state.leader()).isNull();
        assertThat(state.leaderState()).isNull();
        assertThat(state.candidateState()).isNull();
    }

    @Test
    public void toFollower_fromLeader() {
        state.toLeader();

        int term = 23;
        state.toFollower(term);

        assertThat(state.term()).isEqualTo(term);
        assertThat(state.role()).isEqualTo(FOLLOWER);
        assertThat(state.leader()).isNull();
        assertThat(state.leaderState()).isNull();
        assertThat(state.candidateState()).isNull();
    }

    @Test
    public void toCandidate_fromFollower() {
        int term = 23;
        state.toFollower(term);

        state.toCandidate();
        assertThat(state.role()).isEqualTo(CANDIDATE);
        assertThat(state.leaderState()).isNull();
        assertThat(state.term()).isEqualTo(term + 1);
        assertThat(state.votedEndpoint()).isEqualTo(localMember);

        CandidateState candidateState = state.candidateState();
        assertThat(candidateState).isNotNull();
        assertThat(candidateState.majority()).isEqualTo(state.majority());
        assertThat(candidateState.isMajorityGranted()).isFalse();
        assertThat(candidateState.voteCount()).isEqualTo(1);
    }

    @Test
    public void toLeader_fromCandidate() {
        state.toCandidate();

        int term = state.term();
        RaftLog log = state.log();
        log.appendEntry(new DefaultLogEntryOrBuilder().setTerm(term).setIndex(1).build());
        log.appendEntry(new DefaultLogEntryOrBuilder().setTerm(term).setIndex(2).build());
        log.appendEntry(new DefaultLogEntryOrBuilder().setTerm(term).setIndex(3).build());
        long lastLogIndex = log.lastLogOrSnapshotIndex();

        state.toLeader();

        assertThat(state.role()).isEqualTo(LEADER);
        assertThat(state.leader()).isEqualTo(localMember);
        assertThat(state.candidateState()).isNull();

        LeaderState leaderState = state.leaderState();
        assertThat(leaderState).isNotNull();

        for (RaftEndpoint endpoint : state.remoteMembers()) {
            FollowerState followerState = leaderState.getFollowerState(endpoint);
            assertThat(followerState.matchIndex()).isEqualTo(0);
            assertThat(followerState.nextIndex()).isEqualTo(lastLogIndex + 1);
        }

        long[] matchIndices = leaderState.matchIndices();
        assertThat(matchIndices.length).isEqualTo(state.remoteMembers().size() + 1);
        for (long index : matchIndices) {
            assertThat(index).isEqualTo(0);
        }
    }

    @Test
    public void isKnownEndpoint() {
        for (RaftEndpoint endpoint : members) {
            assertThat(state.isKnownMember(endpoint)).isTrue();
        }

        assertThat(state.isKnownMember(LocalRaftEndpoint.newEndpoint())).isFalse();
    }

    @Test
    public void test_majority_withOddMemberGroup() {
        test_majority(7);
    }

    private void test_majority(int count) {
        members = new HashSet<>();
        members.add(localMember);

        for (int i = 1; i < count; i++) {
            members.add(LocalRaftEndpoint.newEndpoint());
        }

        state = RaftState.create("default", localMember, members, 100);

        assertThat(state.majority()).isEqualTo(RaftTestUtils.majority(count));
    }

    @Test
    public void test_majority_withEvenMemberGroup() {
        test_majority(8);
    }

}
