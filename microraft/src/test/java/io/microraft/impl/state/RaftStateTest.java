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
import io.microraft.model.impl.log.DefaultRaftGroupMembersViewOrBuilder;
import io.microraft.model.log.RaftGroupMembersView;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;

import static io.microraft.RaftRole.CANDIDATE;
import static io.microraft.RaftRole.FOLLOWER;
import static io.microraft.RaftRole.LEADER;
import static io.microraft.RaftRole.LEARNER;
import static io.microraft.impl.local.LocalRaftEndpoint.newEndpoint;
import static io.microraft.test.util.RaftTestUtils.majority;
import static java.util.Arrays.asList;
import static org.assertj.core.api.Assertions.assertThat;

public class RaftStateTest {

    @Rule
    public ExpectedException exception = ExpectedException.none();

    private LocalRaftEndpoint localEndpoint = newEndpoint();
    private Collection<RaftEndpoint> initialEndpoints = new LinkedHashSet<>(
            asList(localEndpoint, newEndpoint(), newEndpoint(), newEndpoint(), newEndpoint()));
    private RaftGroupMembersView groupMembers = new DefaultRaftGroupMembersViewOrBuilder().setLogIndex(0)
            .setMembers(initialEndpoints).setVotingMembers(initialEndpoints).build();
    private RaftState state;

    @Before
    public void setup() {
        state = RaftState.create("default", localEndpoint, groupMembers, 100);
    }

    @Test
    public void test_initialState() {
        assertThat(state.memberCount()).isEqualTo(initialEndpoints.size());

        assertThat(state.members()).isEqualTo(initialEndpoints);

        Collection<RaftEndpoint> remoteMembers = new HashSet<>(initialEndpoints);
        remoteMembers.remove(localEndpoint);
        assertThat(state.remoteMembers()).isEqualTo(remoteMembers);

        assertThat(state.term()).isEqualTo(0);
        assertThat(state.role()).isEqualTo(FOLLOWER);
        assertThat(state.leader()).isNull();
        assertThat(state.commitIndex()).isEqualTo(0);
        assertThat(state.lastApplied()).isEqualTo(0);
        assertThat(state.leaderElectionQuorumSize()).isEqualTo(3);
        assertThat(state.votedEndpoint()).isNull();
        assertThat(state.leaderState()).isNull();
        assertThat(state.candidateState()).isNull();

        RaftLog log = state.log();
        assertThat(log.lastLogOrSnapshotTerm()).isEqualTo(0);
        assertThat(log.lastLogOrSnapshotIndex()).isEqualTo(0);
    }

    @Test
    public void test_leader() {
        state.leader(localEndpoint);
        assertThat(state.leader()).isEqualTo(localEndpoint);
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
        state.grantVote(term, localEndpoint);

        assertThat(state.term()).isEqualTo(term);
        assertThat(state.votedEndpoint()).isEqualTo(localEndpoint);
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
        assertThat(state.votedEndpoint()).isEqualTo(localEndpoint);

        CandidateState candidateState = state.candidateState();
        assertThat(candidateState).isNotNull();
        assertThat(candidateState.majority()).isEqualTo(state.leaderElectionQuorumSize());
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
        assertThat(state.leader()).isEqualTo(localEndpoint);
        assertThat(state.candidateState()).isNull();

        LeaderState leaderState = state.leaderState();
        assertThat(leaderState).isNotNull();

        for (RaftEndpoint endpoint : state.remoteMembers()) {
            FollowerState followerState = leaderState.getFollowerState(endpoint);
            assertThat(followerState.matchIndex()).isEqualTo(0);
            assertThat(followerState.nextIndex()).isEqualTo(lastLogIndex + 1);
        }

        long[] matchIndices = leaderState.matchIndices(state.remoteMembers());
        assertThat(matchIndices.length).isEqualTo(state.remoteMembers().size() + 1);
        for (long index : matchIndices) {
            assertThat(index).isEqualTo(0);
        }
    }

    @Test
    public void isKnownEndpoint() {
        for (RaftEndpoint endpoint : initialEndpoints) {
            assertThat(state.isKnownMember(endpoint)).isTrue();
        }

        assertThat(state.isKnownMember(newEndpoint())).isFalse();
    }

    @Test
    public void testQuorumSizesOfOddSizedCluster() {
        int memberCount = 7;
        initialEndpoints = new HashSet<>();
        initialEndpoints.add(localEndpoint);

        for (int i = 1; i < memberCount; i++) {
            initialEndpoints.add(newEndpoint());
        }

        groupMembers = new DefaultRaftGroupMembersViewOrBuilder().setLogIndex(0).setMembers(initialEndpoints)
                .setVotingMembers(initialEndpoints).build();

        state = RaftState.create("default", localEndpoint, groupMembers, 100);

        int majorityQuorumSize = majority(memberCount);
        assertThat(state.leaderElectionQuorumSize()).isEqualTo(majorityQuorumSize);
        assertThat(state.logReplicationQuorumSize()).isEqualTo(majorityQuorumSize);
    }

    @Test
    public void testQuorumSizesOfEvenSizedCluster() {
        int memberCount = 8;
        initialEndpoints = new HashSet<>();
        initialEndpoints.add(localEndpoint);

        for (int i = 1; i < memberCount; i++) {
            initialEndpoints.add(newEndpoint());
        }

        groupMembers = new DefaultRaftGroupMembersViewOrBuilder().setLogIndex(0).setMembers(initialEndpoints)
                .setVotingMembers(initialEndpoints).build();

        state = RaftState.create("default", localEndpoint, groupMembers, 100);

        int majorityQuorumSize = majority(memberCount);
        assertThat(state.leaderElectionQuorumSize()).isEqualTo(majorityQuorumSize);
        assertThat(state.logReplicationQuorumSize()).isEqualTo(majorityQuorumSize - 1);
    }

    @Test
    public void test_initialStateOfJoinedMember() {
        localEndpoint = newEndpoint();

        state = RaftState.create("default", localEndpoint, groupMembers, 100);

        List<RaftEndpoint> newMemberList = new ArrayList<>(initialEndpoints);
        newMemberList.add(localEndpoint);
        state.updateGroupMembers(1, newMemberList, initialEndpoints);

        assertThat(state.remoteMembers()).isEqualTo(initialEndpoints);
        assertThat(state.remoteVotingMembers()).isEqualTo(initialEndpoints);
        assertThat(state.memberCount()).isEqualTo(initialEndpoints.size() + 1);
        assertThat(state.votingMemberCount()).isEqualTo(initialEndpoints.size());

        assertThat(state.term()).isEqualTo(0);
        assertThat(state.role()).isEqualTo(LEARNER);
        assertThat(state.leader()).isNull();
        assertThat(state.commitIndex()).isEqualTo(0);
        assertThat(state.lastApplied()).isEqualTo(0);
        assertThat(state.leaderElectionQuorumSize()).isEqualTo(3);
        assertThat(state.votedEndpoint()).isNull();
        assertThat(state.leaderState()).isNull();
        assertThat(state.candidateState()).isNull();

        RaftLog log = state.log();
        assertThat(log.lastLogOrSnapshotTerm()).isEqualTo(0);
        assertThat(log.lastLogOrSnapshotIndex()).isEqualTo(0);
    }

    @Test
    public void test_promotionToVotingMember() {
        localEndpoint = newEndpoint();

        state = RaftState.create("default", localEndpoint, groupMembers, 100);

        List<RaftEndpoint> newMemberList = new ArrayList<>(initialEndpoints);
        newMemberList.add(localEndpoint);
        state.updateGroupMembers(1, newMemberList, newMemberList);

        assertThat(state.effectiveGroupMembers().getVotingMembersList()).isEqualTo(newMemberList);
        assertThat(state.votingMemberCount()).isEqualTo(initialEndpoints.size() + 1);
        assertThat(state.role()).isEqualTo(FOLLOWER);
    }

    @Test
    public void test_revertVotingMemberPromotion() {
        localEndpoint = newEndpoint();

        state = RaftState.create("default", localEndpoint, groupMembers, 100);

        List<RaftEndpoint> newMemberList = new ArrayList<>(initialEndpoints);
        newMemberList.add(localEndpoint);
        state.updateGroupMembers(1, newMemberList, initialEndpoints);

        state.revertGroupMembers();

        assertThat(state.role()).isEqualTo(LEARNER);
    }

    @Test
    public void test_revertVotingMemberPromotionFailsWhileCandidate() {
        localEndpoint = newEndpoint();

        state = RaftState.create("default", localEndpoint, groupMembers, 100);

        List<RaftEndpoint> newMemberList = new ArrayList<>(initialEndpoints);
        newMemberList.add(localEndpoint);
        state.updateGroupMembers(1, newMemberList, newMemberList);

        state.toCandidate();

        state.revertGroupMembers();

        assertThat(state.role()).isEqualTo(LEARNER);
    }

}
