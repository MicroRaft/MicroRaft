package com.hazelcast.raft.impl.state;

import com.hazelcast.raft.RaftEndpoint;
import com.hazelcast.raft.impl.local.LocalRaftEndpoint;
import com.hazelcast.raft.impl.log.LogEntry;
import com.hazelcast.raft.impl.log.RaftLog;
import org.junit.Before;
import org.junit.Test;

import java.util.Collection;
import java.util.HashSet;

import static com.hazelcast.raft.RaftRole.CANDIDATE;
import static com.hazelcast.raft.RaftRole.FOLLOWER;
import static com.hazelcast.raft.RaftRole.LEADER;
import static com.hazelcast.raft.impl.local.LocalRaftEndpoint.newEndpoint;
import static com.hazelcast.raft.impl.util.RaftUtil.majority;
import static java.util.Arrays.asList;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * @author mdogan
 * @author metanet
 */
public class RaftStateTest {

    private RaftState state;
    private String name = "randomName";
    private LocalRaftEndpoint localMember = newEndpoint();
    private Collection<RaftEndpoint> members;

    @Before
    public void setup() {
        members = new HashSet<>(asList(localMember, localMember, newEndpoint(), newEndpoint(), newEndpoint()));
        state = new RaftState("default", localMember, members, 100);
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
        assertThat(state.votedFor()).isNull();
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
        state.persistVote(term, localMember);

        assertThat(state.term()).isEqualTo(term);
        assertThat(state.votedFor()).isEqualTo(localMember);
    }

    @Test
    public void toFollower_fromCandidate() {
        state.toCandidate(false);

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

        state.toCandidate(false);
        assertThat(state.role()).isEqualTo(CANDIDATE);
        assertThat(state.leaderState()).isNull();
        assertThat(state.term()).isEqualTo(term + 1);
        assertThat(state.votedFor()).isEqualTo(localMember);

        CandidateState candidateState = state.candidateState();
        assertThat(candidateState).isNotNull();
        assertThat(candidateState.majority()).isEqualTo(state.majority());
        assertThat(candidateState.isMajorityGranted()).isFalse();
        assertThat(candidateState.voteCount()).isEqualTo(1);
    }

    @Test
    public void toLeader_fromCandidate() {
        state.toCandidate(false);

        int term = state.term();
        RaftLog log = state.log();
        log.appendEntries(new LogEntry(term, 1, null), new LogEntry(term, 2, null), new LogEntry(term, 3, null));
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

        assertThat(state.isKnownMember(newEndpoint())).isFalse();
    }

    @Test
    public void test_majority_withOddMemberGroup() {
        test_majority(7);
    }

    @Test
    public void test_majority_withEvenMemberGroup() {
        test_majority(8);
    }

    private void test_majority(int count) {
        members = new HashSet<>();
        members.add(localMember);

        for (int i = 1; i < count; i++) {
            members.add(newEndpoint());
        }

        state = new RaftState("default", localMember, members, 100);

        assertThat(state.majority()).isEqualTo(majority(count));
    }
}
