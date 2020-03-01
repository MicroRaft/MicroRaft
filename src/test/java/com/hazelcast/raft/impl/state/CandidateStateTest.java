package com.hazelcast.raft.impl.state;

import com.hazelcast.raft.RaftEndpoint;
import org.junit.Before;
import org.junit.Test;

import static com.hazelcast.raft.impl.local.LocalRaftEndpoint.newEndpoint;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * @author mdogan
 * @author metanet
 */
public class CandidateStateTest {

    private CandidateState state;
    private int majority;

    @Before
    public void setUp() {
        majority = 3;
        state = new CandidateState(majority);
    }

    @Test
    public void test_initialState() {
        assertThat(state.majority()).isEqualTo(majority);
        assertThat(state.voteCount()).isEqualTo(0);
        assertThat(state.isMajorityGranted()).isFalse();
    }

    @Test
    public void test_grantVote_withoutMajority() {
        RaftEndpoint endpoint = newEndpoint();

        assertThat(state.grantVote(endpoint)).isTrue();
        assertThat(state.grantVote(endpoint)).isFalse();

        assertThat(state.voteCount()).isEqualTo(1);
        assertThat(state.isMajorityGranted()).isFalse();
    }

    @Test
    public void test_grantVote_withMajority() {
        for (int i = 0; i < majority; i++) {
            RaftEndpoint endpoint = newEndpoint();
            assertThat(state.grantVote(endpoint)).isTrue();
        }

        assertThat(state.voteCount()).isEqualTo(majority);
        assertThat(state.isMajorityGranted()).isTrue();
    }

}
