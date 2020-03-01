package com.hazelcast.raft.impl.state;

import com.hazelcast.raft.RaftEndpoint;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static com.hazelcast.raft.impl.local.LocalRaftEndpoint.newEndpoint;
import static com.hazelcast.raft.impl.util.RandomPicker.getRandomInt;
import static java.util.Arrays.asList;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * @author mdogan
 * @author metanet
 */
public class LeaderStateTest {

    private LeaderState state;
    private Set<RaftEndpoint> remoteEndpoints;
    private int lastLogIndex;

    @Before
    public void setUp() {
        lastLogIndex = 123;
        remoteEndpoints = new HashSet<>(asList(newEndpoint(), newEndpoint(), newEndpoint(), newEndpoint()));
        state = new LeaderState(remoteEndpoints, lastLogIndex);
    }

    @Test
    public void test_initialState() {
        for (RaftEndpoint endpoint : remoteEndpoints) {
            FollowerState followerState = state.getFollowerState(endpoint);

            assertThat(followerState.matchIndex()).isEqualTo(0);
            assertThat(followerState.nextIndex()).isEqualTo(lastLogIndex + 1);
        }

        long[] matchIndices = state.matchIndices();
        assertThat(matchIndices.length).isEqualTo(remoteEndpoints.size() + 1);

        for (long index : matchIndices) {
            assertThat(index).isEqualTo(0);
        }
    }

    @Test
    public void test_nextIndex() {
        Map<RaftEndpoint, Integer> indices = new HashMap<>();
        for (RaftEndpoint endpoint : remoteEndpoints) {
            int index = 1 + getRandomInt(100);
            state.getFollowerState(endpoint).nextIndex(index);
            indices.put(endpoint, index);
        }

        for (RaftEndpoint endpoint : remoteEndpoints) {
            int index = indices.get(endpoint);
            assertThat(state.getFollowerState(endpoint).nextIndex()).isEqualTo(index);
        }
    }

    @Test
    public void test_matchIndex() {
        Map<RaftEndpoint, Long> indices = new HashMap<>();
        for (RaftEndpoint endpoint : remoteEndpoints) {
            long index = 1 + getRandomInt(100);
            state.getFollowerState(endpoint).matchIndex(index);
            indices.put(endpoint, index);
        }

        for (RaftEndpoint endpoint : remoteEndpoints) {
            long index = indices.get(endpoint);
            assertThat(state.getFollowerState(endpoint).matchIndex()).isEqualTo(index);
        }

        long[] matchIndices = state.matchIndices();
        assertThat(matchIndices.length).isEqualTo(indices.size() + 1);

        for (int i = 0; i < matchIndices.length - 1; i++) {
            long index = matchIndices[i];
            assertThat(indices.containsValue(index)).isTrue();
        }
    }

}
