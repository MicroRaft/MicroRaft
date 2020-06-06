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
import io.microraft.impl.util.RandomPicker;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static java.util.Arrays.asList;
import static org.assertj.core.api.Assertions.assertThat;

public class LeaderStateTest {

    private LeaderState state;
    private Set<RaftEndpoint> remoteEndpoints;
    private int lastLogIndex;

    @Before
    public void setUp() {
        lastLogIndex = 123;
        remoteEndpoints = new HashSet<>(
                asList(LocalRaftEndpoint.newEndpoint(), LocalRaftEndpoint.newEndpoint(), LocalRaftEndpoint.newEndpoint(),
                       LocalRaftEndpoint.newEndpoint()));
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
            int index = 1 + RandomPicker.getRandomInt(100);
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
            long index = 1 + RandomPicker.getRandomInt(100);
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
