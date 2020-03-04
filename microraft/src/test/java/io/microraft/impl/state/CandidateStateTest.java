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
import org.junit.Before;
import org.junit.Test;

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
        RaftEndpoint endpoint = LocalRaftEndpoint.newEndpoint();

        assertThat(state.grantVote(endpoint)).isTrue();
        assertThat(state.grantVote(endpoint)).isFalse();

        assertThat(state.voteCount()).isEqualTo(1);
        assertThat(state.isMajorityGranted()).isFalse();
    }

    @Test
    public void test_grantVote_withMajority() {
        for (int i = 0; i < majority; i++) {
            RaftEndpoint endpoint = LocalRaftEndpoint.newEndpoint();
            assertThat(state.grantVote(endpoint)).isTrue();
        }

        assertThat(state.voteCount()).isEqualTo(majority);
        assertThat(state.isMajorityGranted()).isTrue();
    }

}
