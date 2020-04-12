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

package io.microraft.impl.local;

import io.microraft.RaftEndpoint;
import io.microraft.model.message.AppendEntriesFailureResponse;
import io.microraft.model.message.AppendEntriesRequest;
import io.microraft.model.message.AppendEntriesSuccessResponse;
import io.microraft.model.message.InstallSnapshotRequest;
import io.microraft.model.message.InstallSnapshotResponse;
import io.microraft.model.message.PreVoteRequest;
import io.microraft.model.message.PreVoteResponse;
import io.microraft.model.message.TriggerLeaderElectionRequest;
import io.microraft.model.message.VoteRequest;
import io.microraft.model.message.VoteResponse;
import io.microraft.test.util.BaseTest;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

public class FirewallTest
        extends BaseTest {

    private final Firewall firewall = new Firewall();

    @Test
    public void when_messageIsDroppedForEndpoint_then_messageIsNotSentToThatEndpoint() {
        RaftEndpoint endpoint = LocalRaftEndpoint.newEndpoint();

        firewall.dropMessagesTo(endpoint, AppendEntriesRequest.class);

        assertThat(firewall.shouldDropMessage(endpoint, mock(AppendEntriesRequest.class))).isTrue();
    }

    @Test
    public void when_messageIsDroppedForEndpoint_then_messageIsSentToOtherEndpoint() {
        RaftEndpoint endpoint1 = LocalRaftEndpoint.newEndpoint();
        RaftEndpoint endpoint2 = LocalRaftEndpoint.newEndpoint();

        firewall.dropMessagesTo(endpoint1, AppendEntriesRequest.class);

        assertThat(firewall.shouldDropMessage(endpoint2, mock(AppendEntriesRequest.class))).isFalse();
    }

    @Test
    public void when_messageIsDroppedForEndpoint_then_otherMessageTypeIsSentToThatEndpoint() {
        RaftEndpoint endpoint = LocalRaftEndpoint.newEndpoint();

        firewall.dropMessagesTo(endpoint, VoteRequest.class);

        assertThat(firewall.shouldDropMessage(endpoint, mock(AppendEntriesRequest.class))).isFalse();
    }

    @Test
    public void when_allMessagesAreDroppedForEndpoint_then_noMessageIsSentToThatEndpoint() {
        RaftEndpoint endpoint = LocalRaftEndpoint.newEndpoint();

        firewall.dropAllMessagesTo(endpoint);

        assertThat(firewall.shouldDropMessage(endpoint, mock(AppendEntriesRequest.class))).isTrue();
        assertThat(firewall.shouldDropMessage(endpoint, mock(AppendEntriesSuccessResponse.class))).isTrue();
        assertThat(firewall.shouldDropMessage(endpoint, mock(AppendEntriesFailureResponse.class))).isTrue();
        assertThat(firewall.shouldDropMessage(endpoint, mock(VoteRequest.class))).isTrue();
        assertThat(firewall.shouldDropMessage(endpoint, mock(VoteResponse.class))).isTrue();
        assertThat(firewall.shouldDropMessage(endpoint, mock(PreVoteRequest.class))).isTrue();
        assertThat(firewall.shouldDropMessage(endpoint, mock(PreVoteResponse.class))).isTrue();
        assertThat(firewall.shouldDropMessage(endpoint, mock(InstallSnapshotRequest.class))).isTrue();
        assertThat(firewall.shouldDropMessage(endpoint, mock(InstallSnapshotResponse.class))).isTrue();
        assertThat(firewall.shouldDropMessage(endpoint, mock(TriggerLeaderElectionRequest.class))).isTrue();
    }

    @Test
    public void when_allMessagesAreDroppedForEndpoint_then_messagesAreSentToOtherEndpoint() {
        RaftEndpoint endpoint1 = LocalRaftEndpoint.newEndpoint();
        RaftEndpoint endpoint2 = LocalRaftEndpoint.newEndpoint();

        firewall.dropAllMessagesTo(endpoint1);

        assertThat(firewall.shouldDropMessage(endpoint2, mock(AppendEntriesRequest.class))).isFalse();
        assertThat(firewall.shouldDropMessage(endpoint2, mock(AppendEntriesSuccessResponse.class))).isFalse();
        assertThat(firewall.shouldDropMessage(endpoint2, mock(AppendEntriesFailureResponse.class))).isFalse();
        assertThat(firewall.shouldDropMessage(endpoint2, mock(VoteRequest.class))).isFalse();
        assertThat(firewall.shouldDropMessage(endpoint2, mock(VoteResponse.class))).isFalse();
        assertThat(firewall.shouldDropMessage(endpoint2, mock(PreVoteRequest.class))).isFalse();
        assertThat(firewall.shouldDropMessage(endpoint2, mock(PreVoteResponse.class))).isFalse();
        assertThat(firewall.shouldDropMessage(endpoint2, mock(InstallSnapshotRequest.class))).isFalse();
        assertThat(firewall.shouldDropMessage(endpoint2, mock(InstallSnapshotResponse.class))).isFalse();
        assertThat(firewall.shouldDropMessage(endpoint2, mock(TriggerLeaderElectionRequest.class))).isFalse();
    }

    @Test
    public void when_messageIsDroppedForAllEndpoints_then_messageIsNotSentToAnyEndpoint() {
        firewall.dropMessagesToAll(AppendEntriesRequest.class);

        assertThat(firewall.shouldDropMessage(LocalRaftEndpoint.newEndpoint(), mock(AppendEntriesRequest.class))).isTrue();
        assertThat(firewall.shouldDropMessage(LocalRaftEndpoint.newEndpoint(), mock(AppendEntriesRequest.class))).isTrue();
    }

    @Test
    public void when_messageIsDroppedForAllEndpoints_then_otherMessageTypeIsSentToEndpoints() {
        firewall.dropMessagesToAll(AppendEntriesRequest.class);

        assertThat(firewall.shouldDropMessage(LocalRaftEndpoint.newEndpoint(), mock(VoteRequest.class))).isFalse();
        assertThat(firewall.shouldDropMessage(LocalRaftEndpoint.newEndpoint(), mock(VoteRequest.class))).isFalse();
    }

    @Test
    public void when_messageTypeIsDroppedToEndpoint_then_allMessageTypesCanBeDroppedForThatEndpoint() {
        LocalRaftEndpoint endpoint = LocalRaftEndpoint.newEndpoint();
        firewall.dropMessagesTo(endpoint, AppendEntriesRequest.class);

        firewall.dropAllMessagesTo(endpoint);

        assertThat(firewall.shouldDropMessage(endpoint, mock(AppendEntriesRequest.class))).isTrue();
        assertThat(firewall.shouldDropMessage(endpoint, mock(VoteRequest.class))).isTrue();
    }

    @Test
    public void when_messageTypeIsAllowedForEndpoint_then_messageCanBeSent() {
        LocalRaftEndpoint endpoint = LocalRaftEndpoint.newEndpoint();

        firewall.dropMessagesTo(endpoint, AppendEntriesRequest.class);
        firewall.allowMessagesTo(endpoint, AppendEntriesRequest.class);

        assertThat(firewall.shouldDropMessage(endpoint, mock(AppendEntriesRequest.class))).isFalse();
    }

    @Test
    public void when_allMessageTypesDroppedForEndpoint_then_allMessageTypesCanBeAllowedForEndpoint() {
        LocalRaftEndpoint endpoint = LocalRaftEndpoint.newEndpoint();

        firewall.dropAllMessagesTo(endpoint);
        firewall.allowAllMessagesTo(endpoint);

        assertThat(firewall.shouldDropMessage(endpoint, mock(AppendEntriesRequest.class))).isFalse();
    }

    @Test
    public void when_multipleMessageTypesDroppedForEndpoint_then_allMessageTypesCanBeAllowedForEndpoint() {
        LocalRaftEndpoint endpoint = LocalRaftEndpoint.newEndpoint();

        firewall.dropMessagesTo(endpoint, AppendEntriesRequest.class);
        firewall.dropMessagesTo(endpoint, VoteRequest.class);
        firewall.allowAllMessagesTo(endpoint);

        assertThat(firewall.shouldDropMessage(endpoint, mock(AppendEntriesRequest.class))).isFalse();
        assertThat(firewall.shouldDropMessage(endpoint, mock(VoteRequest.class))).isFalse();
    }

}
