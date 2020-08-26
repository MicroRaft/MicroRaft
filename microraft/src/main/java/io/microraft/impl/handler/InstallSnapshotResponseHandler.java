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

package io.microraft.impl.handler;

import io.microraft.impl.RaftNodeImpl;
import io.microraft.impl.state.FollowerState;
import io.microraft.impl.state.LeaderState;
import io.microraft.model.message.InstallSnapshotRequest;
import io.microraft.model.message.InstallSnapshotResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;

import static io.microraft.RaftRole.FOLLOWER;
import static io.microraft.RaftRole.LEADER;

/**
 * Handles an {@link InstallSnapshotResponse} sent by a Raft follower and
 * responds with a {@link InstallSnapshotRequest}.
 * <p>
 * See <i>7 Log compaction</i> section of
 * <i>In Search of an Understandable Consensus Algorithm</i>
 * paper by <i>Diego Ongaro</i> and <i>John Ousterhout</i>.
 * <p>
 * {@link InstallSnapshotResponse} could be received either by the Raft group
 * leader or a follower whose is known to be installed the requested snapshot.
 * <p>
 * A Raft leader initiates a snapshot installation process by sending an empty
 * {@link InstallSnapshotRequest} to a follower. Then, the follower asks
 * missing snapshot chunks from both the Raft group leader and the followers
 * given in the {@link InstallSnapshotRequest}. If this node's last snapshot is
 * still at the requested log index, i.e., there is no new snapshot taken, then
 * the node responds back with an {@link InstallSnapshotRequest} object that
 * contains the requested snapshot chunk. However, if this node is the leader
 * and has taken a new snapshot in the meantime, which means the requested
 * snapshot chunk is not available for the requested log index. then it sends
 * back a new empty {@link InstallSnapshotRequest} in order to initiate a new
 * snapshot transfer process for the current snapshot.
 *
 * @see InstallSnapshotRequest
 * @see InstallSnapshotResponse
 */
public class InstallSnapshotResponseHandler
        extends AbstractResponseHandler<InstallSnapshotResponse> {

    private static final Logger LOGGER = LoggerFactory.getLogger(InstallSnapshotResponseHandler.class);

    public InstallSnapshotResponseHandler(RaftNodeImpl raftNode, InstallSnapshotResponse response) {
        super(raftNode, response);
    }

    @Override
    protected void handleResponse(@Nonnull InstallSnapshotResponse response) {
        LOGGER.debug("{} received {}.", localEndpointStr(), response);

        if (response.getTerm() > state.term()) {
            if (state.role() == LEADER) {
                LOGGER.warn("{} Ignored invalid response {} for current term: {}", localEndpointStr(), response, state.term());
                return;
            } else if (state.role() != FOLLOWER) {
                // If the request term is greater than the local term,
                // update the local term and convert to follower (§5.1)
                LOGGER.info("{} Demoting to FOLLOWER from current role: {}, term: {} to new term: {} and sender: {}",
                            localEndpointStr(), state.role(), state.term(), response.getTerm(), response.getSender().getId());

                node.toFollower(response.getTerm());
            }
        }

        node.tryAckQuery(response.getQuerySequenceNumber(), response.getSender());

        LeaderState leaderState = state.leaderState();
        FollowerState followerState = leaderState != null ? leaderState.getFollowerState(response.getSender()) : null;
        if (followerState != null) {
            if (response.getFlowControlSequenceNumber() == 0) {
                followerState.resetRequestBackoff();
            } else if (!followerState.responseReceived(response.getFlowControlSequenceNumber())) {
                return;
            }
        }

        node.sendSnapshotChunk(response.getSender(), response.getSnapshotIndex(), response.getRequestedSnapshotChunkIndex());
    }

}
