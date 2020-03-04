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

package io.microraft.impl.handler;

import io.microraft.RaftEndpoint;
import io.microraft.exception.RaftException;
import io.microraft.impl.RaftNodeImpl;
import io.microraft.impl.log.SnapshotChunkCollector;
import io.microraft.model.log.SnapshotChunk;
import io.microraft.model.log.SnapshotEntry.SnapshotEntryBuilder;
import io.microraft.model.message.AppendEntriesFailureResponse;
import io.microraft.model.message.AppendEntriesSuccessResponse;
import io.microraft.model.message.InstallSnapshotRequest;
import io.microraft.model.message.InstallSnapshotResponse;
import io.microraft.model.message.RaftMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import static io.microraft.RaftRole.FOLLOWER;
import static java.util.Collections.shuffle;
import static java.util.Collections.singletonList;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

/**
 * Handles an {@link InstallSnapshotRequest} which could be sent by the leader
 * or a follower.
 * <p>
 * Responds with either an {@link InstallSnapshotResponse},
 * {@link AppendEntriesSuccessResponse},
 * or {@link AppendEntriesFailureResponse}.
 * <p>
 * See <i>7 Log compaction</i> section of
 * <i>In Search of an Understandable Consensus Algorithm</i>
 * paper by <i>Diego Ongaro</i> and <i>John Ousterhout</i>.
 * <p>
 * If the request contains no snapshot chunks, it means that the Raft leader
 * intends to send a snapshot to the follower. In this case, the follower
 * initializes its {@link SnapshotChunkCollector} state based on the snapshot
 * chunk count present in the request, then starts asking snapshot chunks via
 * {@link InstallSnapshotResponse}. The follower collects the snapshot chunks
 * received via following {@link InstallSnapshotRequest} objects and once all
 * snapshot chunks are received, the follower installs the snapshot and sends
 * an {@link AppendEntriesSuccessResponse} back to the leader.
 * <p>
 * Our Raft log design ensures that every Raft group member takes a snapshot
 * at exactly the same commit index. This behaviour enables an optimization.
 * When a follower receives a InstallSnapshotRequest from the leader, it can
 * ask snapshot chunks not only from the leader, but also from the followers.
 * By this way, we utilize the bandwidth of the followers and speed up
 * the snapshot installation process by transferring snapshot chunks in
 * parallel.
 *
 * @author metanet
 * @see InstallSnapshotRequest
 * @see InstallSnapshotResponse
 * @see AppendEntriesSuccessResponse
 * @see AppendEntriesFailureResponse
 */
public class InstallSnapshotRequestHandler
        extends AbstractMessageHandler<InstallSnapshotRequest> {

    private static final Logger LOGGER = LoggerFactory.getLogger(InstallSnapshotRequestHandler.class);

    public InstallSnapshotRequestHandler(RaftNodeImpl raftNode, InstallSnapshotRequest request) {
        super(raftNode, request);
    }

    @Override
    protected void handle(@Nonnull InstallSnapshotRequest request) {
        requireNonNull(request);

        LOGGER.debug("{} received {}.", localEndpointStr(), request);

        RaftEndpoint sender = request.getSender();

        // Reply false if term < currentTerm (§5.1)
        if (request.getTerm() < state.term()) {
            if (LOGGER.isDebugEnabled()) {
                LOGGER.warn(localEndpointStr() + " received stale " + request + " in current term: " + state.term());
            }

            if (request.isSenderLeader()) {
                RaftMessage response = modelFactory.createAppendEntriesFailureResponseBuilder().setGroupId(node.getGroupId())
                                                   .setSender(localEndpoint()).setTerm(state.term()).setExpectedNextIndex(0)
                                                   .setQueryRound(0).build();
                node.send(response, sender);
            }

            return;
        }

        // Transform into follower if a newer term is seen or another node wins the election of the current term
        if (request.getTerm() > state.term() || state.role() != FOLLOWER) {
            // If the request term is greater than the local term, update the local term and convert to follower (§5.1)
            LOGGER.info("{} Demoting to FOLLOWER from current role: {}, term: {} to new term: {} and sender: {}",
                    localEndpointStr(), state.role(), state.term(), request.getTerm(), sender.getId());

            node.toFollower(request.getTerm());

            if (!request.isSenderLeader()) {
                return;
            }
        }

        if (request.isSenderLeader()) {
            if (!sender.equals(state.leader())) {
                LOGGER.info("{} Setting leader: {}", localEndpointStr(), sender.getId());
                node.leader(sender);
            }

            node.leaderHeartbeatReceived();
        }

        if (!checkSnapshotIndex(request)) {
            return;
        }

        SnapshotChunkCollector snapshotChunkCollector = getOrCreateSnapshotChunkCollector(request);
        if (snapshotChunkCollector == null) {
            return;
        }

        Map<RaftEndpoint, List<Integer>> requestedSnapshotChunkIndices = handleSnapshotChunks(request, snapshotChunkCollector);
        if (requestedSnapshotChunkIndices.isEmpty()) {
            SnapshotEntryBuilder snapshotEntryBuilder = node.getModelFactory().createSnapshotEntryBuilder();
            node.installSnapshot(snapshotChunkCollector.buildSnapshotEntry(snapshotEntryBuilder));
            sendAppendEntriesSuccessResponse(request);
        } else {
            requestSnapshotChunks(request, requestedSnapshotChunkIndices);
        }
    }

    private boolean checkSnapshotIndex(InstallSnapshotRequest request) {
        if (request.getSnapshotIndex() < state.commitIndex()) {
            LOGGER.info("{} ignored stale snapshot chunks at log index: {} from: {}, current commit index: {}",
                    localEndpointStr(), request.getSnapshotIndex(), request.getSender().getId(), state.commitIndex());
            return false;
        } else if (request.getSnapshotIndex() == state.commitIndex()) {
            LOGGER.info("{} ignored snapshot chunks at log index: {} from: {} since commit index is same.", localEndpointStr(),
                    request.getSnapshotIndex(), request.getSender().getId());
            if (request.isSenderLeader()) {
                sendAppendEntriesSuccessResponse(request);
            }

            return false;
        }

        return true;
    }

    private void sendAppendEntriesSuccessResponse(InstallSnapshotRequest request) {
        RaftMessage response = modelFactory.createAppendEntriesSuccessResponseBuilder().setGroupId(node.getGroupId())
                                           .setSender(localEndpoint()).setTerm(state.term())
                                           .setLastLogIndex(request.getSnapshotIndex())
                                           .setQueryRound(request.isSenderLeader() ? request.getQueryRound() : 0).build();
        node.send(response, state.leader());
    }

    private SnapshotChunkCollector getOrCreateSnapshotChunkCollector(InstallSnapshotRequest request) {
        SnapshotChunkCollector snapshotChunkCollector = state.snapshotChunkCollector();
        if (snapshotChunkCollector == null) {
            snapshotChunkCollector = new SnapshotChunkCollector(request.getSnapshotIndex(), request.getSnapshotTerm(),
                    request.getTotalSnapshotChunkCount(), request.getGroupMembersLogIndex(), request.getGroupMembers());
            state.snapshotChunkCollector(snapshotChunkCollector);

            return snapshotChunkCollector;
        } else if (snapshotChunkCollector.getSnapshotIndex() > request.getSnapshotIndex()) {
            LOGGER.warn("{} current snapshot chunks at log index: {} are more recent than received snapshot "
                            + "chunks at log index: {} from sender: {} (is leader: {})", localEndpointStr(),
                    snapshotChunkCollector.getSnapshotIndex(), request.getSnapshotIndex(), request.getSender(),
                    request.isSenderLeader());

            return null;
        } else if (snapshotChunkCollector.getSnapshotIndex() < request.getSnapshotIndex()) {
            LOGGER.warn("{} truncating snapshot chunks at log index: {}", localEndpointStr(),
                    snapshotChunkCollector.getSnapshotIndex());

            try {
                state.store().truncateSnapshotChunksUntil(snapshotChunkCollector.getSnapshotIndex());
            } catch (IOException e) {
                throw new RaftException(
                        "Could not truncate snapshot chunks until log index: " + snapshotChunkCollector.getSnapshotIndex(),
                        node.getLeaderEndpoint(), e);
            }

            snapshotChunkCollector = new SnapshotChunkCollector(request.getSnapshotIndex(), request.getSnapshotTerm(),
                    request.getTotalSnapshotChunkCount(), request.getGroupMembersLogIndex(), request.getGroupMembers());
            state.snapshotChunkCollector(snapshotChunkCollector);
        }

        if (snapshotChunkCollector.getSnapshotTerm() != request.getSnapshotTerm()) {
            throw new IllegalStateException(
                    "snapshot index: " + request.getSnapshotIndex() + " snapshot term: " + request.getSnapshotTerm()
                            + " snapshot collector term: " + snapshotChunkCollector.getSnapshotTerm());
        }

        return snapshotChunkCollector;
    }

    private Map<RaftEndpoint, List<Integer>> handleSnapshotChunks(InstallSnapshotRequest request,
                                                                  SnapshotChunkCollector snapshotChunkCollector) {
        List<SnapshotChunk> newSnapshotChunks = snapshotChunkCollector.add(request.getSnapshotChunks());
        for (SnapshotChunk snapshotChunk : newSnapshotChunks) {
            try {
                state.store().persistSnapshotChunk(snapshotChunk);
                // we will flush() after all snapshot chunks are persisted.
            } catch (IOException e) {
                throw new RaftException(
                        "Could not persist snapshot chunk: " + snapshotChunk.getSnapshotChunkIndex() + " at snapshot index: "
                                + snapshotChunk.getIndex() + " and term: " + snapshotChunk.getTerm(), node.getLeaderEndpoint(),
                        e);
            }
        }

        if (newSnapshotChunks.size() > 0 && LOGGER.isDebugEnabled()) {
            List<Integer> newSnapshotChunkIndices = newSnapshotChunks.stream().map(SnapshotChunk::getSnapshotChunkIndex)
                                                                     .collect(toList());
            LOGGER.debug(localEndpointStr() + " added new snapshot chunks: " + newSnapshotChunkIndices + " at snapshot index: "
                    + request.getSnapshotIndex() + " and term: " + request.getSnapshotTerm());
        }

        Map<RaftEndpoint, List<Integer>> indicesMap = new HashMap<>();

        if (request.getSnapshotChunks().isEmpty()) {
            // If this is the first install snapshot request sent by the
            // leader, we start asking snapshot chunks from all remote group
            // members. There is one caveat here. We have a backoff mechanism
            // for the requests that go from the leader to the followers. Once
            // the leader sends a request to a follower, it does not send
            // another request to the same follower either until the follower
            // responds to the previous request or the backoff timeout elapses.
            // However, we don't have the same backoff mechanism for
            // the install snapshot response this node will send to another
            // follower. If the leader sends too many empty install snapshot
            // requests very quickly, we may flood the other followers here.
            // Thankfully, we use the max backoff period on the leader when it
            // sends a install snapshot request and hence we don't expect to
            // encounter the aforementioned problem...

            // index=0 of remoteMembers is the leader.
            List<RaftEndpoint> remoteMembers = getShuffledRemoteMembers();
            List<Integer> requestedChunkIndices = snapshotChunkCollector.getNextChunks(remoteMembers.size());
            for (int i = 0; i < requestedChunkIndices.size(); i++) {
                int chunkIndex = requestedChunkIndices.get(i);
                indicesMap.put(remoteMembers.get(i), singletonList(chunkIndex));
            }

            return indicesMap;
        } else {
            // If we receive a snapshot chunk from a remote member, we only
            // ask that member to send a new snapshot chunk.
            List<Integer> requestedChunkIndices = snapshotChunkCollector.getNextChunks(1);
            if (requestedChunkIndices.size() > 0) {
                indicesMap.put(request.getSender(), requestedChunkIndices);
            }
        }

        return indicesMap;
    }

    private List<RaftEndpoint> getShuffledRemoteMembers() {
        List<RaftEndpoint> remoteMembers = new ArrayList<>(state.committedGroupMembers().remoteMembers());
        shuffle(remoteMembers);

        int leaderIndex = remoteMembers.indexOf(state.leader());
        if (leaderIndex != 0) {
            remoteMembers.set(leaderIndex, remoteMembers.get(0));
            remoteMembers.set(0, state.leader());
        }

        return remoteMembers;
    }

    private void requestSnapshotChunks(InstallSnapshotRequest request,
                                       Map<RaftEndpoint, List<Integer>> requestedSnapshotChunkIndices) {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug(localEndpointStr() + " requesting snapshot chunk indices: " + requestedSnapshotChunkIndices
                    + " at snapshot index: " + request.getSnapshotIndex() + " and term: " + request.getSnapshotTerm());
        }

        for (Entry<RaftEndpoint, List<Integer>> e : requestedSnapshotChunkIndices.entrySet()) {
            RaftEndpoint target = e.getKey();
            RaftMessage response = node.getModelFactory().createInstallSnapshotResponseBuilder().setGroupId(node.getGroupId())
                                       .setSender(localEndpoint()).setTerm(state.term())
                                       .setSnapshotIndex(request.getSnapshotIndex())
                                       .setRequestedSnapshotChunkIndices(e.getValue())
                                       .setQueryRound(state.leader().equals(target) ? request.getQueryRound() : 0).build();

            node.send(response, target);
        }
    }

}
