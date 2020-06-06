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

package io.microraft.tutorial;

import io.microraft.RaftEndpoint;
import io.microraft.RaftNode;
import io.microraft.model.message.RaftMessage;
import io.microraft.transport.Transport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static io.microraft.RaftNodeStatus.TERMINATED;
import static java.util.Objects.requireNonNull;

/**
 * A very simple {@link Transport} implementation used in the tutorial.
 * <p>
 * It uses a concurrent hash map to pass Raft message objects between Raft
 * nodes.
 * <p>
 * YOU CAN SEE THIS CLASS AT:
 * <p>
 * https://github.com/MicroRaft/MicroRaft/blob/master/microraft-tutorial/src/main/java/io/microraft/tutorial/LocalTransport.java
 */
final class LocalTransport
        implements Transport {

    private static final Logger LOGGER = LoggerFactory.getLogger(LocalTransport.class);

    private final RaftEndpoint localEndpoint;
    private final Map<RaftEndpoint, RaftNode> nodes = new ConcurrentHashMap<>();

    LocalTransport(RaftEndpoint localEndpoint) {
        this.localEndpoint = requireNonNull(localEndpoint);
    }

    @Override
    public void send(@Nonnull RaftEndpoint target, @Nonnull RaftMessage message) {
        if (localEndpoint.equals(target)) {
            throw new IllegalArgumentException(localEndpoint.getId() + " cannot send " + message + " to itself!");
        }

        RaftNode node = nodes.get(target);
        if (node != null) {
            node.handle(message);
        }
    }

    @Override
    public boolean isReachable(@Nonnull RaftEndpoint endpoint) {
        return nodes.containsKey(endpoint);
    }

    /**
     * Adds the given Raft node to the known Raft nodes map.
     * <p>
     * After this call, Raft messages sent to the given Raft endpoint are
     * are passed to its Raft node.
     *
     * @param node
     *         the Raft node to be added to the known Raft nodes map
     */
    public void discoverNode(RaftNode node) {
        RaftEndpoint endpoint = node.getLocalEndpoint();
        if (localEndpoint.equals(endpoint)) {
            throw new IllegalArgumentException(localEndpoint + " cannot discover itself!");
        }

        RaftNode existingNode = nodes.putIfAbsent(endpoint, node);
        if (existingNode != null && existingNode != node && existingNode.getStatus() != TERMINATED) {
            throw new IllegalArgumentException(localEndpoint + " already knows: " + endpoint);
        }
    }

    /**
     * Removes the given Raft node from the known Raft nodes map.
     * <p>
     * After this call, Raft messages sent to the given Raft endpoint
     * are silently dropped.
     *
     * @param node
     *         the Raft node to be removed from the known Raft nodes map
     */
    public void undiscoverNode(RaftNode node) {
        RaftEndpoint endpoint = node.getLocalEndpoint();
        if (localEndpoint.equals(endpoint)) {
            throw new IllegalArgumentException(localEndpoint + " cannot undiscover itself!");
        }

        nodes.remove(node.getLocalEndpoint(), node);
    }

    public RaftEndpoint getLocalEndpoint() {
        return localEndpoint;
    }

}
