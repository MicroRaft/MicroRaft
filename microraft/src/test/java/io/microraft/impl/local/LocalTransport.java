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
import io.microraft.RaftNode;
import io.microraft.impl.RaftNodeImpl;
import io.microraft.model.message.RaftMessage;
import io.microraft.transport.Transport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static io.microraft.RaftNodeStatus.TERMINATED;
import static java.util.Objects.requireNonNull;

public class LocalTransport
        implements Transport {

    private static final Logger LOGGER = LoggerFactory.getLogger(LocalTransport.class);

    private final RaftEndpoint localEndpoint;
    private final ConcurrentMap<RaftEndpoint, RaftNode> nodes = new ConcurrentHashMap<>();
    private final Firewall firewall;

    public LocalTransport(RaftEndpoint localEndpoint) {
        this.localEndpoint = requireNonNull(localEndpoint);
        this.firewall = new Firewall();
    }

    @Override
    public void send(@Nonnull RaftEndpoint target, @Nonnull RaftMessage message) {
        if (localEndpoint.equals(target)) {
            throw new IllegalArgumentException(localEndpoint.getId() + " cannot send " + message + " to itself!");
        }

        RaftNode node = nodes.get(target);
        if (node == null || firewall.shouldDropMessage(target, message)) {
            return;
        }

        try {
            RaftMessage maybeAlteredMessage = firewall.tryAlterMessage(target, message);
            if (maybeAlteredMessage == null) {
                LOGGER.error(message + " sent from " + localEndpoint.getId() + " to " + target.getId() + " is altered to null!");
                return;
            }

            node.handle(maybeAlteredMessage);
        } catch (Exception e) {
            LOGGER.error("Send " + message + " to " + target + " failed.", e);
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
    public void undiscoverNode(RaftNodeImpl node) {
        RaftEndpoint endpoint = node.getLocalEndpoint();
        if (localEndpoint.equals(endpoint)) {
            throw new IllegalArgumentException(localEndpoint + " cannot undiscover itself!");
        }

        nodes.remove(node.getLocalEndpoint(), node);
    }

    /**
     * Returns the firewall returned by this transport object.
     */
    public Firewall getFirewall() {
        return firewall;
    }

}
