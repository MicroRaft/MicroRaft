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

package io.microraft.impl.local;

import io.microraft.RaftEndpoint;
import io.microraft.RaftNode;
import io.microraft.impl.RaftNodeImpl;
import io.microraft.integration.RaftNodeRuntime;
import io.microraft.model.message.RaftMessage;
import io.microraft.report.RaftNodeReport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static io.microraft.RaftNodeStatus.TERMINATED;
import static io.microraft.RaftNodeStatus.isTerminal;
import static java.util.concurrent.Executors.newSingleThreadScheduledExecutor;

/**
 * A ver simple implementation of {@link RaftNodeRuntime} used for testing.
 * <p>
 * It uses a single thread executor to execute tasks and operations. It also
 * provides a mechanism to define custom drop / allow rules for specific
 * message types and endpoints.
 *
 * @author mdogan
 * @author metanet
 */
public final class LocalRaftNodeRuntime
        implements RaftNodeRuntime {

    private static final Logger LOGGER = LoggerFactory.getLogger(LocalRaftNodeRuntime.class);

    private final RaftEndpoint localEndpoint;
    private final Firewall firewall;
    private final ScheduledExecutorService executor = newSingleThreadScheduledExecutor();
    private final ConcurrentMap<RaftEndpoint, RaftNode> nodes = new ConcurrentHashMap<>();

    public LocalRaftNodeRuntime(RaftEndpoint localEndpoint, Firewall firewall) {
        this.localEndpoint = localEndpoint;
        this.firewall = firewall;
    }

    public RaftEndpoint getLocalEndpoint() {
        return localEndpoint;
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

        RaftNode old = nodes.get(endpoint);
        if (old != null && old != node && old.getStatus() != TERMINATED) {
            throw new IllegalArgumentException(localEndpoint + " already knows: " + endpoint);
        }

        nodes.put(endpoint, node);
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
            throw new IllegalArgumentException(localEndpoint + " cannot discover itself!");
        }

        nodes.remove(node.getLocalEndpoint(), node);
    }

    @Override
    public boolean isReachable(@Nonnull RaftEndpoint endpoint) {
        return nodes.containsKey(endpoint);
    }

    @Override
    public void execute(@Nonnull Runnable task) {
        try {
            executor.execute(task);
        } catch (RejectedExecutionException e) {
            LOGGER.error(localEndpoint + " failed", e);
        }
    }

    @Override
    public void submit(@Nonnull Runnable task) {
        execute(task);
    }

    @Override
    public void schedule(@Nonnull Runnable task, long delay, @Nonnull TimeUnit timeUnit) {
        try {
            executor.schedule(task, delay, timeUnit);
        } catch (RejectedExecutionException e) {
            LOGGER.error(localEndpoint + " failed", e);
        }
    }

    /**
     * Returns true if the internal executor is terminated.
     *
     * @return true if the internal executor is terminated
     */
    public boolean isShutdown() {
        return executor.isShutdown();
    }

    /**
     * Shuts down the internal executor without running the tasks waiting
     * in the queue.
     */
    public void terminate() {
        executor.shutdownNow();
    }

    @Override
    public void send(@Nonnull RaftEndpoint target, @Nonnull RaftMessage message) {
        if (localEndpoint.equals(target)) {
            throw new IllegalArgumentException("Cannot send " + message + " to itself!");
        }

        RaftNode node = nodes.get(target);
        if (node == null || firewall.shouldDrop(target, message)) {
            return;
        }

        try {
            RaftMessage maybeAlteredMessage = firewall.tryAlterMessage(target, message);
            if (maybeAlteredMessage == null) {
                throw new IllegalStateException(message + " sent to " + target.getId() + " is altered to null!");
            }
            node.handle(maybeAlteredMessage);
        } catch (Exception e) {
            LOGGER.error("Send " + message + " to " + target + " failed.", e);
        }
    }

    @Override
    public void handleRaftNodeReport(@Nonnull RaftNodeReport report) {
        if (isTerminal(report.getStatus())) {
            terminate();
        }
    }

    @Override
    public void onRaftGroupTerminated() {
    }

}
