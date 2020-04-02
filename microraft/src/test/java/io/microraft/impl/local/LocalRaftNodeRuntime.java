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
import io.microraft.impl.RaftNodeImpl;
import io.microraft.integration.RaftNodeRuntime;
import io.microraft.integration.StateMachine;
import io.microraft.model.message.RaftMessage;
import io.microraft.report.RaftNodeReport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;

import static io.microraft.RaftNodeStatus.isTerminal;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * In-memory {@link RaftNodeRuntime} implementation for Raft core testing. Creates a single thread executor
 * to execute/schedule tasks and operations.
 * <p>
 * Additionally provides a mechanism to define custom drop/allow rules for specific message types and endpoints.
 *
 * @author mdogan
 * @author metanet
 */
public final class LocalRaftNodeRuntime
        implements RaftNodeRuntime, StateMachine {

    private static final Logger LOGGER = LoggerFactory.getLogger(LocalRaftNodeRuntime.class);

    private final RaftEndpoint localMember;
    private final boolean appendNopEntryOnLeaderElection;
    private final ScheduledExecutorService scheduledExecutor = Executors.newSingleThreadScheduledExecutor();
    private final ConcurrentMap<RaftEndpoint, RaftNodeImpl> nodes = new ConcurrentHashMap<>();

    private final Set<EndpointDropRule> endpointDropRules = Collections.newSetFromMap(new ConcurrentHashMap<>());
    private final Map<RaftEndpoint, Function<RaftMessage, RaftMessage>> alterRPCRules = new ConcurrentHashMap<>();
    private final Set<Class<? extends RaftMessage>> dropAllRules = Collections.newSetFromMap(new ConcurrentHashMap<>());

    private final SimpleStateMachine stateMachine = new SimpleStateMachine();

    LocalRaftNodeRuntime(LocalRaftEndpoint localMember, boolean appendNopEntryOnLeaderElection) {
        this.localMember = localMember;
        this.appendNopEntryOnLeaderElection = appendNopEntryOnLeaderElection;
    }

    public void discoverNode(RaftNodeImpl node) {
        RaftEndpoint endpoint = node.getLocalEndpoint();
        assertThat(endpoint).isNotEqualTo(localMember);
        RaftNodeImpl old = nodes.putIfAbsent(endpoint, node);
        assertThat(old).satisfiesAnyOf(o -> assertThat(o).isNull(), o -> assertThat(o).isSameAs(node));
    }

    public boolean removeNode(RaftNodeImpl node) {
        RaftEndpoint endpoint = node.getLocalEndpoint();
        assertThat(endpoint).isNotEqualTo(localMember);
        return nodes.remove(node.getLocalEndpoint(), node);
    }

    public RaftEndpoint getLocalMember() {
        return localMember;
    }

    public SimpleStateMachine getStateMachine() {
        return stateMachine;
    }

    @Override
    public boolean isReachable(@Nonnull RaftEndpoint endpoint) {
        return localMember.equals(endpoint) || nodes.containsKey(endpoint);
    }

    @Override
    public Object runOperation(long commitIndex, @Nullable Object op) {
        if (op == null) {
            return null;
        }

        try {
            BiFunction<SimpleStateMachine, Long, Object> operation = (BiFunction<SimpleStateMachine, Long, Object>) op;
            return operation.apply(stateMachine, commitIndex);
        } catch (Throwable t) {
            return t;
        }
    }

    @Override
    public void execute(@Nonnull Runnable task) {
        try {
            scheduledExecutor.execute(task);
        } catch (RejectedExecutionException e) {
            LOGGER.error(localMember + " failed", e);
        }
    }

    @Override
    public void takeSnapshot(long commitIndex, @Nonnull Consumer<Object> snapshotChunkConsumer) {
        stateMachine.takeSnapshot(commitIndex, snapshotChunkConsumer);
    }

    @Override
    public void installSnapshot(long commitIndex, @Nonnull List<Object> snapshotChunks) {
        stateMachine.installSnapshot(commitIndex, snapshotChunks);
    }

    @Override
    public void submit(@Nonnull Runnable task) {
        execute(task);
    }

    @Override
    public Object getNewTermOperation() {
        return appendNopEntryOnLeaderElection ? new Nop() : null;
    }

    void dropMessagesToEndpoint(RaftEndpoint endpoint, Class<? extends RaftMessage> messageType) {
        endpointDropRules.add(new EndpointDropRule(messageType, endpoint));
    }

    @Override
    public void schedule(@Nonnull Runnable task, long delay, @Nonnull TimeUnit timeUnit) {
        try {
            scheduledExecutor.schedule(task, delay, timeUnit);
        } catch (RejectedExecutionException e) {
            LOGGER.error(localMember + " failed", e);
        }
    }

    void allowMessagesToEndpoint(RaftEndpoint endpoint, Class<? extends RaftMessage> messageType) {
        endpointDropRules.remove(new EndpointDropRule(messageType, endpoint));
    }

    void allowAllMessagesToEndpoint(RaftEndpoint endpoint) {
        endpointDropRules.removeIf(entry -> endpoint.equals(entry.endpoint));
    }

    void dropMessagesToAll(Class<? extends RaftMessage> messageType) {
        dropAllRules.add(messageType);
    }

    public void shutdown() {
        scheduledExecutor.shutdownNow();
    }

    void allowMessagesToAll(Class<? extends RaftMessage> messageType) {
        dropAllRules.remove(messageType);
    }

    void resetAllRules() {
        dropAllRules.clear();
        endpointDropRules.clear();
        alterRPCRules.clear();
    }

    void alterMessagesToEndpoint(RaftEndpoint endpoint, Function<RaftMessage, RaftMessage> function) {
        alterRPCRules.put(endpoint, function);
    }

    @Override
    public void send(@Nonnull RaftEndpoint target, @Nonnull RaftMessage message) {
        assertThat(target).isNotEqualTo(localMember);

        RaftNodeImpl node = nodes.get(target);
        if (node == null || shouldDrop(message, target)) {
            return;
        }

        node.handle(alterMessageIfNeeded(message, target));
    }

    void removeAlterMessageRuleToEndpoint(RaftEndpoint endpoint) {
        alterRPCRules.remove(endpoint);
    }

    boolean isShutdown() {
        return scheduledExecutor.isShutdown();
    }

    private boolean shouldDrop(RaftMessage message, RaftEndpoint target) {
        for (Class<? extends RaftMessage> droppedClazz : dropAllRules) {
            if (droppedClazz.isAssignableFrom(message.getClass())) {
                return true;
            }
        }

        for (EndpointDropRule rule : endpointDropRules) {
            if (target.equals(rule.endpoint) && rule.clazz.isAssignableFrom(message.getClass())) {
                return true;
            }
        }

        return false;
    }

    private RaftMessage alterMessageIfNeeded(RaftMessage message, RaftEndpoint endpoint) {
        Function<RaftMessage, RaftMessage> alterFunc = alterRPCRules.get(endpoint);
        if (alterFunc != null) {
            RaftMessage alteredMessage = alterFunc.apply(message);
            if (alteredMessage != null) {
                return alteredMessage;
            }
        }

        return message;
    }

    @Override
    public void handleRaftNodeReport(@Nonnull RaftNodeReport report) {
        if (isTerminal(report.getStatus())) {
            shutdown();
        }
    }

    @Override
    public void onRaftGroupTerminated() {
    }

    private static final class EndpointDropRule {
        final Class<? extends RaftMessage> clazz;
        final RaftEndpoint endpoint;

        private EndpointDropRule(Class<? extends RaftMessage> clazz, RaftEndpoint endpoint) {
            this.clazz = clazz;
            this.endpoint = endpoint;
        }

        @Override
        public int hashCode() {
            int result = clazz.hashCode();
            result = 31 * result + endpoint.hashCode();
            return result;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof EndpointDropRule)) {
                return false;
            }

            EndpointDropRule that = (EndpointDropRule) o;
            return clazz.equals(that.clazz) && endpoint.equals(that.endpoint);
        }

    }

}
