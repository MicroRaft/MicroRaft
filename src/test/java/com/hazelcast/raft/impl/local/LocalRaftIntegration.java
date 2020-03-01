package com.hazelcast.raft.impl.local;

import com.hazelcast.raft.RaftEndpoint;
import com.hazelcast.raft.RaftIntegration;
import com.hazelcast.raft.RaftMsg;
import com.hazelcast.raft.RaftStateSummary;
import com.hazelcast.raft.impl.RaftNodeImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;
import java.util.function.Function;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * In-memory {@link RaftIntegration} implementation for Raft core testing. Creates a single thread executor
 * to execute/schedule tasks and operations.
 * <p>
 * Additionally provides a mechanism to define custom drop/allow rules for specific message types and endpoints.
 *
 * @author mdogan
 * @author metanet
 */
public class LocalRaftIntegration
        implements RaftIntegration {

    private static final Logger LOGGER = LoggerFactory.getLogger(LocalRaftIntegration.class);

    private final RaftEndpoint localMember;
    private final boolean appendNopEntryOnLeaderElection;
    private final ScheduledExecutorService scheduledExecutor = Executors.newSingleThreadScheduledExecutor();
    private final ConcurrentMap<RaftEndpoint, RaftNodeImpl> nodes = new ConcurrentHashMap<>();

    private final Set<EndpointDropEntry> endpointDropRules = Collections.newSetFromMap(new ConcurrentHashMap<>());
    private final Map<RaftEndpoint, Function<Object, Object>> alterRPCRules = new ConcurrentHashMap<>();
    private final Set<Class> dropAllRules = Collections.newSetFromMap(new ConcurrentHashMap<>());

    private final RaftDummyService service = new RaftDummyService();

    LocalRaftIntegration(LocalRaftEndpoint localMember, boolean appendNopEntryOnLeaderElection) {
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

    public RaftDummyService getService() {
        return service;
    }

    @Override
    public void execute(Runnable task) {
        try {
            scheduledExecutor.execute(task);
        } catch (RejectedExecutionException e) {
            LOGGER.error(localMember + " failed", e);
        }
    }

    @Override
    public void schedule(Runnable task, long delay, TimeUnit timeUnit) {
        try {
            scheduledExecutor.schedule(task, delay, timeUnit);
        } catch (RejectedExecutionException e) {
            LOGGER.error(localMember + " failed", e);
        }
    }

    @Override
    public Object getOperationToAppendAfterLeaderElection() {
        return appendNopEntryOnLeaderElection ? new Nop() : null;
    }

    @Override
    public boolean isReady() {
        return true;
    }

    @Override
    public void close() {
        scheduledExecutor.shutdownNow();
    }

    @Override
    public boolean isReachable(RaftEndpoint member) {
        return localMember.equals(member) || nodes.containsKey(member);
    }

    @Override
    public boolean send(RaftMsg msg, RaftEndpoint target) {
        assertThat(target).isNotEqualTo(localMember);

        RaftNodeImpl node = nodes.get(target);
        if (node == null) {
            return false;
        }
        if (shouldDrop(msg, target)) {
            return true;
        }

        node.handle(alterMessageIfNeeded(msg, target));
        return true;
    }

    private boolean shouldDrop(Object message, RaftEndpoint target) {
        return dropAllRules.contains(message.getClass()) || endpointDropRules
                .contains(new EndpointDropEntry(message.getClass(), target));
    }

    private <T> T alterMessageIfNeeded(T message, RaftEndpoint endpoint) {
        Function<Object, Object> alterFunc = alterRPCRules.get(endpoint);
        if (alterFunc != null) {
            Object alteredMessage = alterFunc.apply(message);
            if (alteredMessage != null) {
                return (T) alteredMessage;
            }
        }

        return message;
    }

    @Override
    public Object runOperation(Object op, long commitIndex) {
        if (op == null) {
            return null;
        }

        try {
            BiFunction<RaftDummyService, Long, Object> operation = (BiFunction<RaftDummyService, Long, Object>) op;
            return operation.apply(service, commitIndex);
        } catch (Throwable t) {
            return t;
        }
    }

    @Override
    public Object takeSnapshot(long commitIndex) {
        return service.takeSnapshot(commitIndex);
    }

    @Override
    public void restoreSnapshot(Object object, long commitIndex) {
        service.restoreSnapshot(commitIndex, (Map<Long, Object>) object);
    }

    @Override
    public void onRaftStateChange(RaftStateSummary summary) {
    }

    void dropMessagesToEndpoint(RaftEndpoint endpoint, Class messageType) {
        endpointDropRules.add(new EndpointDropEntry(messageType, endpoint));
    }

    void allowMessagesToEndpoint(RaftEndpoint endpoint, Class messageType) {
        endpointDropRules.remove(new EndpointDropEntry(messageType, endpoint));
    }

    void allowAllMessagesToEndpoint(RaftEndpoint endpoint) {
        endpointDropRules.removeIf(entry -> endpoint.equals(entry.endpoint));
    }

    void dropMessagesToAll(Class messageType) {
        dropAllRules.add(messageType);
    }

    void allowMessagesToAll(Class messageType) {
        dropAllRules.remove(messageType);
    }

    void resetAllRules() {
        dropAllRules.clear();
        endpointDropRules.clear();
        alterRPCRules.clear();
    }

    void alterMessagesToEndpoint(RaftEndpoint endpoint, Function<Object, Object> function) {
        alterRPCRules.put(endpoint, function);
    }

    void removeAlterMessageRuleToEndpoint(RaftEndpoint endpoint) {
        alterRPCRules.remove(endpoint);
    }

    boolean isClosed() {
        return scheduledExecutor.isShutdown();
    }

    private static class EndpointDropEntry {
        final Class messageType;
        final RaftEndpoint endpoint;

        private EndpointDropEntry(Class messageType, RaftEndpoint endpoint) {
            this.messageType = messageType;
            this.endpoint = endpoint;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof EndpointDropEntry)) {
                return false;
            }

            EndpointDropEntry that = (EndpointDropEntry) o;
            return messageType.equals(that.messageType) && endpoint.equals(that.endpoint);
        }

        @Override
        public int hashCode() {
            int result = messageType.hashCode();
            result = 31 * result + endpoint.hashCode();
            return result;
        }
    }

}
