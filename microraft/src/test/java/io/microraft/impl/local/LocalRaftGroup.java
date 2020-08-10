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

import io.microraft.RaftConfig;
import io.microraft.RaftEndpoint;
import io.microraft.RaftNode;
import io.microraft.RaftNode.RaftNodeBuilder;
import io.microraft.executor.impl.DefaultRaftNodeExecutor;
import io.microraft.impl.RaftNodeImpl;
import io.microraft.impl.state.RaftTermState;
import io.microraft.model.message.RaftMessage;
import io.microraft.persistence.NopRaftStore;
import io.microraft.persistence.RaftStore;
import io.microraft.persistence.RestoredRaftState;
import io.microraft.statemachine.StateMachine;
import io.microraft.test.util.AssertionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;
import java.util.function.Function;

import static io.microraft.RaftConfig.DEFAULT_RAFT_CONFIG;
import static io.microraft.impl.log.RaftLog.getLogCapacity;
import static io.microraft.test.util.AssertionUtils.eventually;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * This class is used for running a Raft group with local Raft nodes.
 * It provides methods to access specific Raft nodes, a set of functionalities
 * over them, such as terminations, creating network partitions, dropping or
 * altering network messages.
 *
 * @see LocalRaftEndpoint
 * @see SimpleStateMachine
 * @see Firewall
 */
public final class LocalRaftGroup {

    public static final BiFunction<RaftEndpoint, RaftConfig, RaftStore> IN_MEMORY_RAFT_STATE_STORE_FACTORY
            = (endpoint, config) -> {
        int commitCountToTakeSnapshot = config.getCommitCountToTakeSnapshot();
        int maxPendingLogEntryCount = config.getMaxPendingLogEntryCount();
        return new InMemoryRaftStore(getLogCapacity(commitCountToTakeSnapshot, maxPendingLogEntryCount));
    };

    private static final Logger LOGGER = LoggerFactory.getLogger(LocalRaftGroup.class);

    private final RaftConfig config;
    private final boolean newTermEntryEnabled;
    private final List<RaftEndpoint> initialMembers = new ArrayList<>();
    private final Map<RaftEndpoint, RaftNodeContext> nodeContexts = new HashMap<>();
    private final BiFunction<RaftEndpoint, RaftConfig, RaftStore> raftStoreFactory;

    private LocalRaftGroup(int size, RaftConfig config, boolean newTermEntryEnabled,
                           BiFunction<RaftEndpoint, RaftConfig, RaftStore> raftStoreFactory) {
        this.config = config;
        this.newTermEntryEnabled = newTermEntryEnabled;
        this.raftStoreFactory = raftStoreFactory;

        createNodes(size, config, raftStoreFactory);
    }

    /**
     * Creates and starts a Raft group for the given number of Raft nodes.
     *
     * @param groupSize
     *         the number of Raft nodes to create the Raft group
     *
     * @return the created and started Raft group
     */
    public static LocalRaftGroup start(int groupSize) {
        LocalRaftGroup group = new LocalRaftGroupBuilder(groupSize).build();
        group.start();

        return group;
    }

    /**
     * Creates and starts a Raft group for the given number of Raft nodes.
     *
     * @param groupSize
     *         the number of Raft nodes to create the Raft group
     * @param config
     *         the RaftConfig object to create the Raft nodes with
     *
     * @return the created and started Raft group
     */
    public static LocalRaftGroup start(int groupSize, RaftConfig config) {
        LocalRaftGroup group = new LocalRaftGroupBuilder(groupSize).setConfig(config).build();
        group.start();

        return group;
    }

    /**
     * Returns a new Raft group builder object
     *
     * @param groupSize
     *         the number of Raft nodes to create the Raft group
     *
     * @return a new Raft group builder object
     */
    public static LocalRaftGroupBuilder newBuilder(int groupSize) {
        return new LocalRaftGroupBuilder(groupSize);
    }

    private void createNodes(int size, RaftConfig config, BiFunction<RaftEndpoint, RaftConfig, RaftStore> raftStoreFactory) {
        for (int i = 0; i < size; i++) {
            RaftEndpoint endpoint = LocalRaftEndpoint.newEndpoint();
            initialMembers.add(endpoint);
        }

        for (int i = 0; i < size; i++) {
            RaftEndpoint endpoint = initialMembers.get(i);
            LocalTransport transport = new LocalTransport(endpoint);
            SimpleStateMachine stateMachine = new SimpleStateMachine(newTermEntryEnabled);
            RaftNodeBuilder nodeBuilder = RaftNode.newBuilder().setGroupId("default").setLocalEndpoint(endpoint)
                                                  .setInitialGroupMembers(initialMembers).setConfig(config)
                                                  .setTransport(transport).setStateMachine(stateMachine);
            if (raftStoreFactory != null) {
                nodeBuilder.setStore(raftStoreFactory.apply(endpoint, config));
            }

            RaftNodeImpl node = (RaftNodeImpl) nodeBuilder.build();
            RaftNodeContext context = new RaftNodeContext((DefaultRaftNodeExecutor) node.getExecutor(), transport, stateMachine,
                                                          node);
            nodeContexts.put(endpoint, context);
        }
    }

    /**
     * Enables discovery between the created Raft nodes and starts them.
     */
    public void start() {
        initDiscovery();
        startNodes();
    }

    private void initDiscovery() {
        for (RaftNodeContext ctx1 : nodeContexts.values()) {
            for (RaftNodeContext ctx2 : nodeContexts.values()) {
                if (ctx2.isExecutorRunning() && !ctx1.getLocalEndpoint().equals(ctx2.getLocalEndpoint())) {
                    ctx1.transport.discoverNode(ctx2.node);
                }
            }
        }
    }

    private void startNodes() {
        for (RaftNodeContext ctx : nodeContexts.values()) {
            ctx.node.start();
        }
    }

    /**
     * Creates a new Raft node and makes the other Raft nodes discover it.
     *
     * @return the created Raft node.
     */
    public RaftNodeImpl createNewNode() {
        LocalRaftEndpoint endpoint = LocalRaftEndpoint.newEndpoint();
        LocalTransport transport = new LocalTransport(endpoint);
        SimpleStateMachine stateMachine = new SimpleStateMachine(newTermEntryEnabled);
        RaftStore raftStore = raftStoreFactory != null ? raftStoreFactory.apply(endpoint, config) : new NopRaftStore();
        RaftNodeImpl node = (RaftNodeImpl) RaftNode.newBuilder().setGroupId("default").setLocalEndpoint(endpoint)
                                                   .setInitialGroupMembers(initialMembers).setConfig(config)
                                                   .setTransport(transport).setStateMachine(stateMachine).setStore(raftStore)
                                                   .build();

        nodeContexts
                .put(endpoint, new RaftNodeContext((DefaultRaftNodeExecutor) node.getExecutor(), transport, stateMachine, node));

        node.start();
        initDiscovery();

        return node;
    }

    /**
     * Restores a Raft node with the given {@link RestoredRaftState} object.
     * The Raft node to be restored must be created in this local Raft group.
     * <p>
     * If there exists a running Raft node with the same endpoint, this method
     * fails with {@link IllegalStateException}.
     *
     * @param restoredState
     *         the restored Raft state object to start the Raft node
     * @param store
     *         the Raft store object to start the Raft node
     *
     * @return the restored Raft node
     *
     * @throws IllegalStateException
     *         if there exists a running Raft node with the same endpoint
     */
    public RaftNodeImpl restoreNode(RestoredRaftState restoredState, RaftStore store) {
        boolean exists = nodeContexts.values().stream()
                                     .filter(ctx -> ctx.getLocalEndpoint().equals(restoredState.getLocalEndpoint()))
                                     .anyMatch(RaftNodeContext::isExecutorRunning);

        if (exists) {
            throw new IllegalStateException(restoredState.getLocalEndpoint() + " is already running!");
        }

        requireNonNull(restoredState);

        LocalTransport transport = new LocalTransport(restoredState.getLocalEndpoint());
        SimpleStateMachine stateMachine = new SimpleStateMachine(newTermEntryEnabled);
        RaftNodeImpl node = (RaftNodeImpl) RaftNode.newBuilder().setGroupId("default").setRestoredState(restoredState)
                                                   .setConfig(config).setTransport(transport).setStateMachine(stateMachine)
                                                   .setStore(store).build();
        nodeContexts.put(restoredState.getLocalEndpoint(),
                         new RaftNodeContext((DefaultRaftNodeExecutor) node.getExecutor(), transport, stateMachine, node));

        node.start();
        initDiscovery();

        return node;
    }

    /**
     * Returns all Raft nodes currently running in this local Raft group.
     *
     * @return all Raft nodes currently running in this local Raft group
     */
    public List<RaftNodeImpl> getNodes() {
        return nodeContexts.values().stream().map(ctx -> ctx.node).collect(toList());
    }

    /**
     * Returns all Raft nodes currently running in this local Raft group except
     * the given Raft endpoint.
     *
     * @param endpoint
     *         the Raft endpoint to excluded in the returned Raft node list
     *
     * @return all Raft nodes currently running in this local Raft group except
     *         the given Raft endpoint
     */
    public <T extends RaftNode> List<T> getNodesExcept(RaftEndpoint endpoint) {
        requireNonNull(endpoint);

        List<RaftNodeImpl> nodes = nodeContexts.values().stream().map(ctx -> ctx.node)
                                               .filter(node -> !node.getLocalEndpoint().equals(endpoint)).collect(toList());

        if (nodes.size() != nodeContexts.size() - 1) {
            throw new IllegalArgumentException("Unknown endpoint: " + endpoint);
        }

        return (List<T>) nodes;
    }

    /**
     * Returns the currently running Raft node of the given Raft endpoint.
     *
     * @param endpoint
     *         the Raft endpoint to return its Raft node
     *
     * @return the currently running Raft node of the given Raft endpoint
     */
    public <T extends RaftNode> T getNode(RaftEndpoint endpoint) {
        requireNonNull(endpoint);
        return (T) nodeContexts.get(endpoint).node;
    }

    /**
     * Returns the current leader Raft endpoint of the Raft group, or null
     * if there is no elected leader.
     * <p>
     * If different Raft nodes see different leaders, this method fails with
     * {@link AssertionError}.
     *
     * @return the current leader Raft endpoint of the Raft group, or null
     *         if there is no elected leader
     *
     * @throws AssertionError
     *         if different Raft nodes see different leaders
     */
    public RaftEndpoint getLeaderEndpoint() {
        RaftEndpoint leaderEndpoint = null;
        int leaderTerm = 0;
        for (RaftNodeContext nodeContext : nodeContexts.values()) {
            if (nodeContext.isExecutorShutdown()) {
                continue;
            }

            RaftTermState term = nodeContext.node.getTerm();
            if (term.getLeaderEndpoint() != null) {
                if (leaderEndpoint == null) {
                    leaderEndpoint = term.getLeaderEndpoint();
                    leaderTerm = term.getTerm();
                } else if (!(leaderEndpoint.equals(term.getLeaderEndpoint()) && leaderTerm == term.getTerm())) {
                    leaderEndpoint = null;
                    leaderTerm = 0;
                }
            } else {
                throw new AssertionError("Group doesn't have a single leader endpoint yet!");
            }
        }

        return leaderEndpoint;
    }

    /**
     * Returns the state machine object for the given Raft endpoint.
     *
     * @param endpoint
     *         the Raft endpoint to get the state machine object
     *
     * @return the state machine object for the given Raft endpoint
     */
    public SimpleStateMachine getStateMachine(RaftEndpoint endpoint) {
        requireNonNull(endpoint);
        return nodeContexts.get(endpoint).stateMachine;
    }

    private Firewall getFirewall(RaftEndpoint endpoint) {
        requireNonNull(endpoint);
        return nodeContexts.get(endpoint).transport.getFirewall();
    }

    /**
     * Waits until a leader is elected in this Raft group.
     * <p>
     * Fails with {@link AssertionError} if no leader is elected during
     * {@link AssertionUtils#EVENTUAL_ASSERTION_TIMEOUT_SECS}.
     *
     * @return the leader Raft node
     */
    public <T extends RaftNode> T waitUntilLeaderElected() {
        RaftNodeImpl[] leaderRef = new RaftNodeImpl[1];
        eventually(() -> {
            RaftNodeImpl leaderNode = getLeaderNode();
            assertThat(leaderNode).isNotNull();
            leaderRef[0] = leaderNode;
        });

        return (T) leaderRef[0];
    }

    /**
     * Returns the current leader Raft node of the Raft group, or null
     * if there is no elected leader.
     * <p>
     * If different Raft nodes see different leaders or the Raft node
     * of the leader Raft endpoint is not found, this method fails with
     * {@link AssertionError}.
     *
     * @return the current leader Raft endpoint of the Raft group, or null
     *         if there is no elected leader
     *
     * @throws AssertionError
     *         if different Raft nodes see different leaders or the Raft node
     *         of the leader Raft endpoint is not found
     */
    public <T extends RaftNode> T getLeaderNode() {
        RaftEndpoint leaderEndpoint = getLeaderEndpoint();
        if (leaderEndpoint == null) {
            return null;
        }

        RaftNodeContext leaderCtx = nodeContexts.get(leaderEndpoint);
        if (leaderCtx == null || leaderCtx.isExecutorShutdown()) {
            throw new AssertionError("Leader endpoint is " + leaderEndpoint + ", but leader node could not be found!");
        }

        return (T) leaderCtx.node;
    }

    /**
     * Returns a random Raft node other than the given Raft endpoint.
     * <p>
     * If no running Raft node is found for given Raft endpoint, then this
     * method fails with {@link NullPointerException}.
     *
     * @param endpoint
     *         the endpoint to not to choose for the returned Raft node
     *
     * @return a random Raft node other than the given Raft endpoint
     *
     * @throws NullPointerException
     *         if no running Raft node is found for given Raft endpoint
     */
    public <T extends RaftNode> T getAnyNodeExcept(RaftEndpoint endpoint) {
        requireNonNull(endpoint);
        requireNonNull(nodeContexts.get(endpoint));

        for (Entry<RaftEndpoint, RaftNodeContext> e : nodeContexts.entrySet()) {
            if (!e.getKey().equals(endpoint)) {
                return (T) e.getValue().node;
            }
        }

        throw new IllegalArgumentException("Unknown endpoint: " + endpoint);
    }

    /**
     * Returns a random follower Raft node.
     * <p>
     * If no leader is found, then this method fails with
     * {@link AssertionError}.
     *
     * @return a random follower Raft node
     *
     * @throws AssertionError
     *         if no leader is found
     */
    public <T extends RaftNode> T getAnyFollower() {
        RaftEndpoint leaderEndpoint = getLeaderEndpoint();
        if (leaderEndpoint == null) {
            throw new AssertionError("Group doesn't have a leader yet!");
        }

        return getAnyNodeExcept(leaderEndpoint);
    }

    /**
     * Stops all Raft nodes running in the Raft group.
     */
    public void destroy() {
        for (RaftNodeContext ctx : nodeContexts.values()) {
            ctx.node.terminate();
        }

        for (RaftNodeContext ctx : nodeContexts.values()) {
            ctx.executor.getExecutor().shutdown();
        }

        nodeContexts.clear();
    }

    /**
     * Creates an artificial load on the given Raft node by sleeping its thread
     * for the given duration.
     *
     * @param endpoint
     *         the endpoint of the Raft node to slow down
     * @param seconds
     *         the sleep duration in seconds
     */
    public void slowDownNode(RaftEndpoint endpoint, int seconds) {
        nodeContexts.get(endpoint).executor.submit(() -> {
            try {
                LOGGER.info(endpoint.getId() + " is under high load for " + seconds + " seconds.");
                Thread.sleep(TimeUnit.SECONDS.toMillis(seconds));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });
    }

    /**
     * Split the given Raft endpoints from the rest of the Raft group.
     * It means that the communication between the given endpoints and
     * the other Raft nodes will be blocked completely.
     * <p>
     * This method fails with {@link NullPointerException} if no Raft node
     * is found for any of the given endpoints list.
     *
     * @param endpoints
     *         the list of Raft endpoints to split from the rest of the Raft
     *         group
     *
     * @throws NullPointerException
     *         if no Raft node is found for any of the given endpoints list
     */
    public void splitMembers(RaftEndpoint... endpoints) {
        splitMembers(Arrays.asList(endpoints));
    }

    /**
     * Split the given Raft endpoints from the rest of the Raft group.
     * It means that the communication between the given endpoints and
     * the other Raft nodes will be blocked completely.
     * <p>
     * This method fails with {@link NullPointerException} if no Raft node
     * is found for any of the given endpoints list.
     *
     * @param endpoints
     *         the list of Raft endpoints to split from the rest of the Raft
     *         group
     *
     * @throws NullPointerException
     *         if no Raft node is found for any of the given endpoints list
     */
    public void splitMembers(List<RaftEndpoint> endpoints) {
        for (RaftEndpoint endpoint : endpoints) {
            requireNonNull(nodeContexts.get(endpoint));
        }

        List<RaftNodeContext> side1 = endpoints.stream().map(nodeContexts::get).collect(toList());
        List<RaftNodeContext> side2 = new ArrayList<>(nodeContexts.values());
        side2.removeAll(side1);

        for (RaftNodeContext ctx1 : side1) {
            for (RaftNodeContext ctx2 : side2) {
                ctx1.transport.undiscoverNode(ctx2.node);
                ctx2.transport.undiscoverNode(ctx1.node);
            }
        }
    }

    /**
     * Returns a random set of Raft nodes.
     *
     * @param nodeCount
     *         the number of Raft nodes to return
     * @param includeLeader
     *         denotes whether if the leader Raft node can be
     *         returned or not
     *
     * @return the randomly selected Raft node set
     */
    public List<RaftEndpoint> getRandomNodes(int nodeCount, boolean includeLeader) {
        RaftEndpoint leaderEndpoint = getLeaderEndpoint();

        List<RaftEndpoint> split = new ArrayList<>();
        if (includeLeader) {
            split.add(leaderEndpoint);
        }

        List<RaftNodeContext> contexts = new ArrayList<>(nodeContexts.values());
        Collections.shuffle(contexts);

        for (RaftNodeContext ctx : contexts) {
            if (leaderEndpoint.equals(ctx.getLocalEndpoint())) {
                continue;
            }

            split.add(ctx.getLocalEndpoint());
            if (split.size() == nodeCount) {
                break;
            }
        }

        return split;
    }

    /**
     * Cancels all network partitions and enables the Raft nodes to reach to
     * each other again.
     */
    public void merge() {
        initDiscovery();
    }

    /**
     * Adds a one-way drop-message rule for the given source and target Raft
     * endpoints and the Raft message type.
     * <p>
     * After this call, Raft messages of the given type sent from the given
     * source Raft endpoint to the given target Raft endpoint are silently
     * dropped.
     *
     * @param source
     *         the source Raft endpoint to drop Raft messages of the given type
     * @param target
     *         the target Raft endpoint to drop Raft messages of the given type
     * @param messageType
     *         the type of the Raft messages to be dropped
     */
    public <T extends RaftMessage> void dropMessagesTo(RaftEndpoint source, RaftEndpoint target, Class<T> messageType) {
        getFirewall(source).dropMessagesTo(target, messageType);
    }

    /**
     * Deletes the one-way drop-message rule for the given source and target
     * Raft endpoints and the Raft message type.
     *
     * @param source
     *         the source Raft endpoint to remove the drop-message rule
     * @param target
     *         the target Raft endpoint to remove the drop-message rule
     * @param messageType
     *         the type of the Raft messages
     */
    public <T extends RaftMessage> void allowMessagesTo(RaftEndpoint source, RaftEndpoint target, Class<T> messageType) {
        getFirewall(source).allowMessagesTo(target, messageType);
    }

    /**
     * Adds a one-way drop-all-messages rule for the given source and target
     * Raft endpoints.
     * <p>
     * After this call, all Raft messages sent from the source Raft endpoint to
     * the target Raft endpoint are silently dropped.
     * <p>
     * If there were drop-message rules from the source Raft endpoint to
     * the target Raft endpoint, they are replaced with a drop-all-messages
     * rule.
     *
     * @param source
     *         the source Raft endpoint to drop all Raft messages
     * @param target
     *         the target Raft endpoint to drop all Raft messages
     */
    public void dropAllMessagesTo(RaftEndpoint source, RaftEndpoint target) {
        getFirewall(source).dropAllMessagesTo(target);
    }

    /**
     * Deletes all one-way drop-message and drop-all-messages rules created
     * for the source Raft endpoint and the target Raft endpoint.
     *
     * @param source
     *         the source Raft endpoint
     * @param target
     *         the target Raft endpoint
     */
    public void allowAllMessagesTo(RaftEndpoint source, RaftEndpoint target) {
        getFirewall(source).allowAllMessagesTo(target);
    }

    /**
     * Adds a one-way drop-message rule for the given Raft message type
     * from the source Raft endpoint to all other Raft endpoints.
     *
     * @param messageType
     *         the type of the Raft messages to be dropped
     */
    public <T extends RaftMessage> void dropMessagesToAll(RaftEndpoint source, Class<T> messageType) {
        getFirewall(source).dropMessagesToAll(messageType);
    }

    /**
     * Deletes the one-way drop-all-messages rule for the given Raft message
     * type from the source Raft endpoint to any other Raft endpoint.
     *
     * @param messageType
     *         the type of the Raft message to delete the rule
     */
    public <T extends RaftMessage> void allowMessagesToAll(RaftEndpoint source, Class<T> messageType) {
        getFirewall(source).allowMessagesToAll(messageType);
    }

    /**
     * Resets all drop rules from the source Raft endpoint.
     */
    public void resetAllRulesFrom(RaftEndpoint source) {
        getFirewall(source).resetAllRules();
    }

    /**
     * Applies the given function an all Raft messages sent from the source
     * Raft endpoint to the target Raft endpoint.
     * <p>
     * If the given function is not altering a given Raft message, it should
     * return it as it is, instead of returning null.
     * <p>
     * Only a single alter rule can be created in the source Raft endpoint for
     * a given target Raft endpoint and a new alter rule overwrites
     * the previous one.
     *
     * @param source
     *         the source Raft endpoint to apply the alter function
     * @param target
     *         the target Raft endpoint to apply the alter function
     * @param function
     *         the alter function to apply to Raft messages
     */
    public void alterMessagesTo(RaftEndpoint source, RaftEndpoint target, Function<RaftMessage, RaftMessage> function) {
        getFirewall(source).alterMessagesTo(target, function);
    }

    /**
     * Deletes the alter-message rule from the source Raft endpoint to
     * the target Raft endpoint.
     *
     * @param source
     *         the source Raft endpoint to delete the alter function
     * @param target
     *         the target Raft endpoint to delete the alter function
     */
    void removeAlterMessageRuleTo(RaftEndpoint source, RaftEndpoint target) {
        getFirewall(source).removeAlterMessageFunctionTo(target);
    }

    /**
     * Terminates the Raft node with the given Raft endpoint and removes it
     * from the discovery state of the other Raft nodes. It means that
     * the other Raft nodes will see the terminated Raft node as unreachable.
     * <p>
     * This method fails with {@link NullPointerException} if there is no
     * running Raft node with the given endpoint.
     *
     * @param endpoint
     *         the Raft endpoint to terminate its Raft node
     *
     * @throws NullPointerException
     *         if there is no running Raft node with the given endpoint
     */
    public void terminateNode(RaftEndpoint endpoint) {
        RaftNodeContext ctx = nodeContexts.get(requireNonNull(endpoint));
        requireNonNull(ctx);
        ctx.node.terminate().join();
        splitMembers(ctx.getLocalEndpoint());
        ctx.executor.getExecutor().shutdown();
        nodeContexts.remove(endpoint);
    }

    /**
     * Builder for creating and starting Raft groups
     */
    public static final class LocalRaftGroupBuilder {

        private final int groupSize;
        private RaftConfig config = DEFAULT_RAFT_CONFIG;
        private boolean newTermOperationEnabled;
        private BiFunction<RaftEndpoint, RaftConfig, RaftStore> raftStoreFactory;

        private LocalRaftGroupBuilder(int groupSize) {
            if (groupSize < 1) {
                throw new IllegalArgumentException("Raft groups must have at least 2 Raft nodes!");
            }
            this.groupSize = groupSize;
        }

        /**
         * Sets the RaftConfig object to create Raft nodes.
         *
         * @param config
         *         the RaftConfig object to create Raft nodes
         *
         * @return the builder object for fluent calls
         */
        public LocalRaftGroupBuilder setConfig(RaftConfig config) {
            requireNonNull(config);
            this.config = config;
            return this;
        }

        /**
         * @return the builder object for fluent calls
         *
         * @see StateMachine#getNewTermOperation()
         */
        public LocalRaftGroupBuilder enableNewTermOperation() {
            this.newTermOperationEnabled = true;
            return this;
        }

        /**
         * Sets the factory object for creating Raft state stores.
         *
         * @param raftStoreFactory
         *         the factory object for creating Raft state
         *         stores
         *
         * @return the builder object for fluent calls
         */
        public LocalRaftGroupBuilder setRaftStoreFactory(BiFunction<RaftEndpoint, RaftConfig, RaftStore> raftStoreFactory) {
            requireNonNull(raftStoreFactory);
            this.raftStoreFactory = raftStoreFactory;
            return this;
        }

        /**
         * Builds the local Raft group with the configured settings.
         * Please note that the returned Raft group is not started yet.
         *
         * @return the created local Raft group
         */
        public LocalRaftGroup build() {
            return new LocalRaftGroup(groupSize, config, newTermOperationEnabled, raftStoreFactory);
        }

        /**
         * Builds and starts the local Raft group with the configured settings.
         *
         * @return the created and started local Raft group
         */
        public LocalRaftGroup start() {
            LocalRaftGroup group = new LocalRaftGroup(groupSize, config, newTermOperationEnabled, raftStoreFactory);
            group.start();

            return group;
        }
    }

    private static class RaftNodeContext {
        final DefaultRaftNodeExecutor executor;
        final LocalTransport transport;
        final SimpleStateMachine stateMachine;
        final RaftNodeImpl node;

        RaftNodeContext(DefaultRaftNodeExecutor executor, LocalTransport transport, SimpleStateMachine stateMachine,
                        RaftNodeImpl node) {
            this.executor = executor;
            this.transport = transport;
            this.stateMachine = stateMachine;
            this.node = node;
        }

        RaftEndpoint getLocalEndpoint() {
            return node.getLocalEndpoint();
        }

        boolean isExecutorRunning() {
            return !isExecutorShutdown();
        }

        boolean isExecutorShutdown() {
            return executor.getExecutor().isShutdown();
        }
    }

}
