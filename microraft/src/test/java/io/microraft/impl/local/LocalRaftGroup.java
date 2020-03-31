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
import io.microraft.impl.RaftNodeImpl;
import io.microraft.model.message.RaftMessage;
import io.microraft.persistence.NopRaftStore;
import io.microraft.persistence.RaftStore;
import io.microraft.persistence.RestoredRaftState;

import java.util.Arrays;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;

import static io.microraft.RaftConfig.DEFAULT_RAFT_CONFIG;
import static io.microraft.RaftNode.newBuilder;
import static io.microraft.impl.util.AssertionUtils.eventually;
import static io.microraft.impl.util.RaftTestUtils.getTerm;
import static io.microraft.impl.util.RaftTestUtils.majority;
import static io.microraft.impl.util.RaftTestUtils.minority;
import static java.lang.System.arraycopy;
import static java.util.Arrays.asList;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertNotNull;

/**
 * Represents a single Raft group of which all members run locally,
 * and provides methods to access specific nodes, terminate nodes, split/merge
 * the group and define allow/drop rules between nodes.
 *
 * @author mdogan
 * @author metanet
 */
public class LocalRaftGroup {

    private final RaftConfig config;
    private final boolean appendNopEntryOnLeaderElection;
    private RaftEndpoint[] initialMembers;
    private RaftEndpoint[] members;
    private LocalRaftNodeRuntime[] runtimes;
    private RaftNodeImpl[] nodes;
    private int createdNodeCount;
    private BiFunction<RaftEndpoint, RaftConfig, RaftStore> raftStoreFactory;
    private Supplier<LocalRaftEndpoint> endpointFactory = LocalRaftEndpoint::newEndpoint;

    public LocalRaftGroup(int size) {
        this(size, DEFAULT_RAFT_CONFIG);
    }

    public LocalRaftGroup(int size, RaftConfig config) {
        this(size, config, false, LocalRaftEndpoint::newEndpoint, (raftEndpoint, c) -> NopRaftStore.INSTANCE, null);
    }

    public LocalRaftGroup(int size, RaftConfig config, boolean appendNopEntryOnLeaderElection,
                          Supplier<LocalRaftEndpoint> endpointFactory,
                          BiFunction<RaftEndpoint, RaftConfig, RaftStore> raftStoreFactory,
                          BiFunction<RaftEndpoint, RaftConfig, Supplier<RestoredRaftState>> raftStateLoaderFactory) {
        this.initialMembers = new RaftEndpoint[size];
        this.members = new RaftEndpoint[size];
        this.runtimes = new LocalRaftNodeRuntime[size];
        this.config = config;
        this.appendNopEntryOnLeaderElection = appendNopEntryOnLeaderElection;

        if (endpointFactory != null) {
            this.endpointFactory = endpointFactory;
        }

        this.raftStoreFactory = raftStoreFactory;

        for (; createdNodeCount < size; createdNodeCount++) {
            LocalRaftNodeRuntime runtime = createNewLocalRaftNodeRuntime();
            runtimes[createdNodeCount] = runtime;
            initialMembers[createdNodeCount] = runtime.getLocalMember();
            members[createdNodeCount] = runtime.getLocalMember();
        }

        nodes = new RaftNodeImpl[size];
        for (int i = 0; i < size; i++) {
            LocalRaftNodeRuntime runtime = runtimes[i];

            if (raftStoreFactory == null) {
                if (raftStateLoaderFactory != null) {
                    try {
                        Supplier<RestoredRaftState> loader = raftStateLoaderFactory.apply(members[i], config);
                        RestoredRaftState state = loader.get();
                        assertNotNull(state);
                        nodes[i] = (RaftNodeImpl) newBuilder().setGroupId("default").setRestoredState(state).setConfig(config)
                                                              .setRuntime(runtime).setStateMachine(runtime).build();
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                } else {
                    nodes[i] = (RaftNodeImpl) newBuilder().setGroupId("default").setLocalEndpoint(members[i])
                                                          .setInitialGroupMembers(asList(members)).setConfig(config)
                                                          .setRuntime(runtime).setStateMachine(runtime).build();
                }

            } else {
                RaftStore raftStore = raftStoreFactory.apply(members[i], config);
                if (raftStateLoaderFactory != null) {
                    try {
                        Supplier<RestoredRaftState> loader = raftStateLoaderFactory.apply(members[i], config);
                        RestoredRaftState state = loader.get();
                        assertNotNull(state);
                        nodes[i] = (RaftNodeImpl) newBuilder().setGroupId("default").setRestoredState(state).setConfig(config)
                                                              .setRuntime(runtime).setStateMachine(runtime).setStore(raftStore)
                                                              .build();
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                } else {
                    nodes[i] = (RaftNodeImpl) newBuilder().setGroupId("default").setLocalEndpoint(members[i])
                                                          .setInitialGroupMembers(asList(members)).setConfig(config)
                                                          .setRuntime(runtime).setStateMachine(runtime).setStore(raftStore)
                                                          .build();
                }
            }
        }
    }

    private LocalRaftNodeRuntime createNewLocalRaftNodeRuntime() {
        return createNewLocalRaftRuntime(endpointFactory.get());
    }

    private LocalRaftNodeRuntime createNewLocalRaftRuntime(LocalRaftEndpoint endpoint) {
        return new LocalRaftNodeRuntime(endpoint, appendNopEntryOnLeaderElection);
    }

    public void start() {
        startWithoutDiscovery();
        initDiscovery();
    }

    public void startWithoutDiscovery() {
        for (RaftNodeImpl node : nodes) {
            node.start();
        }
    }

    private void initDiscovery() {
        for (LocalRaftNodeRuntime runtime : runtimes) {
            for (int i = 0; i < size(); i++) {
                if (runtimes[i].isShutdown()) {
                    continue;
                }
                RaftNodeImpl node = nodes[i];
                if (!node.getLocalEndpoint().equals(runtime.getLocalMember())) {
                    runtime.discoverNode(node);
                }
            }
        }
    }

    public int size() {
        return members.length;
    }

    public RaftNodeImpl createNewRaftNode() {
        int oldSize = this.runtimes.length;
        int newSize = oldSize + 1;
        RaftEndpoint[] endpoints = new RaftEndpoint[newSize];
        LocalRaftNodeRuntime[] runtimes = new LocalRaftNodeRuntime[newSize];
        RaftNodeImpl[] nodes = new RaftNodeImpl[newSize];
        arraycopy(this.members, 0, endpoints, 0, oldSize);
        arraycopy(this.runtimes, 0, runtimes, 0, oldSize);
        arraycopy(this.nodes, 0, nodes, 0, oldSize);
        LocalRaftNodeRuntime runtime = createNewLocalRaftNodeRuntime();
        createdNodeCount++;
        runtimes[oldSize] = runtime;
        RaftEndpoint endpoint = runtime.getLocalMember();
        endpoints[oldSize] = endpoint;
        RaftStore raftStore = raftStoreFactory != null ? raftStoreFactory.apply(endpoint, config) : NopRaftStore.INSTANCE;
        RaftNodeImpl node = (RaftNodeImpl) newBuilder().setGroupId("default").setLocalEndpoint(endpoint)
                                                       .setInitialGroupMembers(asList(initialMembers)).setConfig(config)
                                                       .setRuntime(runtime).setStateMachine(runtime).setStore(raftStore).build();
        nodes[oldSize] = node;
        this.members = endpoints;
        this.runtimes = runtimes;
        this.nodes = nodes;

        node.start();
        initDiscovery();

        return node;
    }

    public RaftNodeImpl createNewRaftNode(RestoredRaftState restoredRaftState, RaftStore store) {
        requireNonNull(restoredRaftState);
        int oldSize = this.runtimes.length;
        int newSize = oldSize + 1;
        RaftEndpoint[] endpoints = new RaftEndpoint[newSize];
        LocalRaftNodeRuntime[] runtimes = new LocalRaftNodeRuntime[newSize];
        RaftNodeImpl[] nodes = new RaftNodeImpl[newSize];
        System.arraycopy(this.members, 0, endpoints, 0, oldSize);
        System.arraycopy(this.runtimes, 0, runtimes, 0, oldSize);
        System.arraycopy(this.nodes, 0, nodes, 0, oldSize);
        LocalRaftNodeRuntime runtime = createNewLocalRaftRuntime((LocalRaftEndpoint) restoredRaftState.getLocalEndpoint());
        createdNodeCount++;
        runtimes[oldSize] = runtime;
        RaftEndpoint endpoint = runtime.getLocalMember();
        // TODO [basri] check if any already running raft node exists with the same endpoint
        endpoints[oldSize] = endpoint;
        RaftNodeImpl node = (RaftNodeImpl) newBuilder().setGroupId("default").setRestoredState(restoredRaftState)
                                                       .setConfig(config).setRuntime(runtime).setStateMachine(runtime)
                                                       .setStore(store).build();
        nodes[oldSize] = node;
        this.members = endpoints;
        this.runtimes = runtimes;
        this.nodes = nodes;

        node.start();
        initDiscovery();

        return node;
    }

    public RaftNodeImpl[] getNodes() {
        return nodes;
    }

    public RaftNodeImpl[] getNodesExcept(RaftEndpoint endpoint) {
        RaftNodeImpl[] n = new RaftNodeImpl[nodes.length - 1];
        int i = 0;
        for (RaftNodeImpl node : nodes) {
            if (!node.getLocalEndpoint().equals(endpoint)) {
                n[i++] = node;
            }
        }

        if (i != n.length) {
            throw new IllegalArgumentException();
        }

        return n;
    }

    public RaftNodeImpl getNode(RaftEndpoint endpoint) {
        return nodes[getIndexOfRunning(endpoint)];
    }

    public int getIndexOfRunning(RaftEndpoint endpoint) {
        assertNotNull(endpoint);
        for (int i = 0; i < members.length; i++) {
            if (!runtimes[i].isShutdown() && endpoint.equals(members[i])) {
                return i;
            }
        }

        throw new IllegalArgumentException("Unknown endpoint: " + endpoint);
    }

    public RaftEndpoint[] getFollowerEndpoints() {
        RaftEndpoint leaderEndpoint = getLeaderEndpoint();
        RaftEndpoint[] n = new RaftEndpoint[members.length - 1];
        int i = 0;
        for (RaftEndpoint member : members) {
            if (!member.equals(leaderEndpoint)) {
                n[i++] = member;
            }
        }

        if (i != n.length) {
            throw new IllegalArgumentException();
        }

        return n;
    }

    public RaftEndpoint getLeaderEndpoint() {
        RaftEndpoint leader = null;
        for (int i = 0; i < size(); i++) {
            if (runtimes[i].isShutdown()) {
                continue;
            }
            RaftNodeImpl node = nodes[i];
            RaftEndpoint endpoint = node.getLeaderEndpoint();
            if (leader == null) {
                leader = endpoint;
            } else if (!leader.equals(endpoint)) {
                throw new AssertionError("Group doesn't have a single leader endpoint yet!");
            }
        }
        return leader;
    }

    public RaftEndpoint getEndpoint(int index) {
        return members[index];
    }

    public LocalRaftNodeRuntime getRuntime(RaftEndpoint endpoint) {
        return getRuntime(getIndexOfRunning(endpoint));
    }

    public LocalRaftNodeRuntime getRuntime(int index) {
        return runtimes[index];
    }

    public RaftNodeImpl waitUntilLeaderElected() {
        RaftNodeImpl[] leaderRef = new RaftNodeImpl[1];
        eventually(() -> {
            RaftNodeImpl leaderNode = getLeaderNode();
            assertThat(leaderNode).isNotNull();

            int leaderTerm = getTerm(leaderNode);

            for (RaftNodeImpl raftNode : nodes) {
                if (runtimes[getIndexOf(raftNode.getLocalEndpoint())].isShutdown()) {
                    continue;
                }

                RaftEndpoint leader = raftNode.getLeaderEndpoint();
                assertThat(leader).isEqualTo(leaderNode.getLocalEndpoint());
                assertThat(getTerm(raftNode)).isEqualTo(leaderTerm);
            }

            leaderRef[0] = leaderNode;
        });

        return leaderRef[0];
    }

    public RaftNodeImpl getLeaderNode() {
        RaftEndpoint leaderEndpoint = getLeaderEndpoint();
        if (leaderEndpoint == null) {
            return null;
        }
        for (int i = 0; i < size(); i++) {
            if (runtimes[i].isShutdown()) {
                continue;
            }
            RaftNodeImpl node = nodes[i];
            if (leaderEndpoint.equals(node.getLocalEndpoint())) {
                return node;
            }
        }
        throw new AssertionError("Leader endpoint is " + leaderEndpoint + ", but leader node could not be found!");
    }

    public int getIndexOf(RaftEndpoint endpoint) {
        assertThat(endpoint).isNotNull();
        for (int i = 0; i < members.length; i++) {
            if (endpoint.equals(members[i])) {
                return i;
            }
        }
        throw new IllegalArgumentException("Unknown endpoint: " + endpoint);
    }

    public RaftNodeImpl getAnyFollowerNode() {
        RaftEndpoint leaderEndpoint = getLeaderEndpoint();
        if (leaderEndpoint == null) {
            throw new AssertionError("Group doesn't have a leader yet!");
        }
        for (int i = 0; i < size(); i++) {
            if (runtimes[i].isShutdown()) {
                continue;
            }
            RaftNodeImpl node = nodes[i];
            if (!leaderEndpoint.equals(node.getLocalEndpoint())) {
                return node;
            }
        }
        throw new AssertionError("There's no follower node available!");
    }

    public void destroy() {
        for (int i = 0; i < nodes.length; i++) {
            if (!runtimes[i].isShutdown()) {
                nodes[i].terminate();
            }
        }

        for (LocalRaftNodeRuntime runtime : runtimes) {
            runtime.shutdown();
        }
    }

    /**
     * Split nodes having these members from rest of the cluster.
     */
    public void splitMembers(RaftEndpoint... endpoints) {
        int[] indexes = new int[endpoints.length];
        for (int i = 0; i < indexes.length; i++) {
            indexes[i] = getIndexOfRunning(endpoints[i]);
        }
        split(indexes);
    }

    /**
     * Split nodes with these indexes from rest of the cluster.
     */
    public void split(int... indexes) {
        assertThat(indexes).hasSizeBetween(1, size() - 1);

        int[] firstSplit = new int[size() - indexes.length];
        int[] secondSplit = indexes;

        int ix = 0;
        for (int i = 0; i < size(); i++) {
            if (Arrays.binarySearch(indexes, i) < 0) {
                firstSplit[ix++] = i;
            }
        }

        split(secondSplit, firstSplit);
        split(firstSplit, secondSplit);

    }

    private void split(int[] firstSplit, int[] secondSplit) {
        for (int i : firstSplit) {
            for (int j : secondSplit) {
                runtimes[i].removeNode(nodes[j]);
            }
        }
    }

    public int[] createMinoritySplitIndexes(boolean includingLeader) {
        return createSplitIndexes(includingLeader, minority(size()));
    }

    public int[] createSplitIndexes(boolean includingLeader, int splitSize) {
        int leader = getLeaderIndex();

        int[] indexes = new int[splitSize];
        int ix = 0;

        if (includingLeader) {
            indexes[0] = leader;
            ix = 1;
        }

        for (int i = 0; i < size(); i++) {
            if (i == leader) {
                continue;
            }
            if (ix == indexes.length) {
                break;
            }
            indexes[ix++] = i;
        }
        return indexes;
    }

    public int getLeaderIndex() {
        RaftEndpoint leaderEndpoint = getLeaderEndpoint();
        if (leaderEndpoint == null) {
            return -1;
        }
        for (int i = 0; i < members.length; i++) {
            if (leaderEndpoint.equals(members[i])) {
                return i;
            }
        }
        throw new AssertionError("Leader endpoint is " + leaderEndpoint + ", but this endpoint is unknown to group!");
    }

    public int[] createMajoritySplitIndexes(boolean includingLeader) {
        return createSplitIndexes(includingLeader, majority(size()));
    }

    public void merge() {
        initDiscovery();
    }

    /**
     * Drops specific message type one-way between from -> to.
     */
    public <T extends RaftMessage> void dropMessagesToMember(RaftEndpoint from, RaftEndpoint to, Class<T> messageType) {
        getRuntime(getIndexOfRunning(from)).dropMessagesToEndpoint(to, messageType);
    }

    /**
     * Allows specific message type one-way between from -> to.
     */
    public <T extends RaftMessage> void allowMessagesToMember(RaftEndpoint from, RaftEndpoint to, Class<T> messageType) {
        LocalRaftNodeRuntime runtime = getRuntime(getIndexOfRunning(from));
        if (!runtime.isReachable(to)) {
            throw new IllegalStateException(
                    "Cannot allow " + messageType + " from " + from + " -> " + to + ", since all messages are dropped"
                            + " between.");
        }
        runtime.allowMessagesToEndpoint(to, messageType);
    }

    /**
     * Drops all kind of messages one-way between from -> to.
     */
    public void dropAllMessagesToMember(RaftEndpoint from, RaftEndpoint to) {
        getRuntime(getIndexOfRunning(from)).removeNode(getNode(getIndexOfRunning(to)));
    }

    public RaftNodeImpl getNode(int index) {
        return nodes[index];
    }

    /**
     * Allows all kind of messages one-way between from -> to.
     */
    public void allowAllMessagesToMember(RaftEndpoint from, RaftEndpoint to) {
        LocalRaftNodeRuntime runtime = getRuntime(getIndexOfRunning(from));
        runtime.allowAllMessagesToEndpoint(to);
        runtime.discoverNode(getNode(getIndexOfRunning(to)));
    }

    /**
     * Drops specific message type one-way from -> to all nodes.
     */
    public <T extends RaftMessage> void dropMessagesToAll(RaftEndpoint from, Class<T> messageType) {
        getRuntime(getIndexOfRunning(from)).dropMessagesToAll(messageType);
    }

    /**
     * Allows specific message type one-way from -> to all nodes.
     */
    public <T extends RaftMessage> void allowMessagesToAll(RaftEndpoint from, Class<T> messageType) {
        LocalRaftNodeRuntime runtime = getRuntime(getIndexOfRunning(from));
        for (RaftEndpoint endpoint : members) {
            if (!runtime.isReachable(endpoint)) {
                throw new IllegalStateException("Cannot allow " + messageType + " from " + from + " -> " + endpoint
                        + ", since all messages are dropped between.");
            }
        }
        runtime.allowMessagesToAll(messageType);
    }

    /**
     * Resets all rules from endpoint.
     */
    public void resetAllRulesFrom(RaftEndpoint endpoint) {
        getRuntime(getIndexOfRunning(endpoint)).resetAllRules();
    }

    public void alterMessagesToMember(RaftEndpoint from, RaftEndpoint to, Function<RaftMessage, RaftMessage> function) {
        getRuntime(getIndexOfRunning(from)).alterMessagesToEndpoint(to, function);
    }

    void removeAlterMessageRuleToMember(RaftEndpoint from, RaftEndpoint to) {
        getRuntime(getIndexOfRunning(from)).removeAlterMessageRuleToEndpoint(to);
    }

    public void terminateNode(int index) {
        // TODO [basri] do we need to split() here...
        split(index);
        getRuntime(index).shutdown();
    }

    public void terminateNode(RaftEndpoint endpoint) {
        assertNotNull(endpoint);

        for (int i = 0; i < members.length; i++) {
            if (endpoint.equals(members[i])) {
                split(i);
                runtimes[i].shutdown();
            }
        }
    }

    public boolean isRunning(RaftEndpoint endpoint) {
        assertNotNull(endpoint);
        for (int i = 0; i < members.length; i++) {
            if (!endpoint.equals(members[i])) {
                continue;
            }
            return !runtimes[i].isShutdown();
        }

        throw new IllegalArgumentException("Unknown endpoint: " + endpoint);
    }

}
