package com.hazelcast.raft.impl.local;

import com.hazelcast.raft.RaftConfig;
import com.hazelcast.raft.RaftEndpoint;
import com.hazelcast.raft.impl.RaftNodeImpl;
import com.hazelcast.raft.impl.util.AssertUtil;

import java.util.Arrays;
import java.util.function.Function;

import static com.hazelcast.raft.RaftConfig.DEFAULT_RAFT_CONFIG;
import static com.hazelcast.raft.impl.local.LocalRaftEndpoint.newEndpoint;
import static com.hazelcast.raft.impl.util.RaftUtil.getLeaderMember;
import static com.hazelcast.raft.impl.util.RaftUtil.getTerm;
import static com.hazelcast.raft.impl.util.RaftUtil.majority;
import static com.hazelcast.raft.impl.util.RaftUtil.minority;
import static java.util.Arrays.asList;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Represents a single Raft group of which all members run locally,
 * and provides methods to access specific nodes, terminate nodes, split/merge
 * the group and define allow/drop rules between nodes.
 *
 * @author mdogan
 * @author metanet
 */
public class LocalRaftGroup {

    private final RaftConfig raftConfig;
    private final boolean appendNopEntryOnLeaderElection;
    private RaftEndpoint[] initialMembers;
    private RaftEndpoint[] members;
    private LocalRaftIntegration[] integrations;
    private RaftNodeImpl[] nodes;
    private int createdNodeCount;

    public LocalRaftGroup(int size) {
        this(size, DEFAULT_RAFT_CONFIG);
    }

    public LocalRaftGroup(int size, RaftConfig raftConfig) {
        this(size, raftConfig, false);
    }

    public LocalRaftGroup(int size, RaftConfig raftConfig, boolean appendNopEntryOnLeaderElection) {
        initialMembers = new RaftEndpoint[size];
        members = new RaftEndpoint[size];
        integrations = new LocalRaftIntegration[size];
        this.raftConfig = raftConfig;

        this.appendNopEntryOnLeaderElection = appendNopEntryOnLeaderElection;

        for (; createdNodeCount < size; createdNodeCount++) {
            LocalRaftIntegration integration = createNewLocalRaftIntegration();
            integrations[createdNodeCount] = integration;
            initialMembers[createdNodeCount] = integration.getLocalMember();
            members[createdNodeCount] = integration.getLocalMember();
        }

        nodes = new RaftNodeImpl[size];
        for (int i = 0; i < size; i++) {
            LocalRaftIntegration integration = integrations[i];
            nodes[i] = new RaftNodeImpl("test", members[i], asList(members), raftConfig, integration);
        }
    }

    private LocalRaftIntegration createNewLocalRaftIntegration() {
        return new LocalRaftIntegration(newEndpoint(), appendNopEntryOnLeaderElection);
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
        for (LocalRaftIntegration integration : integrations) {
            for (int i = 0; i < size(); i++) {
                if (integrations[i].isClosed()) {
                    continue;
                }
                RaftNodeImpl node = nodes[i];
                if (!node.getLocalEndpoint().equals(integration.getLocalMember())) {
                    integration.discoverNode(node);
                }
            }
        }
    }

    public RaftNodeImpl createNewRaftNode() {
        int oldSize = this.integrations.length;
        int newSize = oldSize + 1;
        RaftEndpoint[] endpoints = new RaftEndpoint[newSize];
        LocalRaftIntegration[] integrations = new LocalRaftIntegration[newSize];
        RaftNodeImpl[] nodes = new RaftNodeImpl[newSize];
        System.arraycopy(this.members, 0, endpoints, 0, oldSize);
        System.arraycopy(this.integrations, 0, integrations, 0, oldSize);
        System.arraycopy(this.nodes, 0, nodes, 0, oldSize);
        LocalRaftIntegration integration = createNewLocalRaftIntegration();
        createdNodeCount++;
        integrations[oldSize] = integration;
        RaftEndpoint endpoint = integration.getLocalMember();
        endpoints[oldSize] = endpoint;
        RaftNodeImpl node = new RaftNodeImpl("default", endpoint, asList(initialMembers), raftConfig, integration);
        nodes[oldSize] = node;
        this.members = endpoints;
        this.integrations = integrations;
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

    public RaftNodeImpl getNode(int index) {
        return nodes[index];
    }

    public RaftNodeImpl getNode(RaftEndpoint endpoint) {
        return nodes[getIndexOf(endpoint)];
    }

    public RaftEndpoint getEndpoint(int index) {
        return members[index];
    }

    public LocalRaftIntegration getIntegration(int index) {
        return integrations[index];
    }

    public LocalRaftIntegration getIntegration(RaftEndpoint endpoint) {
        return getIntegration(getIndexOf(endpoint));
    }

    public RaftNodeImpl waitUntilLeaderElected() {
        RaftNodeImpl[] leaderRef = new RaftNodeImpl[1];
        AssertUtil.eventually(() -> {
            RaftNodeImpl leaderNode = getLeaderNode();
            assertThat(leaderNode).isNotNull();

            int leaderTerm = getTerm(leaderNode);

            for (RaftNodeImpl raftNode : nodes) {
                if (integrations[getIndexOf(raftNode.getLocalEndpoint())].isClosed()) {
                    continue;
                }

                RaftEndpoint leader = getLeaderMember(raftNode);
                assertThat(leader).isEqualTo(leaderNode.getLocalEndpoint());
                assertThat(getTerm(raftNode)).isEqualTo(leaderTerm);
            }

            leaderRef[0] = leaderNode;
        });

        return leaderRef[0];
    }

    public RaftEndpoint getLeaderEndpoint() {
        RaftEndpoint leader = null;
        for (int i = 0; i < size(); i++) {
            if (integrations[i].isClosed()) {
                continue;
            }
            RaftNodeImpl node = nodes[i];
            RaftEndpoint endpoint = getLeaderMember(node);
            if (leader == null) {
                leader = endpoint;
            } else if (!leader.equals(endpoint)) {
                throw new AssertionError("Group doesn't have a single leader endpoint yet!");
            }
        }
        return leader;
    }

    public RaftNodeImpl getLeaderNode() {
        RaftEndpoint leaderEndpoint = getLeaderEndpoint();
        if (leaderEndpoint == null) {
            return null;
        }
        for (int i = 0; i < size(); i++) {
            if (integrations[i].isClosed()) {
                continue;
            }
            RaftNodeImpl node = nodes[i];
            if (leaderEndpoint.equals(node.getLocalEndpoint())) {
                return node;
            }
        }
        throw new AssertionError("Leader endpoint is " + leaderEndpoint + ", but leader node could not be found!");
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

    public RaftNodeImpl getAnyFollowerNode() {
        RaftEndpoint leaderEndpoint = getLeaderEndpoint();
        if (leaderEndpoint == null) {
            throw new AssertionError("Group doesn't have a leader yet!");
        }
        for (int i = 0; i < size(); i++) {
            if (integrations[i].isClosed()) {
                continue;
            }
            RaftNodeImpl node = nodes[i];
            if (!leaderEndpoint.equals(node.getLocalEndpoint())) {
                return node;
            }
        }
        throw new AssertionError("There's no follower node available!");
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

    public void destroy() {
        for (RaftNodeImpl node : nodes) {
            node.forceTerminate();
        }

        for (LocalRaftIntegration integration : integrations) {
            integration.close();
        }
    }

    public int size() {
        return members.length;
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
                integrations[i].removeNode(nodes[j]);
            }
        }
    }

    /**
     * Split nodes having these members from rest of the cluster.
     */
    public void splitMembers(RaftEndpoint... endpoints) {
        int[] indexes = new int[endpoints.length];
        for (int i = 0; i < indexes.length; i++) {
            indexes[i] = getIndexOf(endpoints[i]);
        }
        split(indexes);
    }

    public int[] createMinoritySplitIndexes(boolean includingLeader) {
        return createSplitIndexes(includingLeader, minority(size()));
    }

    public int[] createMajoritySplitIndexes(boolean includingLeader) {
        return createSplitIndexes(includingLeader, majority(size()));
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

    public void merge() {
        initDiscovery();
    }

    /**
     * Drops specific message type one-way between from -> to.
     */
    public void dropMessagesToMember(RaftEndpoint from, RaftEndpoint to, Class messageType) {
        getIntegration(getIndexOf(from)).dropMessagesToEndpoint(to, messageType);
    }

    /**
     * Allows specific message type one-way between from -> to.
     */
    public void allowMessagesToMember(RaftEndpoint from, RaftEndpoint to, Class messageType) {
        LocalRaftIntegration integration = getIntegration(getIndexOf(from));
        if (!integration.isReachable(to)) {
            throw new IllegalStateException(
                    "Cannot allow " + messageType + " from " + from + " -> " + to + ", since all messages are dropped between.");
        }
        integration.allowMessagesToEndpoint(to, messageType);
    }

    /**
     * Drops all kind of messages one-way between from -> to.
     */
    public void dropAllMessagesToMember(RaftEndpoint from, RaftEndpoint to) {
        getIntegration(getIndexOf(from)).removeNode(getNode(getIndexOf(to)));
    }

    /**
     * Allows all kind of messages one-way between from -> to.
     */
    public void allowAllMessagesToMember(RaftEndpoint from, RaftEndpoint to) {
        LocalRaftIntegration integration = getIntegration(getIndexOf(from));
        integration.allowAllMessagesToEndpoint(to);
        integration.discoverNode(getNode(getIndexOf(to)));
    }

    /**
     * Drops specific message type one-way from -> to all nodes.
     */
    public void dropMessagesToAll(RaftEndpoint from, Class messageType) {
        getIntegration(getIndexOf(from)).dropMessagesToAll(messageType);
    }

    /**
     * Allows specific message type one-way from -> to all nodes.
     */
    public void allowMessagesToAll(RaftEndpoint from, Class messageType) {
        LocalRaftIntegration integration = getIntegration(getIndexOf(from));
        for (RaftEndpoint endpoint : members) {
            if (!integration.isReachable(endpoint)) {
                throw new IllegalStateException("Cannot allow " + messageType + " from " + from + " -> " + endpoint
                        + ", since all messages are dropped between.");
            }
        }
        integration.allowMessagesToAll(messageType);
    }

    /**
     * Resets all rules from endpoint.
     */
    public void resetAllRulesFrom(RaftEndpoint endpoint) {
        getIntegration(getIndexOf(endpoint)).resetAllRules();
    }

    public void alterMessagesToMember(RaftEndpoint from, RaftEndpoint to, Function<Object, Object> function) {
        getIntegration(getIndexOf(from)).alterMessagesToEndpoint(to, function);
    }

    void removeAlterMessageRuleToMember(RaftEndpoint from, RaftEndpoint to) {
        getIntegration(getIndexOf(from)).removeAlterMessageRuleToEndpoint(to);
    }

    public void terminateNode(int index) {
        split(index);
        getIntegration(index).close();
    }

    public void terminateNode(RaftEndpoint endpoint) {
        terminateNode(getIndexOf(endpoint));
    }
}
