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

package io.microraft.impl;

import static io.microraft.RaftConfig.DEFAULT_RAFT_CONFIG;
import static io.microraft.report.RaftGroupMembers.MAX_LEARNER_COUNT;
import static java.util.Objects.requireNonNull;

import java.time.Clock;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.Random;

import javax.annotation.Nonnull;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.microraft.RaftConfig;
import io.microraft.RaftEndpoint;
import io.microraft.RaftNode;
import io.microraft.RaftNode.RaftNodeBuilder;
import io.microraft.RaftRole;
import io.microraft.executor.RaftNodeExecutor;
import io.microraft.executor.impl.DefaultRaftNodeExecutor;
import io.microraft.impl.metrics.MetricsContext;
import io.microraft.model.RaftModelFactory;
import io.microraft.model.impl.DefaultRaftModelFactory;
import io.microraft.model.impl.log.DefaultRaftGroupMembersViewOrBuilder;
import io.microraft.model.log.RaftGroupMembersView;
import io.microraft.persistence.NopRaftStore;
import io.microraft.persistence.RaftStore;
import io.microraft.persistence.RestoredRaftState;
import io.microraft.report.RaftNodeReportListener;
import io.microraft.statemachine.StateMachine;
import io.microraft.transport.Transport;

/**
 * Builder for {@link RaftNode}.
 */
public class RaftNodeBuilderImpl implements RaftNodeBuilder {

    private Object groupId;
    private RaftEndpoint localEndpoint;
    private Collection<RaftEndpoint> initialGroupMembers;
    private Collection<RaftEndpoint> initialVotingGroupMembers;
    private RestoredRaftState restoredState;
    private RaftConfig config = DEFAULT_RAFT_CONFIG;
    private RaftNodeExecutor executor = new DefaultRaftNodeExecutor();
    private Transport transport;
    private StateMachine stateMachine;
    private RaftNodeReportListener listener = report -> {
    };
    private RaftStore store = new NopRaftStore();
    private RaftModelFactory modelFactory = new DefaultRaftModelFactory();
    private Random random = new Random();
    private Clock clock = Clock.systemUTC();
    private MeterRegistry meterRegistry = new SimpleMeterRegistry();
    private boolean done;

    @Nonnull
    @Override
    public RaftNodeBuilder setGroupId(@Nonnull Object groupId) {
        this.groupId = requireNonNull(groupId);
        return this;
    }

    @Nonnull
    @Override
    public RaftNodeBuilder setLocalEndpoint(@Nonnull RaftEndpoint localEndpoint) {
        if (this.restoredState != null) {
            throw new IllegalStateException("Local member cannot be set when restored Raft state is provided!");
        }

        this.localEndpoint = requireNonNull(localEndpoint);
        return this;
    }

    @Nonnull
    @Override
    public RaftNodeBuilder setInitialGroupMembers(@Nonnull Collection<RaftEndpoint> initialGroupMembers) {
        if (this.restoredState != null) {
            throw new IllegalStateException(
                    "Initial group members cannot be set when restored Raft state is provided!");
        }

        this.initialGroupMembers = new LinkedHashSet<>(requireNonNull(initialGroupMembers));
        this.initialVotingGroupMembers = new LinkedHashSet<>(this.initialGroupMembers);
        return this;
    }

    @Nonnull
    @Override
    public RaftNodeBuilder setInitialGroupMembers(@Nonnull Collection<RaftEndpoint> initialGroupMembers,
            @Nonnull Collection<RaftEndpoint> initialVotingGroupMembers) {
        this.initialGroupMembers = new LinkedHashSet<>(requireNonNull(initialGroupMembers));
        Collection<RaftEndpoint> votingMembers = new LinkedHashSet<>(initialGroupMembers);
        votingMembers.retainAll(requireNonNull(initialVotingGroupMembers));
        this.initialVotingGroupMembers = votingMembers;
        return this;
    }

    @Nonnull
    @Override
    public RaftNodeBuilder setRestoredState(@Nonnull RestoredRaftState restoredState) {
        if (this.localEndpoint != null || this.initialGroupMembers != null) {
            throw new IllegalStateException(
                    "Restored state cannot be set when either local member or initial group members is provided!");
        }

        this.restoredState = requireNonNull(restoredState);
        return this;
    }

    @Nonnull
    @Override
    public RaftNodeBuilder setConfig(@Nonnull RaftConfig config) {
        this.config = requireNonNull(config);
        return this;
    }

    @Nonnull
    @Override
    public RaftNodeBuilder setExecutor(@Nonnull RaftNodeExecutor executor) {
        this.executor = requireNonNull(executor);
        return this;
    }

    @Nonnull
    @Override
    public RaftNodeBuilder setTransport(@Nonnull Transport transport) {
        this.transport = requireNonNull(transport);
        return this;
    }

    @Nonnull
    @Override
    public RaftNodeBuilder setStateMachine(@Nonnull StateMachine stateMachine) {
        this.stateMachine = requireNonNull(stateMachine);
        return this;
    }

    @Nonnull
    @Override
    public RaftNodeBuilder setStore(@Nonnull RaftStore store) {
        this.store = requireNonNull(store);
        return this;
    }

    @Nonnull
    @Override
    public RaftNodeBuilder setModelFactory(@Nonnull RaftModelFactory modelFactory) {
        this.modelFactory = requireNonNull(modelFactory);
        return this;
    }

    @Nonnull
    @Override
    public RaftNodeBuilder setRaftNodeReportListener(@Nonnull RaftNodeReportListener listener) {
        this.listener = requireNonNull(listener);
        return this;
    }

    @Nonnull
    @Override
    public RaftNodeBuilder setRandom(@Nonnull Random random) {
        this.random = requireNonNull(random);
        return this;
    }

    @Nonnull
    @Override
    public RaftNodeBuilder setClock(@Nonnull Clock clock) {
        this.clock = requireNonNull(clock);
        return this;
    }

    @Override
    @Nonnull
    public RaftNodeBuilder setMeterRegistry(@Nonnull MeterRegistry meterRegistry) {
        this.meterRegistry = requireNonNull(meterRegistry);
        return this;
    }

    @Nonnull
    @Override
    public RaftNode build() {
        if (done) {
            throw new IllegalStateException("Raft node is already built!");
        }

        if (!((localEndpoint != null && initialGroupMembers != null && !initialGroupMembers.isEmpty()
                && !initialVotingGroupMembers.isEmpty()
                && initialGroupMembers.size() - initialVotingGroupMembers.size() <= MAX_LEARNER_COUNT)
                || restoredState != null)) {
            String message = "Either local Raft endpoint and initial Raft group members, or restored state must be provided! In addition, "
                    + "there can be at most " + MAX_LEARNER_COUNT + " " + RaftRole.LEARNER + "s";
            throw new IllegalStateException(message);
        }

        MetricsContext metricsContext = new MetricsContext(clock, meterRegistry, groupId,
                localEndpoint != null
                        ? localEndpoint
                        : restoredState.getLocalEndpointPersistentState().getLocalEndpoint());

        done = true;
        if (restoredState != null) {
            return new RaftNodeImpl(groupId, restoredState, config, executor, stateMachine, transport, modelFactory,
                    store, listener, random, clock, metricsContext);
        } else {
            // this groupMembers object does not hit network or disk.
            RaftGroupMembersView groupMembers = new DefaultRaftGroupMembersViewOrBuilder().setLogIndex(0)
                    .setMembers(initialGroupMembers).setVotingMembers(initialVotingGroupMembers).build();
            return new RaftNodeImpl(groupId, localEndpoint, groupMembers, config, executor, stateMachine, transport,
                    modelFactory, store, listener, random, clock, metricsContext);
        }
    }

}
