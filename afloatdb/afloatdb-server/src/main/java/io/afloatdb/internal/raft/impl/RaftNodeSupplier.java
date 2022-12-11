/*
 * Copyright (c) 2020, AfloatDB.
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

package io.afloatdb.internal.raft.impl;

import io.afloatdb.AfloatDBException;
import io.afloatdb.config.AfloatDBConfig;
import io.afloatdb.internal.lifecycle.ProcessTerminationLogger;
import io.afloatdb.internal.raft.RaftNodeReportSupplier;
import io.afloatdb.internal.rpc.RaftRpcService;
import io.microraft.RaftEndpoint;
import io.microraft.RaftNode;
import io.microraft.model.RaftModelFactory;
import io.microraft.statemachine.StateMachine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import io.microraft.persistence.RaftStore;
import io.microraft.store.sqlite.RaftSqliteStore;
import io.microraft.persistence.RestoredRaftState;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Singleton;
import java.util.Collection;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.Optional;

import static io.afloatdb.internal.di.AfloatDBModule.CONFIG_KEY;
import static io.afloatdb.internal.di.AfloatDBModule.INITIAL_ENDPOINTS_KEY;
import static io.afloatdb.internal.di.AfloatDBModule.LOCAL_ENDPOINT_KEY;
/**
 * Creates and contains a RaftNode instance in the AfloatDB instance
 */
@Singleton
public class RaftNodeSupplier implements Supplier<RaftNode> {

    private static final Logger LOGGER = LoggerFactory.getLogger(RaftNodeSupplier.class);

    private final RaftNode raftNode;
    private final ProcessTerminationLogger processTerminationLogger;

    @Inject
    public RaftNodeSupplier(@Named(CONFIG_KEY) AfloatDBConfig config,
            @Named(LOCAL_ENDPOINT_KEY) RaftEndpoint localEndpoint,
            @Named(INITIAL_ENDPOINTS_KEY) Collection<RaftEndpoint> initialGroupMembers, RaftRpcService rpcService,
            StateMachine stateMachine, RaftModelFactory modelFactory, RaftNodeReportSupplier raftNodeReportSupplier,
            RaftStoreSupplier raftStoreSupplier, ProcessTerminationLogger processTerminationLogger) {
        RaftSqliteStore store = raftStoreSupplier.get();

        RaftNode.RaftNodeBuilder builder = RaftNode.newBuilder().setGroupId(config.getRaftGroupConfig().getId())

                .setConfig(config.getRaftConfig()).setTransport(rpcService).setStateMachine(stateMachine)
                .setModelFactory(modelFactory).setRaftNodeReportListener(raftNodeReportSupplier).setStore(store);

        Optional<RestoredRaftState> restoredRaftStateOpt = store.getRestoredRaftState();
        if (restoredRaftStateOpt.isPresent()) {
            RestoredRaftState restoredRaftState = restoredRaftStateOpt.get();
            LOGGER.info(
                    "{} restored local endpoint: {}, voting: {}, initial group members: {}, term: {}, voted for: {}",
                    localEndpoint.getId(), restoredRaftState.isLocalEndpointVoting(),
                    restoredRaftState.getInitialGroupMembers(), restoredRaftState.getTerm(),
                    restoredRaftState.getVotedMember());
            builder.setRestoredState(restoredRaftState);
        } else {
            builder.setLocalEndpoint(localEndpoint).setInitialGroupMembers(initialGroupMembers);
        }

        this.raftNode = builder.build();
        this.processTerminationLogger = processTerminationLogger;
    }

    @PostConstruct
    public void start() {
        LOGGER.info("{} starting Raft node...", raftNode.getLocalEndpoint().getId());
        try {
            raftNode.start().join();
        } catch (Throwable t) {
            throw new AfloatDBException(raftNode.getLocalEndpoint().getId() + " could not start Raft node!", t);
        }
    }

    @PreDestroy
    public void shutdown() {
        processTerminationLogger.logInfo(LOGGER, raftNode.getLocalEndpoint().getId() + " terminating Raft node...");

        try {
            // TODO [basri] make this configurable.
            raftNode.terminate().get(10, TimeUnit.SECONDS);
            processTerminationLogger.logInfo(LOGGER, raftNode.getLocalEndpoint().getId() + " RaftNode is terminated.");
        } catch (Throwable t) {
            if (t instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }

            String message = raftNode.getLocalEndpoint().getId() + " failure during termination of Raft node";
            processTerminationLogger.logError(LOGGER, message, t);
        }
    }

    @Override
    public RaftNode get() {
        return raftNode;
    }

}
