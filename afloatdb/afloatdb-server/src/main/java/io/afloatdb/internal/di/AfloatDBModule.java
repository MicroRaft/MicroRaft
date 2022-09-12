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

package io.afloatdb.internal.di;

import com.google.inject.AbstractModule;
import com.google.inject.TypeLiteral;
import io.afloatdb.config.AfloatDBConfig;
import io.afloatdb.internal.lifecycle.ProcessTerminationLogger;
import io.afloatdb.internal.lifecycle.impl.ProcessTerminationLoggerImpl;
import io.afloatdb.internal.raft.RaftNodeReportSupplier;
import io.afloatdb.internal.raft.impl.AfloatDBClusterEndpointsPublisher;
import io.afloatdb.internal.raft.impl.KVStoreStateMachine;
import io.afloatdb.internal.raft.impl.RaftNodeSupplier;
import io.afloatdb.internal.raft.impl.model.ProtoRaftModelFactory;
import io.afloatdb.internal.rpc.RaftRpcService;
import io.afloatdb.internal.rpc.RpcServer;
import io.afloatdb.internal.rpc.impl.KVRequestHandler;
import io.afloatdb.internal.rpc.impl.ManagementRequestHandler;
import io.afloatdb.internal.rpc.impl.RaftMessageHandler;
import io.afloatdb.internal.rpc.impl.RaftRpcServiceImpl;
import io.afloatdb.internal.rpc.impl.RpcServerImpl;
import io.afloatdb.kv.proto.KVRequestHandlerGrpc.KVRequestHandlerImplBase;
import io.afloatdb.management.proto.ManagementRequestHandlerGrpc.ManagementRequestHandlerImplBase;
import io.afloatdb.raft.proto.RaftMessageHandlerGrpc.RaftMessageHandlerImplBase;
import io.microraft.RaftEndpoint;
import io.microraft.RaftNode;
import io.microraft.model.RaftModelFactory;
import io.microraft.statemachine.StateMachine;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

import static com.google.inject.name.Names.named;

public class AfloatDBModule extends AbstractModule {

    public static final String CONFIG_KEY = "Config";
    public static final String LOCAL_ENDPOINT_KEY = "LocalEndpoint";
    public static final String INITIAL_ENDPOINTS_KEY = "InitialEndpoints";
    public static final String RAFT_ENDPOINT_ADDRESSES_KEY = "RaftEndpointAddresses";
    public static final String RAFT_NODE_SUPPLIER_KEY = "RaftNodeSupplier";

    private final AfloatDBConfig config;
    private final RaftEndpoint localEndpoint;
    private final List<RaftEndpoint> initialEndpoints;
    private final Map<RaftEndpoint, String> addresses;
    private final AtomicBoolean processTerminationFlag;

    public AfloatDBModule(AfloatDBConfig config, RaftEndpoint localEndpoint, List<RaftEndpoint> initialEndpoints,
            Map<RaftEndpoint, String> addresses, AtomicBoolean processTerminationFlag) {
        this.config = config;
        this.localEndpoint = localEndpoint;
        this.initialEndpoints = initialEndpoints;
        this.addresses = addresses;
        this.processTerminationFlag = processTerminationFlag;
    }

    @Override
    protected void configure() {
        bind(AtomicBoolean.class).annotatedWith(named(ProcessTerminationLoggerImpl.PROCESS_TERMINATION_FLAG_KEY))
                .toInstance(processTerminationFlag);
        bind(ProcessTerminationLogger.class).to(ProcessTerminationLoggerImpl.class);

        bind(AfloatDBConfig.class).annotatedWith(named(CONFIG_KEY)).toInstance(config);
        bind(RaftEndpoint.class).annotatedWith(named(LOCAL_ENDPOINT_KEY)).toInstance(localEndpoint);
        bind(new TypeLiteral<Collection<RaftEndpoint>>() {
        }).annotatedWith(named(INITIAL_ENDPOINTS_KEY)).toInstance(initialEndpoints);
        bind(new TypeLiteral<Map<RaftEndpoint, String>>() {
        }).annotatedWith(named(RAFT_ENDPOINT_ADDRESSES_KEY)).toInstance(addresses);

        bind(RaftNodeReportSupplier.class).to(AfloatDBClusterEndpointsPublisher.class);
        bind(StateMachine.class).to(KVStoreStateMachine.class);
        bind(RaftModelFactory.class).to(ProtoRaftModelFactory.class);
        bind(RaftMessageHandlerImplBase.class).to(RaftMessageHandler.class);
        bind(RpcServer.class).to(RpcServerImpl.class);
        bind(RaftRpcService.class).to(RaftRpcServiceImpl.class);
        bind(KVRequestHandlerImplBase.class).to(KVRequestHandler.class);
        bind(ManagementRequestHandlerImplBase.class).to(ManagementRequestHandler.class);

        bind(new TypeLiteral<Supplier<RaftNode>>() {
        }).annotatedWith(named(RAFT_NODE_SUPPLIER_KEY)).to(RaftNodeSupplier.class);
    }

}
