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

package io.afloatdb.internal.rpc.impl;

import io.afloatdb.AfloatDBException;
import io.afloatdb.cluster.proto.AfloatDBClusterServiceGrpc.AfloatDBClusterServiceImplBase;
import io.afloatdb.config.AfloatDBConfig;
import io.afloatdb.internal.lifecycle.ProcessTerminationLogger;
import io.afloatdb.internal.lifecycle.TerminationAware;
import io.afloatdb.internal.raft.RaftNodeReportSupplier;
import io.afloatdb.internal.rpc.RpcServer;
import io.grpc.Server;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import io.grpc.netty.shaded.io.netty.channel.EventLoopGroup;
import io.grpc.netty.shaded.io.netty.channel.ServerChannel;
import io.grpc.netty.shaded.io.netty.channel.nio.NioEventLoopGroup;
import io.grpc.netty.shaded.io.netty.channel.socket.nio.NioServerSocketChannel;
import io.microraft.RaftEndpoint;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Singleton;
import java.io.IOException;
import java.util.concurrent.TimeUnit;

import static io.afloatdb.internal.di.AfloatDBModule.CONFIG_KEY;
import static io.afloatdb.internal.di.AfloatDBModule.LOCAL_ENDPOINT_KEY;

@Singleton
public class RpcServerImpl implements RpcServer {

    private static final Logger LOGGER = LoggerFactory.getLogger(RpcServer.class);

    private final RaftEndpoint localEndpoint;
    private final Server server;
    private final ProcessTerminationLogger processTerminationLogger;

    @Inject
    public RpcServerImpl(@Named(LOCAL_ENDPOINT_KEY) RaftEndpoint localEndpoint,
            @Named(CONFIG_KEY) AfloatDBConfig config, KVRequestHandler kvRequestHandler,
            RaftMessageHandler raftMessageHandler, ManagementRequestHandler managementRequestHandler,
            RaftNodeReportSupplier raftNodeReportSupplier, ProcessTerminationLogger processTerminationLogger) {
        this.localEndpoint = localEndpoint;
        // TODO [basri] do perf analysis for this setup
        EventLoopGroup boss = new NioEventLoopGroup(1);
        EventLoopGroup worker = new NioEventLoopGroup(1);
        Class<? extends ServerChannel> channelType = NioServerSocketChannel.class;
        this.server = NettyServerBuilder.forAddress(config.getLocalEndpointConfig().getSocketAddress())
                .bossEventLoopGroup(boss).workerEventLoopGroup(worker).channelType(channelType)
                .addService(kvRequestHandler).addService(raftMessageHandler).addService(managementRequestHandler)
                .addService((AfloatDBClusterServiceImplBase) raftNodeReportSupplier).directExecutor().build();
        this.processTerminationLogger = processTerminationLogger;
    }

    @PostConstruct
    public void start() {
        try {
            server.start();
            LOGGER.info(localEndpoint.getId() + " RpcServer started.");
        } catch (IOException e) {
            throw new AfloatDBException(localEndpoint.getId() + " RpcServer start failed!", e);
        }
    }

    @PreDestroy
    public void shutdown() {
        processTerminationLogger.logInfo(LOGGER, localEndpoint.getId() + " shutting down RpcServer...");

        try {
            server.shutdownNow();
            processTerminationLogger.logInfo(LOGGER, localEndpoint.getId() + " RpcServer is shut down.");
        } catch (Throwable t) {
            String message = localEndpoint.getId() + " failure during termination of RpcServer";
            processTerminationLogger.logError(LOGGER, message, t);
        }
    }

    @Override
    public void awaitTermination() {
        try {
            server.awaitTermination();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            processTerminationLogger.logWarn(LOGGER,
                    localEndpoint.getId() + " await termination of RpcServer interrupted!");
        }
    }

}
