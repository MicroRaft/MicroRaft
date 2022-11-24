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

import static io.afloatdb.internal.di.AfloatDBModule.CONFIG_KEY;
import static io.afloatdb.internal.di.AfloatDBModule.LOCAL_ENDPOINT_KEY;
import static io.afloatdb.internal.di.AfloatDBModule.RAFT_ENDPOINT_ADDRESSES_KEY;
import static io.afloatdb.internal.utils.Exceptions.runSilently;
import static io.afloatdb.internal.utils.Serialization.wrap;
import static io.afloatdb.internal.utils.Threads.nextThreadId;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.Executors.newSingleThreadScheduledExecutor;
import static java.util.concurrent.TimeUnit.SECONDS;

import io.afloatdb.config.AfloatDBConfig;
import io.afloatdb.internal.lifecycle.ProcessTerminationLogger;
import io.afloatdb.internal.rpc.RaftRpcService;
import io.afloatdb.raft.proto.RaftRequest;
import io.afloatdb.raft.proto.RaftResponse;
import io.afloatdb.raft.proto.RaftServiceGrpc;
import io.afloatdb.raft.proto.RaftServiceGrpc.RaftServiceStub;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;
import io.microraft.RaftEndpoint;
import io.microraft.model.message.RaftMessage;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import javax.annotation.Nonnull;
import javax.annotation.PreDestroy;
import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Singleton
public class RaftRpcServiceImpl implements RaftRpcService {

    private static final Logger LOGGER = LoggerFactory.getLogger(RaftRpcService.class);

    private final RaftEndpoint localEndpoint;
    private final Map<RaftEndpoint, String> addresses;
    private final Map<RaftEndpoint, RaftRpcContext> stubs = new ConcurrentHashMap<>();
    private final Set<RaftEndpoint> initializingEndpoints = Collections.newSetFromMap(new ConcurrentHashMap<>());
    private final ThreadGroup threadGroup = new ThreadGroup("RaftRpc");
    private final ScheduledExecutorService executor = newSingleThreadScheduledExecutor(
            r -> new Thread(threadGroup, r, "RaftRpc-" + nextThreadId()));
    private final ProcessTerminationLogger processTerminationLogger;
    private final long rpcTimeoutSecs;

    @Inject
    public RaftRpcServiceImpl(@Named(LOCAL_ENDPOINT_KEY) RaftEndpoint localEndpoint,
            @Named(CONFIG_KEY) AfloatDBConfig config,
            @Named(RAFT_ENDPOINT_ADDRESSES_KEY) Map<RaftEndpoint, String> addresses,
            ProcessTerminationLogger processTerminationLogger) {
        this.localEndpoint = localEndpoint;
        this.addresses = new ConcurrentHashMap<>(addresses);
        this.processTerminationLogger = processTerminationLogger;
        this.rpcTimeoutSecs = config.getRpcConfig().getRpcTimeoutSecs();
    }

    @PreDestroy
    public void shutdown() {
        stubs.values().forEach(RaftRpcContext::shutdownSilently);
        stubs.clear();
        executor.shutdownNow();

        processTerminationLogger.logInfo(LOGGER, localEndpoint.getId() + " RaftMessageDispatcher is shut down.");
    }

    @Override
    public void addAddress(@Nonnull RaftEndpoint endpoint, @Nonnull String address) {
        requireNonNull(endpoint);
        requireNonNull(address);

        String currentAddress = addresses.put(endpoint, address);
        if (currentAddress == null) {
            LOGGER.info("{} added address: {} for {}", localEndpoint.getId(), address, endpoint.getId());
        } else if (!currentAddress.equals(address)) {
            LOGGER.warn("{} replaced current address: {} with new address: {} for {}", localEndpoint.getId(),
                    currentAddress, address, endpoint.getId());
        }
    }

    @Override
    public Map<RaftEndpoint, String> getAddresses() {
        return new HashMap<>(addresses);
    }

    @Override
    public void send(@Nonnull RaftEndpoint target, @Nonnull RaftMessage message) {
        RaftRpcContext stub = getOrCreateStub(requireNonNull(target));
        if (stub != null) {
            stub.send(message);
        }
    }

    @Override
    public boolean isReachable(@Nonnull RaftEndpoint endpoint) {
        return stubs.containsKey(endpoint);
    }

    private RaftRpcContext getOrCreateStub(RaftEndpoint target) {
        if (localEndpoint.equals(target)) {
            LOGGER.error("{} cannot send Raft message to itself...", localEndpoint.getId());
            return null;
        }

        RaftRpcContext context = stubs.get(target);
        if (context != null) {
            return context;
        } else if (!addresses.containsKey(target)) {
            LOGGER.error("{} unknown target: {}", localEndpoint.getId(), target);
            return null;
        }

        return connect(target);
    }

    private RaftRpcContext connect(RaftEndpoint target) {
        if (!initializingEndpoints.add(target)) {
            return null;
        }

        try {
            String address = addresses.get(target);
            Executor channelExecutor = newSingleThreadScheduledExecutor(
                    r -> new Thread(threadGroup, r, "RaftRpc-" + nextThreadId()));
            // https://qr.ae/pv0Vm7
            // https://github.com/MicroRaft/MicroRaft/issues/29
            ManagedChannel channel = ManagedChannelBuilder.forTarget(address).disableRetry().usePlaintext()
                    .executor(channelExecutor).build();
            RaftServiceStub replicationStub = RaftServiceGrpc.newStub(channel).withDeadlineAfter(rpcTimeoutSecs,
                    SECONDS);
            RaftRpcContext context = new RaftRpcContext(target, channel, channelExecutor);
            context.raftMessageSender = replicationStub.handle(new ResponseStreamObserver(context));

            stubs.put(target, context);

            LOGGER.info("{} initialized stub for {}", localEndpoint, target);

            return context;
        } finally {
            initializingEndpoints.remove(target);
        }
    }

    private void checkChannel(RaftRpcContext context) {
        if (stubs.remove(context.targetEndpoint, context)) {
            context.shutdownSilently();
        }

        delayChannelCreation(context.targetEndpoint);
    }

    private void delayChannelCreation(RaftEndpoint target) {
        if (initializingEndpoints.add(target)) {
            LOGGER.debug("{} delaying channel creation to {}.", localEndpoint.getId(), target.getId());
            try {
                executor.schedule(() -> {
                    initializingEndpoints.remove(target);
                }, 1, SECONDS);
            } catch (RejectedExecutionException e) {
                LOGGER.warn("{} could not schedule task for channel creation to: {}.", localEndpoint.getId(),
                        target.getId());
                initializingEndpoints.remove(target);
            }
        }
    }

    private class RaftRpcContext {

        final RaftEndpoint targetEndpoint;
        final ManagedChannel channel;
        final Executor executor;
        StreamObserver<RaftRequest> raftMessageSender;

        RaftRpcContext(RaftEndpoint targetEndpoint, ManagedChannel channel, Executor executor) {
            this.targetEndpoint = targetEndpoint;
            this.channel = channel;
            this.executor = executor;
        }

        void shutdownSilently() {
            runSilently(raftMessageSender::onCompleted);
            runSilently(channel::shutdown);
        }

        public void send(@Nonnull RaftMessage message) {
            executor.execute(() -> {
                try {
                    raftMessageSender.onNext(wrap(message));
                } catch (Throwable t) {
                    if (LOGGER.isDebugEnabled()) {
                        LOGGER.error(localEndpoint.getId() + " failure during sending "
                                + message.getClass().getSimpleName() + " to " + targetEndpoint, t);
                    } else {
                        LOGGER.error("{} failure during sending {} to {}. Exception: {} Message: {}",
                                localEndpoint.getId(), message.getClass().getSimpleName(), targetEndpoint,
                                t.getClass().getSimpleName(), t.getMessage());
                    }
                }
            });
        }
    }

    private class ResponseStreamObserver implements StreamObserver<RaftResponse> {

        final RaftRpcContext context;

        private ResponseStreamObserver(RaftRpcContext context) {
            this.context = context;
        }

        @Override
        public void onNext(RaftResponse response) {
            LOGGER.warn("{} received {} from Raft RPC stream to {}", localEndpoint.getId(), response,
                    context.targetEndpoint.getId());
        }

        @Override
        public void onError(Throwable t) {
            if (LOGGER.isDebugEnabled()) {
                LOGGER.error(localEndpoint.getId() + " streaming Raft RPC to " + context.targetEndpoint.getId()
                        + " has failed.", t);
            } else {
                LOGGER.error("{} Raft RPC stream to {} has failed. Exception: {} Message: {}", localEndpoint.getId(),
                        context.targetEndpoint.getId(), t.getClass().getSimpleName(), t.getMessage());
            }

            checkChannel(context);
        }

        @Override
        public void onCompleted() {
            LOGGER.warn("{} Raft RPC stream to {} has completed.", localEndpoint.getId(),
                    context.targetEndpoint.getId());
        }
    }
}
