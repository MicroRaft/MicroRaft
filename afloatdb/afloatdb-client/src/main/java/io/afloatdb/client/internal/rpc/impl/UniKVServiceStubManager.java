package io.afloatdb.client.internal.rpc.impl;

import io.afloatdb.client.AfloatDBClientException;
import io.afloatdb.client.config.AfloatDBClientConfig;
import io.afloatdb.client.internal.channel.ChannelManager;
import io.afloatdb.kv.proto.KVRequestHandlerGrpc;
import io.afloatdb.kv.proto.KVRequestHandlerGrpc.KVRequestHandlerFutureStub;
import io.afloatdb.client.internal.rpc.InvocationService;
import io.grpc.StatusRuntimeException;
import io.grpc.Status;
import javax.annotation.PreDestroy;

import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import com.google.common.util.concurrent.ListenableFuture;
import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Singleton;

import com.google.rpc.Code;

import io.afloatdb.kv.proto.KVResponse;

import java.util.function.Supplier;
import static java.util.concurrent.Executors.newSingleThreadScheduledExecutor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ExecutionException;

import static com.google.common.base.Throwables.getRootCause;
import static io.afloatdb.client.internal.di.AfloatDBClientModule.CONFIG_KEY;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

@Singleton
public class UniKVServiceStubManager implements InvocationService {

    private final int rpcTimeoutSecs;
    private final KVRequestHandlerFutureStub stub;
    private final ScheduledExecutorService executor = newSingleThreadScheduledExecutor();

    @Inject
    public UniKVServiceStubManager(@Named(CONFIG_KEY) AfloatDBClientConfig config, ChannelManager channelManager) {
        this.rpcTimeoutSecs = config.getRpcTimeoutSecs();
        this.stub = KVRequestHandlerGrpc.newFutureStub(channelManager.getOrCreateChannel(config.getServerAddress()))
                .withDeadlineAfter(rpcTimeoutSecs, SECONDS);
    }

    @Override
    public CompletableFuture<KVResponse> invoke(
            Function<KVRequestHandlerFutureStub, ListenableFuture<KVResponse>> func) {
        CompletableFuture<KVResponse> future = new CompletableFuture<>();
        ListenableFuture<KVResponse> rawFuture = func.apply(stub);
        rawFuture.addListener(new Runnable() {
            @Override
            public void run() {
                handleRpcResult(future, rawFuture);
            }
        }, executor);

        return future;
    }

    void handleRpcResult(CompletableFuture<KVResponse> future, ListenableFuture<KVResponse> rawFuture) {
        try {
            future.complete(rawFuture.get());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            future.completeExceptionally(e);
        } catch (ExecutionException e) {
            future.completeExceptionally(new AfloatDBClientException(getRootCause(e)));
        }
    }

    @PreDestroy
    public void shutdown() {
        executor.shutdown();
    }

}
