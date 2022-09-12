package io.afloatdb.client.internal.rpc;

import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

import io.afloatdb.kv.proto.KVRequestHandlerGrpc.KVRequestHandlerFutureStub;
import com.google.common.util.concurrent.ListenableFuture;
import io.afloatdb.kv.proto.KVResponse;

public interface InvocationService {

    CompletableFuture<KVResponse> invoke(Function<KVRequestHandlerFutureStub, ListenableFuture<KVResponse>> func);

}