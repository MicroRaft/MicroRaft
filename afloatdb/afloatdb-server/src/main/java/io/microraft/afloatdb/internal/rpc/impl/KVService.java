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

package io.microraft.afloatdb.internal.rpc.impl;

import io.microraft.afloatdb.kv.proto.ClearRequest;
import io.microraft.afloatdb.kv.proto.ContainsRequest;
import io.microraft.afloatdb.kv.proto.DeleteRequest;
import io.microraft.afloatdb.kv.proto.GetRequest;
import io.microraft.afloatdb.kv.proto.KVServiceGrpc.KVServiceImplBase;
import io.microraft.afloatdb.kv.proto.KVResponse;
import io.microraft.afloatdb.kv.proto.PutRequest;
import io.microraft.afloatdb.kv.proto.RemoveRequest;
import io.microraft.afloatdb.kv.proto.ReplaceRequest;
import io.microraft.afloatdb.kv.proto.SetRequest;
import io.microraft.afloatdb.kv.proto.SizeRequest;
import io.microraft.afloatdb.raft.proto.SizeOp;
import io.microraft.afloatdb.raft.proto.PutOp;
import io.microraft.afloatdb.raft.proto.GetOp;
import io.microraft.afloatdb.raft.proto.RemoveOp;
import io.microraft.afloatdb.raft.proto.ReplaceOp;
import io.microraft.afloatdb.raft.proto.ClearOp;
import io.microraft.Ordered;
import io.grpc.stub.StreamObserver;
import io.microraft.RaftNode;

import static io.microraft.afloatdb.internal.di.AfloatDBModule.RAFT_NODE_SUPPLIER_KEY;

import javax.inject.Inject;
import javax.inject.Singleton;

import javax.inject.Named;
import java.util.function.Supplier;

import static io.microraft.afloatdb.internal.utils.Exceptions.wrap;
import static io.microraft.QueryPolicy.EVENTUAL_CONSISTENCY;
import static io.microraft.QueryPolicy.LINEARIZABLE;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Singleton
public class KVService extends KVServiceImplBase {

    private static final Logger LOGGER = LoggerFactory.getLogger(KVService.class);

    private final RaftNode raftNode;

    @Inject
    public KVService(@Named(RAFT_NODE_SUPPLIER_KEY) Supplier<RaftNode> raftNodeSupplier) {
        this.raftNode = raftNodeSupplier.get();
    }

    @Override
    public void put(PutRequest request, StreamObserver<KVResponse> responseObserver) {
        // TODO (basri) build the proxy logic here (invocation logic)
        PutOp op = PutOp.newBuilder().setKey(request.getKey()).setVal(request.getVal())
                .setPutIfAbsent(request.getPutIfAbsent()).build();
        raftNode.<io.microraft.afloatdb.raft.proto.PutResult>replicate(op)
                .whenComplete((Ordered<io.microraft.afloatdb.raft.proto.PutResult> result, Throwable throwable) -> {
                    // TODO [basri] bottleneck. offload to IO thread...
                    if (throwable == null) {
                        responseObserver.onNext(KVResponse.newBuilder().setCommitIndex(result.getCommitIndex())
                                .setPutResult(io.microraft.afloatdb.kv.proto.PutResult.newBuilder()
                                        .setOldVal(result.getResult().getOldVal()).build())
                                .build());
                    } else {
                        responseObserver.onError(wrap(throwable));
                    }
                    responseObserver.onCompleted();
                });
    }

    @Override
    public void set(SetRequest request, StreamObserver<KVResponse> responseObserver) {
        PutOp op = PutOp.newBuilder().setKey(request.getKey()).setVal(request.getVal()).build();
        raftNode.<io.microraft.afloatdb.raft.proto.PutResult>replicate(op)
                .whenComplete((Ordered<io.microraft.afloatdb.raft.proto.PutResult> result, Throwable throwable) -> {
                    // TODO [basri] bottleneck. offload to IO thread...
                    if (throwable == null) {
                        responseObserver.onNext(KVResponse.newBuilder().setCommitIndex(result.getCommitIndex())
                                .setSetResult(io.microraft.afloatdb.kv.proto.SetResult.newBuilder()
                                        .setOldValExisted(result.getResult().hasOldVal()).build())
                                .build());
                    } else {
                        responseObserver.onError(wrap(throwable));
                    }
                    responseObserver.onCompleted();
                });
    }

    @Override
    public void get(GetRequest request, StreamObserver<KVResponse> responseObserver) {
        GetOp op = GetOp.newBuilder().setKey(request.getKey()).build();
        raftNode.<io.microraft.afloatdb.raft.proto.GetResult>query(op,
                request.getMinCommitIndex() == -1 ? LINEARIZABLE : EVENTUAL_CONSISTENCY,
                Math.max(0, request.getMinCommitIndex()))
                .whenComplete((Ordered<io.microraft.afloatdb.raft.proto.GetResult> result, Throwable throwable) -> {
                    // TODO [basri] bottleneck. offload to IO thread...
                    if (throwable == null) {
                        responseObserver.onNext(KVResponse.newBuilder().setCommitIndex(result.getCommitIndex())
                                .setGetResult(io.microraft.afloatdb.kv.proto.GetResult.newBuilder()
                                        .setVal(result.getResult().getVal()))
                                .build());
                    } else {
                        responseObserver.onError(wrap(throwable));
                    }
                    responseObserver.onCompleted();
                });
    }

    @Override
    public void contains(ContainsRequest request, StreamObserver<KVResponse> responseObserver) {
        GetOp op = GetOp.newBuilder().setKey(request.getKey()).build();
        raftNode.<io.microraft.afloatdb.raft.proto.GetResult>query(op,
                request.getMinCommitIndex() == -1 ? LINEARIZABLE : EVENTUAL_CONSISTENCY,
                Math.max(0, request.getMinCommitIndex()))
                .whenComplete((Ordered<io.microraft.afloatdb.raft.proto.GetResult> result, Throwable throwable) -> {
                    // TODO [basri] bottleneck. offload to IO thread...
                    if (throwable == null) {
                        io.microraft.afloatdb.kv.proto.ContainsResult.Builder builder2 = io.microraft.afloatdb.kv.proto.ContainsResult
                                .newBuilder();
                        if (!result.getResult().hasVal()) {
                            builder2.setSuccess(false);
                        } else if (request.hasVal()) {
                            builder2.setSuccess(request.getVal().equals(result.getResult().getVal()));
                        } else {
                            builder2.setSuccess(true);
                        }
                        responseObserver.onNext(KVResponse.newBuilder().setCommitIndex(result.getCommitIndex())
                                .setContainsResult(builder2.build()).build());
                    } else {
                        responseObserver.onError(wrap(throwable));
                    }
                    responseObserver.onCompleted();
                });
    }

    @Override
    public void delete(DeleteRequest request, StreamObserver<KVResponse> responseObserver) {
        RemoveOp op = RemoveOp.newBuilder().setKey(request.getKey()).build();
        raftNode.<io.microraft.afloatdb.raft.proto.RemoveResult>replicate(op)
                .whenComplete((Ordered<io.microraft.afloatdb.raft.proto.RemoveResult> result, Throwable throwable) -> {
                    // TODO [basri] bottleneck. offload to IO thread...
                    if (throwable == null) {
                        responseObserver.onNext(KVResponse.newBuilder().setCommitIndex(result.getCommitIndex())
                                .setDeleteResult(io.microraft.afloatdb.kv.proto.DeleteResult.newBuilder()
                                        .setSuccess(result.getResult().getSuccess()).build())
                                .build());
                    } else {
                        responseObserver.onError(wrap(throwable));
                    }
                    responseObserver.onCompleted();
                });
    }

    @Override
    public void remove(RemoveRequest request, StreamObserver<KVResponse> responseObserver) {
        RemoveOp.Builder builder = RemoveOp.newBuilder().setKey(request.getKey());
        if (request.hasVal()) {
            builder.setVal(request.getVal());
        }
        raftNode.<io.microraft.afloatdb.raft.proto.RemoveResult>replicate(builder.build())
                .whenComplete((Ordered<io.microraft.afloatdb.raft.proto.RemoveResult> result, Throwable throwable) -> {
                    // TODO [basri] bottleneck. offload to IO thread...
                    if (throwable == null) {
                        io.microraft.afloatdb.kv.proto.RemoveResult.Builder builder2 = io.microraft.afloatdb.kv.proto.RemoveResult
                                .newBuilder().setSuccess(result.getResult().getSuccess());
                        if (!request.hasVal() && result.getResult().hasOldVal()) {
                            builder2.setOldVal(result.getResult().getOldVal());
                        }
                        responseObserver.onNext(KVResponse.newBuilder().setCommitIndex(result.getCommitIndex())
                                .setRemoveResult(builder2.build()).build());
                    } else {
                        responseObserver.onError(wrap(throwable));
                    }
                    responseObserver.onCompleted();
                });
    }

    @Override
    public void replace(ReplaceRequest request, StreamObserver<KVResponse> responseObserver) {
        ReplaceOp op = ReplaceOp.newBuilder().setKey(request.getKey()).setOldVal(request.getOldVal())
                .setNewVal(request.getNewVal()).build();
        raftNode.<io.microraft.afloatdb.raft.proto.ReplaceResult>replicate(op)
                .whenComplete((Ordered<io.microraft.afloatdb.raft.proto.ReplaceResult> result, Throwable throwable) -> {
                    // TODO [basri] bottleneck. offload to IO thread...
                    if (throwable == null) {
                        responseObserver.onNext(KVResponse.newBuilder().setCommitIndex(result.getCommitIndex())
                                .setReplaceResult(io.microraft.afloatdb.kv.proto.ReplaceResult.newBuilder()
                                        .setSuccess(result.getResult().getSuccess()).build())
                                .build());
                    } else {
                        responseObserver.onError(wrap(throwable));
                    }
                    responseObserver.onCompleted();
                });
    }

    @Override
    public void size(SizeRequest request, StreamObserver<KVResponse> responseObserver) {
        raftNode.<io.microraft.afloatdb.raft.proto.SizeResult>query(SizeOp.getDefaultInstance(),
                request.getMinCommitIndex() == -1 ? LINEARIZABLE : EVENTUAL_CONSISTENCY,
                Math.max(0, request.getMinCommitIndex()))
                .whenComplete((Ordered<io.microraft.afloatdb.raft.proto.SizeResult> result, Throwable throwable) -> {
                    // TODO [basri] bottleneck. offload to IO thread...
                    if (throwable == null) {
                        responseObserver.onNext(KVResponse.newBuilder().setCommitIndex(result.getCommitIndex())
                                .setSizeResult(io.microraft.afloatdb.kv.proto.SizeResult.newBuilder()
                                        .setSize(result.getResult().getSize()))
                                .build());
                    } else {
                        responseObserver.onError(wrap(throwable));
                    }
                    responseObserver.onCompleted();
                });
    }

    @Override
    public void clear(ClearRequest request, StreamObserver<KVResponse> responseObserver) {
        raftNode.<io.microraft.afloatdb.raft.proto.ClearResult>replicate(ClearOp.getDefaultInstance())
                .whenComplete((Ordered<io.microraft.afloatdb.raft.proto.ClearResult> result, Throwable throwable) -> {
                    // TODO [basri] bottleneck. offload to IO thread...
                    if (throwable == null) {
                        responseObserver.onNext(KVResponse.newBuilder().setCommitIndex(result.getCommitIndex())
                                .setClearResult(io.microraft.afloatdb.kv.proto.ClearResult.newBuilder()
                                        .setSize(result.getResult().getSize()).build())
                                .build());
                    } else {
                        responseObserver.onError(wrap(throwable));
                    }
                    responseObserver.onCompleted();
                });
    }

}
