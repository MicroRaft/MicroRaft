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

import io.afloatdb.kv.proto.Val;
import io.afloatdb.raft.proto.KVEntry;
import io.afloatdb.raft.proto.KVSnapshotChunkData;
import io.afloatdb.raft.proto.StartNewTermOpProto;
import io.afloatdb.raft.proto.PutOp;
import io.afloatdb.raft.proto.PutResult;
import io.afloatdb.raft.proto.GetOp;
import io.afloatdb.raft.proto.GetResult;
import io.afloatdb.raft.proto.RemoveOp;
import io.afloatdb.raft.proto.RemoveResult;
import io.afloatdb.raft.proto.ReplaceOp;
import io.afloatdb.raft.proto.StartNewTermOpProto;
import io.afloatdb.raft.proto.ReplaceResult;
import io.afloatdb.raft.proto.SizeOp;
import io.afloatdb.raft.proto.SizeResult;
import io.afloatdb.raft.proto.ClearOp;
import io.afloatdb.raft.proto.ClearResult;

import io.microraft.RaftEndpoint;
import io.microraft.statemachine.StateMachine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Singleton;

import static java.util.Objects.requireNonNull;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.function.Consumer;

import static io.afloatdb.internal.di.AfloatDBModule.LOCAL_ENDPOINT_KEY;

/**
 * State machine implementation of KV.proto
 */
@Singleton
public class KVStoreStateMachine implements StateMachine {

    private static final Logger LOGGER = LoggerFactory.getLogger(KVStoreStateMachine.class);

    // we need to keep the insertion order to create snapshot chunks
    // in a deterministic way on all servers.
    private final Map<String, Val> map = new LinkedHashMap<>();

    private final RaftEndpoint localMember;

    @Inject
    public KVStoreStateMachine(@Named(LOCAL_ENDPOINT_KEY) RaftEndpoint localMember) {
        this.localMember = localMember;
    }

    @Override
    public Object runOperation(long commitIndex, @Nonnull Object operation) {
        requireNonNull(operation);
        if (operation instanceof PutOp) {
            return put(commitIndex, (PutOp) operation);
        } else if (operation instanceof GetOp) {
            return get(commitIndex, (GetOp) operation);
        } else if (operation instanceof RemoveOp) {
            return remove(commitIndex, (RemoveOp) operation);
        } else if (operation instanceof ReplaceOp) {
            return replace(commitIndex, (ReplaceOp) operation);
        } else if (operation instanceof SizeOp) {
            return size(commitIndex);
        } else if (operation instanceof ClearOp) {
            return clear(commitIndex);
        } else if (operation instanceof StartNewTermOpProto) {
            return null;
        }

        throw new IllegalArgumentException("Invalid operation: " + operation + " of clazz: " + operation.getClass()
                + " at commit index: " + commitIndex);
    }

    private PutResult put(long commitIndex, PutOp op) {
        Val oldVal = op.getPutIfAbsent()
                ? map.putIfAbsent(op.getKey(), op.getVal())
                : map.put(op.getKey(), op.getVal());
        PutResult.Builder builder = PutResult.newBuilder();
        if (oldVal != null) {
            builder.setOldVal(oldVal);
        }

        return builder.build();
    }

    private GetResult get(long commitIndex, GetOp op) {
        GetResult.Builder builder = GetResult.newBuilder();
        Val val = map.get(op.getKey());
        if (val != null) {
            builder.setVal(val);
        }

        return builder.build();
    }

    private RemoveResult remove(long commitIndex, RemoveOp op) {
        RemoveResult.Builder builder = RemoveResult.newBuilder();
        boolean success = false;
        if (op.hasVal()) {
            success = map.remove(op.getKey(), op.getVal());
        } else {
            Val val = map.remove(op.getKey());
            if (val != null) {
                builder.setOldVal(val);
                success = true;
            }
        }

        return builder.setSuccess(success).build();
    }

    private ReplaceResult replace(long commitIndex, ReplaceOp op) {
        boolean success = map.replace(op.getKey(), op.getOldVal(), op.getNewVal());

        return ReplaceResult.newBuilder().setSuccess(success).build();
    }

    private SizeResult size(long commitIndex) {
        int size = map.size();

        return SizeResult.newBuilder().setSize(size).build();
    }

    private ClearResult clear(long commitIndex) {
        int size = map.size();
        map.clear();

        return ClearResult.newBuilder().setSize(size).build();
    }

    @Override
    public void takeSnapshot(long commitIndex, Consumer<Object> snapshotChunkConsumer) {
        KVSnapshotChunkData.Builder chunkBuilder = KVSnapshotChunkData.newBuilder();

        int chunkCount = 0, keyCount = 0;
        for (Entry<String, Val> e : map.entrySet()) {
            keyCount++;
            KVEntry kvEntry = KVEntry.newBuilder().setKey(e.getKey()).setVal(e.getValue()).build();
            chunkBuilder.addEntry(kvEntry);
            if (chunkBuilder.getEntryCount() == 10000) {
                snapshotChunkConsumer.accept(chunkBuilder.build());
                chunkBuilder = KVSnapshotChunkData.newBuilder();
                chunkCount++;
            }
        }

        if (map.size() == 0 || chunkBuilder.getEntryCount() > 0) {
            snapshotChunkConsumer.accept(chunkBuilder.build());
            chunkCount++;
        }

        LOGGER.info("{} took snapshot with {} chunks and {} keys at log index: {}", localMember.getId(), chunkCount,
                keyCount, commitIndex);
    }

    @Override
    public void installSnapshot(long commitIndex, @Nonnull List<Object> snapshotChunks) {
        map.clear();

        for (Object chunk : snapshotChunks) {
            for (KVEntry entry : ((KVSnapshotChunkData) chunk).getEntryList()) {
                map.put(entry.getKey(), entry.getVal());
            }
        }

        LOGGER.info("{} restored snapshot with {} keys at commit index: {}", localMember.getId(), map.size(),
                commitIndex);
    }

    @Nonnull
    @Override
    public Object getNewTermOperation() {
        return StartNewTermOpProto.getDefaultInstance();
    }

}
