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

package io.microraft.afloatdb.internal.raft.impl.model;

import static io.microraft.afloatdb.internal.di.AfloatDBModule.LOCAL_ENDPOINT_KEY;
import static java.util.Objects.requireNonNull;

import com.google.protobuf.InvalidProtocolBufferException;
import io.microraft.afloatdb.internal.raft.impl.model.log.LogEntryOrBuilder;
import io.microraft.afloatdb.internal.raft.impl.model.log.RaftGroupMembersViewOrBuilder;
import io.microraft.afloatdb.internal.raft.impl.model.log.SnapshotChunkOrBuilder;
import io.microraft.afloatdb.raft.proto.LogEntryProto;
import io.microraft.afloatdb.raft.proto.RaftEndpointProto;
import io.microraft.afloatdb.raft.proto.RaftGroupMembersViewProto;
import io.microraft.afloatdb.raft.proto.KVSnapshotChunk;
import io.microraft.RaftEndpoint;
import io.microraft.model.log.LogEntry;
import io.microraft.model.log.RaftGroupMembersView;
import io.microraft.model.log.SnapshotChunk;
import io.microraft.store.sqlite.StoreModelSerializer;
import io.microraft.store.sqlite.StoreModelSerializer.Serializer;
import io.microraft.afloatdb.internal.raft.impl.model.AfloatDBEndpoint;
import javax.annotation.Nonnull;
import javax.inject.Inject;
import javax.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Singleton
public class ProtoStateStoreSerializer implements StoreModelSerializer {

    private static final Logger LOGGER = LoggerFactory.getLogger(ProtoStateStoreSerializer.class);

    @Override
    public Serializer<RaftGroupMembersView> raftGroupMembersViewSerializer() {
        return new Serializer<RaftGroupMembersView>() {
            @Nonnull
            @Override
            public byte[] serialize(@Nonnull RaftGroupMembersView element) {
                if (element instanceof RaftGroupMembersViewOrBuilder) {
                    RaftGroupMembersViewProto proto = ((RaftGroupMembersViewOrBuilder) element).getGroupMembersView();
                    return proto.toByteArray();
                }
                throw new IllegalArgumentException("Cannot serialize: " + element.getClass());
            }

            @Nonnull
            @Override
            public RaftGroupMembersView deserialize(@Nonnull byte[] element) {
                try {
                    return new RaftGroupMembersViewOrBuilder(RaftGroupMembersViewProto.parseFrom(element));
                } catch (InvalidProtocolBufferException e) {
                    throw new IllegalArgumentException(
                            "Cannot deserialize byte array to RaftGroupMembersViewProto: " + e);
                }
            }
        };
    }

    @Override
    public Serializer<RaftEndpoint> raftEndpointSerializer() {
        return new Serializer<RaftEndpoint>() {
            @Nonnull
            @Override
            public byte[] serialize(@Nonnull RaftEndpoint element) {
                RaftEndpointProto proto = AfloatDBEndpoint.unwrap(element);
                if (proto != null) {
                    return proto.toByteArray();
                }
                throw new IllegalArgumentException("Cannot serialize: " + element.getClass());
            }

            @Nonnull
            @Override
            public RaftEndpoint deserialize(@Nonnull byte[] element) {
                try {
                    return AfloatDBEndpoint.wrap(RaftEndpointProto.parseFrom(element));
                } catch (InvalidProtocolBufferException e) {
                    throw new IllegalArgumentException("Cannot deserialize byte array to RaftEndpointProto: " + e);
                }
            }
        };
    }

    @Override
    public Serializer<LogEntry> logEntrySerializer() {
        return new Serializer<LogEntry>() {
            @Nonnull
            @Override
            public byte[] serialize(@Nonnull LogEntry element) {
                if (element instanceof LogEntryOrBuilder) {
                    LogEntryProto proto = ((LogEntryOrBuilder) element).getEntry();
                    return proto.toByteArray();
                }
                throw new IllegalArgumentException("Cannot serialize: " + element.getClass());
            }

            @Nonnull
            @Override
            public LogEntry deserialize(@Nonnull byte[] element) {
                try {
                    return new LogEntryOrBuilder(LogEntryProto.parseFrom(element));
                } catch (InvalidProtocolBufferException e) {
                    throw new IllegalArgumentException("Cannot deserialize byte array to LogEntryProto: " + e);
                }
            }
        };
    }

    @Override
    public Serializer<SnapshotChunk> snapshotChunkSerializer() {
        return new Serializer<SnapshotChunk>() {
            @Nonnull
            @Override
            public byte[] serialize(@Nonnull SnapshotChunk element) {
                if (element instanceof SnapshotChunkOrBuilder) {
                    KVSnapshotChunk proto = ((SnapshotChunkOrBuilder) element).getSnapshotChunk();
                    return proto.toByteArray();
                }
                throw new IllegalArgumentException("Cannot serialize: " + element.getClass());
            }

            @Nonnull
            @Override
            public SnapshotChunk deserialize(@Nonnull byte[] element) {
                try {
                    return new SnapshotChunkOrBuilder(KVSnapshotChunk.parseFrom(element));
                } catch (InvalidProtocolBufferException e) {
                    throw new IllegalArgumentException("Cannot deserialize byte array to KVSnapshotChunk: " + e);
                }
            }
        };
    }
}
