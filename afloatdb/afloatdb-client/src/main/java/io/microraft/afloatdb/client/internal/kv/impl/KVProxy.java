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

package io.microraft.afloatdb.client.internal.kv.impl;

import io.microraft.afloatdb.client.kv.KV;
import io.microraft.afloatdb.client.kv.Ordered;
import io.microraft.afloatdb.kv.proto.Val;
import io.microraft.afloatdb.kv.proto.ClearRequest;
import io.microraft.afloatdb.kv.proto.ContainsRequest;
import io.microraft.afloatdb.kv.proto.DeleteRequest;
import io.microraft.afloatdb.kv.proto.GetRequest;
import io.microraft.afloatdb.kv.proto.GetResult;
import io.microraft.afloatdb.kv.proto.KVServiceGrpc.KVServiceFutureStub;
import io.microraft.afloatdb.kv.proto.KVResponse;
import io.microraft.afloatdb.kv.proto.PutRequest;
import io.microraft.afloatdb.kv.proto.PutResult;
import io.microraft.afloatdb.kv.proto.RemoveRequest;
import io.microraft.afloatdb.kv.proto.RemoveResult;
import io.microraft.afloatdb.kv.proto.ReplaceRequest;
import io.microraft.afloatdb.kv.proto.SetRequest;
import io.microraft.afloatdb.kv.proto.SizeRequest;
import com.google.protobuf.ByteString;

import com.google.common.util.concurrent.ListenableFuture;

import javax.annotation.Nonnull;

import java.util.function.Function;

import static java.util.Objects.requireNonNull;

import io.microraft.afloatdb.client.internal.rpc.InvocationService;

public class KVProxy implements KV {

    private Object extract(@Nonnull Val val) {
        switch (val.getValCase()) {
            case VAL_NOT_SET :
                return null;
            case STR :
                return val.getStr();
            case NUM :
                return val.getNum();
            case BYTEARRAY :
                return val.getByteArray().toByteArray();
            default :
                throw new IllegalArgumentException("Invalid val: " + val);
        }
    }

    private Val toVal(Object o) {
        requireNonNull(o);
        if (o instanceof String) {
            return Val.newBuilder().setStr((String) o).build();
        } else if (o instanceof Integer || o instanceof Long) {
            return Val.newBuilder().setNum((long) o).build();
        } else if (o instanceof byte[]) {
            return Val.newBuilder().setByteArray(ByteString.copyFrom((byte[]) o)).build();
        }
        throw new IllegalArgumentException("Invalid type for val: " + o);
    }

    private final InvocationService invocationService;

    public KVProxy(@Nonnull InvocationService invocationService) {
        this.invocationService = requireNonNull(invocationService);
    }

    @Nonnull
    @Override
    public Ordered<byte[]> put(@Nonnull String key, @Nonnull byte[] value) {
        return putOrdered(key, Val.newBuilder().setByteArray(ByteString.copyFrom(value)).build(), false);
    }

    @Nonnull
    @Override
    public Ordered<Long> put(@Nonnull String key, long value) {
        return putOrdered(key, Val.newBuilder().setNum(value).build(), false);
    }

    @Nonnull
    @Override
    public Ordered<String> put(@Nonnull String key, @Nonnull String value) {
        return putOrdered(key, Val.newBuilder().setStr(value).build(), false);
    }

    @Nonnull
    @Override
    public Ordered<byte[]> putIfAbsent(@Nonnull String key, @Nonnull byte[] value) {
        return putOrdered(key, Val.newBuilder().setByteArray(ByteString.copyFrom(value)).build(), true);
    }

    @Nonnull
    @Override
    public Ordered<Long> putIfAbsent(@Nonnull String key, long value) {
        return putOrdered(key, Val.newBuilder().setNum(value).build(), true);
    }

    @Nonnull
    @Override
    public Ordered<String> putIfAbsent(@Nonnull String key, @Nonnull String value) {
        return putOrdered(key, Val.newBuilder().setStr(value).build(), true);
    }

    private <T> Ordered<T> putOrdered(String key, @Nonnull Val val, boolean absent) {
        KVResponse response = invocationService.invoke((KVServiceFutureStub stub) -> {
            PutRequest request = PutRequest.newBuilder().setKey(requireNonNull(key)).setVal(requireNonNull(val))
                    .setPutIfAbsent(absent).build();
            return stub.put(request);
        }).join();
        PutResult result = response.getPutResult();
        return new OrderedImpl<>(response.getCommitIndex(), (T) extract(result.getOldVal()));
    }

    @Override
    public Ordered<Void> set(@Nonnull String key, @Nonnull byte[] value) {
        return set(key, Val.newBuilder().setByteArray(ByteString.copyFrom(value)).build());
    }

    @Override
    public Ordered<Void> set(@Nonnull String key, long value) {
        return set(key, Val.newBuilder().setNum(value).build());
    }

    @Override
    public Ordered<Void> set(@Nonnull String key, @Nonnull String value) {
        return set(key, Val.newBuilder().setStr(value).build());
    }

    private Ordered<Void> set(@Nonnull String key, @Nonnull Val val) {
        KVResponse response = invocationService.invoke((stub) -> {
            SetRequest request = SetRequest.newBuilder().setKey(requireNonNull(key)).setVal(requireNonNull(val))
                    .build();
            return stub.set(request);
        }).join();
        return new OrderedImpl<>(response.getCommitIndex(), null);
    }

    @Nonnull
    @Override
    public <T> Ordered<T> get(@Nonnull String key, long minCommitIndex) {
        KVResponse response = invocationService.invoke((stub) -> {
            GetRequest request = GetRequest.newBuilder().setKey(requireNonNull(key)).setMinCommitIndex(minCommitIndex)
                    .build();
            return stub.get(request);
        }).join();
        GetResult result = response.getGetResult();
        return new OrderedImpl<>(response.getCommitIndex(), (T) extract(result.getVal()));
    }

    @Nonnull
    @Override
    public Ordered<Boolean> containsKey(@Nonnull String key, long minCommitIndex) {
        return contains(key, (Val) null, minCommitIndex);
    }

    @Nonnull
    @Override
    public Ordered<Boolean> contains(@Nonnull String key, @Nonnull byte[] value, long minCommitIndex) {
        return contains(key, Val.newBuilder().setByteArray(ByteString.copyFrom(value)).build(), minCommitIndex);
    }

    @Nonnull
    @Override
    public Ordered<Boolean> contains(@Nonnull String key, long value, long minCommitIndex) {
        return contains(key, Val.newBuilder().setNum(value).build(), minCommitIndex);
    }

    @Nonnull
    @Override
    public Ordered<Boolean> contains(@Nonnull String key, @Nonnull String value, long minCommitIndex) {
        return contains(key, Val.newBuilder().setStr(value).build(), minCommitIndex);
    }

    private Ordered<Boolean> contains(@Nonnull String key, Val val, long minCommitIndex) {
        KVResponse response = invocationService.invoke((stub) -> {
            ContainsRequest.Builder builder = ContainsRequest.newBuilder().setKey(requireNonNull(key))
                    .setMinCommitIndex(minCommitIndex);
            if (val != null) {
                builder.setVal(val);
            }
            return stub.contains(builder.build());
        }).join();
        return new OrderedImpl<>(response.getCommitIndex(), response.getContainsResult().getSuccess());
    }

    @Nonnull
    @Override
    public Ordered<Boolean> delete(@Nonnull String key) {
        KVResponse response = invocationService.invoke((stub) -> {
            DeleteRequest request = DeleteRequest.newBuilder().setKey(requireNonNull(key)).build();
            return stub.delete(request);
        }).join();
        return new OrderedImpl<>(response.getCommitIndex(), response.getDeleteResult().getSuccess());
    }

    @Nonnull
    @Override
    public <T> Ordered<T> remove(@Nonnull String key) {
        KVResponse response = invocationService.invoke((stub) -> {
            RemoveRequest request = RemoveRequest.newBuilder().setKey(requireNonNull(key)).build();
            return stub.remove(request);
        }).join();
        RemoveResult result = response.getRemoveResult();
        return new OrderedImpl<>(response.getCommitIndex(), (T) extract(result.getOldVal()));
    }

    @Override
    public Ordered<Boolean> remove(@Nonnull String key, @Nonnull byte[] value) {
        return remove(key, Val.newBuilder().setByteArray(ByteString.copyFrom(value)).build());
    }

    @Nonnull
    @Override
    public Ordered<Boolean> remove(@Nonnull String key, long value) {
        return remove(key, Val.newBuilder().setNum(value).build());
    }

    @Nonnull
    @Override
    public Ordered<Boolean> remove(@Nonnull String key, @Nonnull String value) {
        return remove(key, Val.newBuilder().setStr(value).build());
    }

    private Ordered<Boolean> remove(@Nonnull String key, @Nonnull Val val) {
        KVResponse response = invocationService.invoke((stub) -> {
            RemoveRequest request = RemoveRequest.newBuilder().setKey(requireNonNull(key)).setVal(requireNonNull(val))
                    .build();
            return stub.remove(request);
        }).join();
        return new OrderedImpl<>(response.getCommitIndex(), response.getRemoveResult().getSuccess());
    }

    @Nonnull
    @Override
    public Ordered<Boolean> replace(@Nonnull String key, @Nonnull Object oldValue, @Nonnull Object newValue) {
        KVResponse response = invocationService.invoke((stub) -> {
            ReplaceRequest request = ReplaceRequest.newBuilder().setKey(requireNonNull(key)).setOldVal(toVal(oldValue))
                    .setNewVal(toVal(newValue)).build();
            return stub.replace(request);
        }).join();
        return new OrderedImpl<>(response.getCommitIndex(), response.getReplaceResult().getSuccess());
    }

    @Nonnull
    @Override
    public Ordered<Boolean> isEmpty(long minCommitIndex) {
        KVResponse response = invocationService.invoke((stub) -> {
            SizeRequest request = SizeRequest.newBuilder().setMinCommitIndex(minCommitIndex).build();
            return stub.size(request);
        }).join();
        return new OrderedImpl<>(response.getCommitIndex(), response.getSizeResult().getSize() == 0);
    }

    @Nonnull
    @Override
    public Ordered<Integer> size(long minCommitIndex) {
        KVResponse response = invocationService.invoke((stub) -> {
            SizeRequest request = SizeRequest.newBuilder().setMinCommitIndex(minCommitIndex).build();
            return stub.size(request);
        }).join();
        return new OrderedImpl<>(response.getCommitIndex(), response.getSizeResult().getSize());
    }

    @Nonnull
    @Override
    public Ordered<Integer> clear() {
        KVResponse response = invocationService.invoke((stub) -> {
            return stub.clear(ClearRequest.getDefaultInstance());
        }).join();
        return new OrderedImpl<>(response.getCommitIndex(), response.getClearResult().getSize());
    }

    private static class OrderedImpl<T> implements Ordered<T> {

        private final long commitIndex;
        private final T value;

        public OrderedImpl(long commitIndex, T value) {
            this.commitIndex = commitIndex;
            this.value = value;
        }

        @Override
        public long getCommitIndex() {
            return commitIndex;
        }

        @Override
        public T get() {
            return value;
        }

        @Override
        public String toString() {
            return "Ordered{commitIndex=" + commitIndex + ", value=" + value + "}";
        }

    }
}
