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

package io.microraft.afloatdb.client.kv;

import javax.annotation.Nonnull;

public interface KV {

    @Nonnull
    Ordered<byte[]> put(@Nonnull String key, @Nonnull byte[] value);

    @Nonnull
    Ordered<Long> put(@Nonnull String key, long value);

    @Nonnull
    Ordered<String> put(@Nonnull String key, @Nonnull String value);

    @Nonnull
    Ordered<byte[]> putIfAbsent(@Nonnull String key, @Nonnull byte[] value);

    @Nonnull
    Ordered<Long> putIfAbsent(@Nonnull String key, long value);

    @Nonnull
    Ordered<String> putIfAbsent(@Nonnull String key, @Nonnull String value);

    Ordered<Void> set(@Nonnull String key, @Nonnull byte[] value);

    Ordered<Void> set(@Nonnull String key, long value);

    Ordered<Void> set(@Nonnull String key, @Nonnull String value);

    @Nonnull
    Ordered<Boolean> replace(@Nonnull String key, @Nonnull Object oldValue, @Nonnull Object newValue);

    @Nonnull
    <T> Ordered<T> remove(@Nonnull String key);

    @Nonnull
    Ordered<Boolean> remove(@Nonnull String key, @Nonnull byte[] value);

    @Nonnull
    Ordered<Boolean> remove(@Nonnull String key, long value);

    @Nonnull
    Ordered<Boolean> remove(@Nonnull String key, @Nonnull String value);

    @Nonnull
    Ordered<Boolean> delete(@Nonnull String key);

    default @Nonnull <T> Ordered<T> get(@Nonnull String key) {
        return get(key, -1L);
    }

    @Nonnull
    <T> Ordered<T> get(@Nonnull String key, long minCommitIndex);

    default @Nonnull Ordered<Boolean> containsKey(@Nonnull String key) {
        return containsKey(key, -1L);
    }

    @Nonnull
    Ordered<Boolean> containsKey(@Nonnull String key, long minCommitIndex);

    default @Nonnull Ordered<Boolean> contains(@Nonnull String key, @Nonnull byte[] value) {
        return contains(key, value, -1L);
    }

    @Nonnull
    Ordered<Boolean> contains(@Nonnull String key, @Nonnull byte[] value, long minCommitIndex);

    default @Nonnull Ordered<Boolean> contains(@Nonnull String key, long value) {
        return contains(key, value, -1L);
    }

    @Nonnull
    Ordered<Boolean> contains(@Nonnull String key, long value, long minCommitIndex);

    default @Nonnull Ordered<Boolean> contains(@Nonnull String key, @Nonnull String value) {
        return contains(key, value, -1L);
    }

    @Nonnull
    Ordered<Boolean> contains(@Nonnull String key, @Nonnull String value, long minCommitIndex);

    default @Nonnull Ordered<Boolean> isEmpty() {
        return isEmpty(-1L);
    }

    @Nonnull
    Ordered<Boolean> isEmpty(long minCommitIndex);

    default @Nonnull Ordered<Integer> size() {
        return size(-1L);
    }

    @Nonnull
    Ordered<Integer> size(long minCommitIndex);

    @Nonnull
    Ordered<Integer> clear();

}
