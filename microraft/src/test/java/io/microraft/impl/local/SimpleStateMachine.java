/*
 * Original work Copyright (c) 2008-2020, Hazelcast, Inc.
 * Modified work Copyright 2020, MicroRaft.
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

package io.microraft.impl.local;

import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiFunction;
import java.util.function.Consumer;

/**
 * @author mdogan
 * @author metanet
 */
public class SimpleStateMachine {

    private final Map<Long, Object> values = new ConcurrentHashMap<>();

    SimpleStateMachine() {
    }

    public static BiFunction<SimpleStateMachine, Long, Object> apply(Object val) {
        return new Apply(val);
    }

    public static BiFunction<SimpleStateMachine, Long, Object> query() {
        return new Query();
    }

    private Object apply(long commitIndex, Object value) {
        assert !values.containsKey(commitIndex) : "Cannot apply " + value + "since commitIndex: " + commitIndex
                + " already contains: " + values.get(commitIndex);

        values.put(commitIndex, value);
        return value;
    }

    public Object get(long commitIndex) {
        return values.get(commitIndex);
    }

    public int size() {
        return values.size();
    }

    public Set<Object> values() {
        return new HashSet<>(values.values());
    }

    public Object[] valuesArray() {
        return values.entrySet().stream().sorted(Comparator.comparingLong(Entry::getKey)).map(Entry::getValue).toArray();
    }

    void takeSnapshot(long commitIndex, Consumer<Object> chunkConsumer) {
        Map<Long, Object> chunk = new HashMap<>();
        for (Entry<Long, Object> e : values.entrySet()) {
            assert e.getKey() <= commitIndex : "Key: " + e.getKey() + ", commit-index: " + commitIndex;
            chunk.put(e.getKey(), e.getValue());
            if (chunk.size() == 10) {
                chunkConsumer.accept(chunk);
                chunk = new HashMap<>();
            }
        }

        if (values.size() == 0 || chunk.size() > 0) {
            chunkConsumer.accept(chunk);
        }
    }

    void installSnapshot(long commitIndex, List<Object> chunks) {
        values.clear();
        for (Object chunk : chunks) {
            values.putAll((Map<Long, Object>) chunk);
        }
    }

    private static class Apply
            implements BiFunction<SimpleStateMachine, Long, Object> {

        private Object val;

        Apply(Object val) {
            this.val = val;
        }

        @Override
        public Object apply(SimpleStateMachine stateMachine, Long commitIndex) {
            return stateMachine.apply(commitIndex, val);
        }

        @Override
        public String toString() {
            return "Apply{" + "val=" + val + '}';
        }

    }

    private static class Query
            implements BiFunction<SimpleStateMachine, Long, Object> {

        @Override
        public Object apply(SimpleStateMachine stateMachine, Long commitIndex) {
            return stateMachine.get(commitIndex);
        }

        @Override
        public String toString() {
            return "Query{}";
        }

    }

}
