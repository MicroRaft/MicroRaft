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

import io.microraft.statemachine.StateMachine;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.function.Consumer;

/**
 * A simple {@link StateMachine} implementation used for testing.
 * <p>
 * This state machine implementation just collects all committed values along with their commit indices, and those
 * values can be used for assertions in tests.
 * <p>
 * This class is thread safe. Committed values can be queried from test threads while they are being updated in Raft
 * node threads.
 */
public class SimpleStateMachine implements StateMachine {

    private final Map<Long, Object> map = createMap();
    private final boolean newTermOpEnabled;
    private Object lastValue;

    public SimpleStateMachine() {
        this(true);
    }

    public SimpleStateMachine(boolean newTermOpEnabled) {
        this.newTermOpEnabled = newTermOpEnabled;
    }

    /**
     * Returns an operation that will add the given value to the state machine.
     *
     * @param val
     *            the value to be added to the state machine
     *
     * @return the operation that will add the given value to the state machine
     */
    public static Object applyValue(Object val) {
        return new Apply(val);
    }

    /**
     * Returns an operation that will query the last value applied to the state machine.
     *
     * @return the operation that will query the last value applied to the state machine
     */
    public static Object queryLastValue() {
        return new QueryLast();
    }

    /**
     * Returns an operation that will query all of the values applied to the state machine.
     *
     * @return the operation that will query all of the values applied to the state machine
     */
    public static Object queryAllValues() {
        return new QueryAll();
    }

    /**
     * Returns the value committed at the given commit index.
     *
     * @param commitIndex
     *            the commit index to get the value
     *
     * @return the value committed at the given commit index
     */
    public synchronized Object get(long commitIndex) {
        return map.get(commitIndex);
    }

    /**
     * Returns the number of values committed.
     *
     * @return the number of values committed
     */
    public synchronized int size() {
        return map.size();
    }

    /**
     * Returns a set of committed values.
     *
     * @return a set of committed values.
     */
    public synchronized Set<Object> valueSet() {
        return new HashSet<>(map.values());
    }

    /**
     * Returns a list of committed values sorted by their commit indices.
     *
     * @return a list of committed values sorted by their commit indices
     */
    public synchronized List<Object> valueList() {
        return new ArrayList<>(map.values());
    }

    @Override
    public synchronized Object runOperation(long commitIndex, @Nonnull Object operation) {
        if (operation instanceof Apply) {
            Apply apply = (Apply) operation;
            assert !map.containsKey(commitIndex) : "Cannot apply " + apply.val + "since commitIndex: " + commitIndex
                    + " already contains: " + map.get(commitIndex);
            map.put(commitIndex, apply.val);
            lastValue = apply.val;
            return apply.val;
        } else if (operation instanceof QueryLast) {
            return lastValue;
        } else if (operation instanceof QueryAll) {
            return valueList();
        } else if (operation instanceof NewTermOp) {
            return null;
        }

        throw new IllegalArgumentException("Invalid op: " + operation + " at commit index: " + commitIndex);
    }

    @Override
    public void takeSnapshot(long commitIndex, Consumer<Object> chunkConsumer) {
        // no need for synchronized because we are not mutating the map
        // and we are on the RaftNode thread

        Map<Long, Object> chunk = createMap();
        for (Entry<Long, Object> e : map.entrySet()) {
            assert e.getKey() <= commitIndex : "Key: " + e.getKey() + ", commit-index: " + commitIndex;
            chunk.put(e.getKey(), e.getValue());
            if (chunk.size() == 10) {
                chunkConsumer.accept(chunk);
                chunk = createMap();
            }
        }

        if (map.size() == 0 || chunk.size() > 0) {
            chunkConsumer.accept(chunk);
        }
    }

    @Override
    public synchronized void installSnapshot(long commitIndex, @Nonnull List<Object> chunks) {
        map.clear();
        for (Object chunk : chunks) {
            for (Entry<Long, Object> e : ((Map<Long, Object>) chunk).entrySet()) {
                map.put(e.getKey(), e.getValue());
                lastValue = e.getValue();
            }
        }
    }

    @Nonnull
    @Override
    public Object getNewTermOperation() {
        return newTermOpEnabled ? new NewTermOp() : null;
    }

    private Map<Long, Object> createMap() {
        return new LinkedHashMap<>();
    }

    private static class Apply {
        private Object val;

        Apply(Object val) {
            this.val = val;
        }

        @Override
        public String toString() {
            return "Apply{" + "val=" + val + '}';
        }
    }

    private static class QueryLast {
        @Override
        public String toString() {
            return "Query{}";
        }
    }

    private static class QueryAll {
        @Override
        public String toString() {
            return "QueryAll{}";
        }
    }

    private static final class NewTermOp {
        @Override
        public String toString() {
            return "NewTermOp{}";
        }
    }

}
