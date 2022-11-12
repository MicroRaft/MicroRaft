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

package io.microraft.tutorial;

import io.microraft.RaftEndpoint;

import javax.annotation.Nonnull;
import java.util.concurrent.atomic.AtomicInteger;

import static java.util.Objects.requireNonNull;

/**
 * A very simple {@link RaftEndpoint} implementation used in the tutorial.
 * <p>
 * Unique Raft endpoints can be created via
 * {@link LocalRaftEndpoint#newEndpoint()}.
 * <p>
 * YOU CAN SEE THIS CLASS AT:
 * <p>
 * https://github.com/MicroRaft/MicroRaft/blob/master/microraft-tutorial/src/main/java/io/microraft/tutorial/LocalRaftEndpoint.java
 */
final class LocalRaftEndpoint implements RaftEndpoint {

    private static final AtomicInteger ID_GENERATOR = new AtomicInteger();
    private final String id;

    private LocalRaftEndpoint(String id) {
        this.id = requireNonNull(id);
    }

    /**
     * Returns a new unique Raft endpoint.
     *
     * @return a new unique Raft endpoint
     */
    static LocalRaftEndpoint newEndpoint() {
        return new LocalRaftEndpoint("node" + ID_GENERATOR.incrementAndGet());
    }

    @Nonnull
    @Override
    public Object getId() {
        return id;
    }

    @Override
    public int hashCode() {
        return id.hashCode();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        LocalRaftEndpoint that = (LocalRaftEndpoint) o;

        return id.equals(that.id);
    }

    @Override
    public String toString() {
        return "LocalRaftEndpoint{" + "id=" + id + '}';
    }

}
