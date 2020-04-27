/*
 * Copyright (c) 2020, MicroRaft.
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

package io.microraft.tutorial.atomicregister;

import io.microraft.statemachine.StateMachine;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.function.Consumer;

/**
 * Provides snapshotting logic for the atomic register state machine.
 * <p>
 * YOU CAN SEE THIS CLASS AT:
 * <p>
 * https://github.com/MicroRaft/MicroRaft/blob/master/microraft-tutorial/src/main/java/io/microraft/tutorial/atomicregister/SnapshotableAtomicRegister.java
 */
public class SnapshotableAtomicRegister
        extends OperableAtomicRegister
        implements StateMachine {

    @Override
    public void takeSnapshot(long commitIndex, Consumer<Object> snapshotChunkConsumer) {
        // put the current value of the atomic register into a snapshot chunk.
        snapshotChunkConsumer.accept(new AtomicRegisterSnapshotChunk(value));
    }

    @Override
    public void installSnapshot(long commitIndex, @Nonnull List<Object> snapshotChunks) {
        if (snapshotChunks.size() != 1) {
            // takeSnapshot() method returns a single snapshot chunk.
            throw new IllegalArgumentException("Invalid snapshot chunks: " + snapshotChunks + " at commit index: " + commitIndex);
        }

        // install the value of the atomic register from the snapshot chunk.
        this.value = ((AtomicRegisterSnapshotChunk) snapshotChunks.get(0)).value;
    }

    private static class AtomicRegisterSnapshotChunk {
        final Object value;

        AtomicRegisterSnapshotChunk(Object value) {
            this.value = value;
        }

        @Override
        public String toString() {
            return "AtomicRegisterSnapshotChunk{" + "value='" + value + '\'' + '}';
        }
    }

}
