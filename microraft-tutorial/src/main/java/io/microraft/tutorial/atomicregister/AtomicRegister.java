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

package io.microraft.tutorial.atomicregister;

import io.microraft.statemachine.StateMachine;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;
import java.util.function.Consumer;

/**
 * This is the base class for our atomic register implementation.
 * In this class, we only define a marker interface for the operations we will
 * commit on the atomic register state machine, and the "new term operation"
 * which will be committed after leader elections.
 * <p>
 * Subclasses are expected to implement operation execution and snapshotting
 * logic.
 * <p>
 * YOU CAN SEE THIS CLASS AT:
 * <p>
 * https://github.com/MicroRaft/MicroRaft/blob/master/microraft-tutorial/src/main/java/io/microraft/tutorial/atomicregister/AtomicRegister.java
 */
public class AtomicRegister
        implements StateMachine {

    @Override
    public Object runOperation(long commitIndex, @Nonnull Object operation) {
        if (operation instanceof NewTermOperation) {
            return null;
        }

        throw new IllegalArgumentException("Invalid operation: " + operation + " at commit index: " + commitIndex);
    }

    @Nullable
    @Override
    public Object getNewTermOperation() {
        return new NewTermOperation();
    }

    @Override
    public void takeSnapshot(long commitIndex, Consumer<Object> snapshotChunkConsumer) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void installSnapshot(long commitIndex, @Nonnull List<Object> snapshotChunks) {
        throw new UnsupportedOperationException();
    }

    private static class NewTermOperation {
    }

}
