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
import java.util.List;
import java.util.Objects;
import java.util.function.Consumer;

/**
 * Implements operations on the atomic register state machine.
 * <p>
 * This class does not implement the snapshotting logic.
 * <p>
 * YOU CAN SEE THIS CLASS AT:
 * <p>
 * https://github.com/MicroRaft/MicroRaft/blob/master/microraft-tutorial/src/main/java/io/microraft/tutorial/atomicregister/OperableAtomicRegister.java
 */
public class OperableAtomicRegister extends AtomicRegister implements StateMachine {

    protected Object value;

    /**
     * Returns a new operation to update the atomic register with the given value.
     * The operation returns the previous value as response.
     *
     * @param value
     *            to update the atomic register
     *
     * @return the operation to update the atomic register value
     */
    public static AtomicRegisterOperation newSetOperation(String value) {
        return new SetOperation(value);
    }

    /**
     * Returns a new operation to get the current value of the atomic register.
     *
     * @return the operation to get the current value of the atomic register
     */
    public static AtomicRegisterOperation newGetOperation() {
        return new GetOperation();
    }

    /**
     * Returns a compare-and-swap operation to update the atomic register with the
     * given new value of only if its current value matches with the given current
     * value. The operations returns true if the compare-and-swap operation is
     * successful.
     *
     * @param currentValue
     *            the current value of the atomic register for cas
     * @param newValue
     *            the new value for cas
     *
     * @return the operation to perform cas on the atomic register
     */
    public static AtomicRegisterOperation newCasOperation(Object currentValue, Object newValue) {
        return new CasOperation(currentValue, newValue);
    }

    @Override
    public Object runOperation(long commitIndex, @Nonnull Object operation) {
        if (operation instanceof SetOperation) {
            Object prev = this.value;
            this.value = ((SetOperation) operation).value;
            return prev;
        } else if (operation instanceof CasOperation) {
            CasOperation cas = (CasOperation) operation;
            boolean success = Objects.equals(this.value, cas.currentValue);
            if (success) {
                this.value = cas.newValue;
            }

            return success;
        } else if (operation instanceof GetOperation) {
            return value;
        }

        return super.runOperation(commitIndex, operation);
    }

    @Override
    public void takeSnapshot(long commitIndex, Consumer<Object> snapshotChunkConsumer) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void installSnapshot(long commitIndex, @Nonnull List<Object> snapshotChunks) {
        throw new UnsupportedOperationException();
    }

    /**
     * Marker interface for operations to be executed on the atomic register state
     * machine.
     */
    public interface AtomicRegisterOperation {
    }

    private static class SetOperation implements AtomicRegisterOperation {
        final Object value;

        SetOperation(Object value) {
            this.value = value;
        }

        @Override
        public String toString() {
            return "SetOperation{" + "value='" + value + '\'' + '}';
        }
    }

    private static class GetOperation implements AtomicRegisterOperation {
        @Override
        public String toString() {
            return "GetOperation{}";
        }
    }

    private static class CasOperation implements AtomicRegisterOperation {
        final Object currentValue;
        final Object newValue;

        CasOperation(Object currentValue, Object newValue) {
            this.currentValue = currentValue;
            this.newValue = newValue;
        }

        @Override
        public String toString() {
            return "CasOperation{" + "currentValue='" + currentValue + '\'' + ", newValue='" + newValue + '\'' + '}';
        }
    }

}
