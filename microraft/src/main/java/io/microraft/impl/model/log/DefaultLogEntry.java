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

package io.microraft.impl.model.log;

import io.microraft.model.log.LogEntry;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * @author metanet
 */
public class DefaultLogEntry
        extends AbstractLogEntry
        implements LogEntry {

    private DefaultLogEntry() {
    }

    @Override
    public String toString() {
        return "LogEntry{" + "term=" + term + ", index=" + index + ", operation=" + operation + '}';
    }

    public static class DefaultLogEntryBuilder
            implements LogEntryBuilder {

        private DefaultLogEntry entry = new DefaultLogEntry();

        @Nonnull
        @Override
        public LogEntryBuilder setIndex(long index) {
            entry.index = index;
            return this;
        }

        @Nonnull
        @Override
        public LogEntryBuilder setTerm(int term) {
            entry.term = term;
            return this;
        }

        @Nonnull
        @Override
        public LogEntryBuilder setOperation(@Nullable Object operation) {
            entry.operation = operation;
            return this;
        }

        @Nonnull
        @Override
        public LogEntry build() {
            if (this.entry == null) {
                throw new IllegalStateException("LogEntry already built!");
            }

            LogEntry entry = this.entry;
            this.entry = null;
            return entry;
        }

    }

}
