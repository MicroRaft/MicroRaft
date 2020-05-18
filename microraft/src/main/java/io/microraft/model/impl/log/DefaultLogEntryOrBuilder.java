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

package io.microraft.model.impl.log;

import io.microraft.model.log.LogEntry;
import io.microraft.model.log.LogEntry.LogEntryBuilder;

import javax.annotation.Nonnull;

import static java.util.Objects.requireNonNull;

/**
 * @author metanet
 */
public class DefaultLogEntryOrBuilder
        extends AbstractLogEntry
        implements LogEntry, LogEntryBuilder {

    private DefaultLogEntryOrBuilder builder = this;

    @Nonnull
    @Override
    public LogEntryBuilder setIndex(long index) {
        builder.index = index;
        return this;
    }

    @Nonnull
    @Override
    public LogEntryBuilder setTerm(int term) {
        builder.term = term;
        return this;
    }

    @Nonnull
    @Override
    public LogEntryBuilder setOperation(@Nonnull Object operation) {
        builder.operation = operation;
        return this;
    }

    @Nonnull
    @Override
    public LogEntry build() {
        requireNonNull(builder);
        builder = null;
        return this;
    }

    @Override
    public String toString() {
        String header = builder != null ? "LogEntryBuilder" : "LogEntry";
        return header + "{" + "term=" + term + ", index=" + index + ", operation=" + operation + '}';
    }

}
