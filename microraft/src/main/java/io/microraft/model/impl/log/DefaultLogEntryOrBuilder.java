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

import static java.util.Objects.requireNonNull;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;

import io.microraft.model.log.LogEntry;
import io.microraft.model.log.LogEntry.LogEntryBuilder;

/**
 * The default impl of the {@link LogEntry} and {@link LogEntryBuilder}
 * interfaces. When an instance of this class is created, it is in the builder
 * mode and its state is populated. Once all fields are set, the object switches
 * to the DTO mode where it no longer allows mutations.
 * <p>
 * Please note that {@link #build()} does not verify if all fields are set or
 * not. It is up to the user to populate the DTO state via the builder.
 */
public class DefaultLogEntryOrBuilder extends DefaultAbstractLogEntry implements LogEntry, LogEntryBuilder {

    private DefaultLogEntryOrBuilder builder = this;

    @Nonnull
    @Override
    public LogEntryBuilder setIndex(@Nonnegative long index) {
        builder.index = index;
        return this;
    }

    @Nonnull
    @Override
    public LogEntryBuilder setTerm(@Nonnegative int term) {
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
