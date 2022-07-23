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

import io.microraft.model.log.BaseLogEntry;

import javax.annotation.Nonnull;

/**
 * Base class for Raft log entry implementations.
 */
public class DefaultAbstractLogEntry implements BaseLogEntry {

    int term;
    long index;
    Object operation;

    DefaultAbstractLogEntry() {
    }

    @Override
    public long getIndex() {
        return index;
    }

    @Override
    public int getTerm() {
        return term;
    }

    @Nonnull
    @Override
    public Object getOperation() {
        return operation;
    }

}
