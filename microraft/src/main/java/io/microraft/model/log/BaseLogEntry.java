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

package io.microraft.model.log;

import io.microraft.model.RaftModel;
import io.microraft.model.RaftModelFactory;

import javax.annotation.Nonnull;

/**
 * Base class for Raft log entries.
 * <p>
 * Each log entry stores an operation that will be executed on the state
 * machine along with the term number when the operation was received by
 * the leader. Term numbers are used to detect inconsistencies between logs.
 * Each log entry also has an integer index identifying its position
 * in the Raft log.
 * <p>
 * {@link BaseLogEntry} objects are created by {@link RaftModelFactory}.
 *
 * @see RaftModel
 * @see RaftModelFactory
 */
public interface BaseLogEntry
        extends RaftModel {

    long getIndex();

    int getTerm();

    @Nonnull
    Object getOperation();

}
