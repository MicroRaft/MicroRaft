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

package io.microraft.integration;

import io.microraft.RaftNode;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;
import java.util.function.Consumer;

/**
 * The abstraction used by {@link RaftNode} instances to execute operations
 * on the user-supplied state machines.
 * <p>
 * Raft nodes do not deal with the actual logic of committed operations.
 * Once a given operation is committed by Raft, i.e., it is replicated to
 * the majority of the Raft nodes, the operation is passed to the given state
 * machine implementation. It is the state machine implementation's
 * responsibility to ensure deterministic execution of the committed
 * operations. In other words, a committed operation must produce the same
 * result independent of when or on which Raft node instance it is being
 * executed.
 * <p>
 * The operations committed in a Raft node instance are run in the same thread
 * that runs the tasks submitted by that Raft node instance. Since
 * {@link RaftNodeRuntime} ensures the thread-safe execution of the tasks
 * submitted by a Raft node, state machine implementations do not need to be
 * thread-safe.
 *
 * @author mdogan
 * @author metanet
 * @see RaftNode
 */
public interface StateMachine {

    /**
     * Executes the given operation on the state machine and returns
     * result of the operation.
     * <p>
     * Please note that the given operation must be deterministic and return
     * the same result on all Raft nodes of the Raft group.
     *
     * @param commitIndex Raft log index the given operation is committed at
     * @param operation   user-supplied operation to execute
     * @return result of the operation execution
     */
    Object runOperation(long commitIndex, @Nullable Object operation);

    /**
     * Takes a snapshot of the state machine for the given commit index
     * which is the current commit index at the local Raft node.
     * <p>
     * If a state machine implementation maintains a large state, it can divide
     * its state into multiple chunks to help the local Raft node send
     * the snapshot to the other Raft nodes in the group without overloading
     * the system.
     *
     * @param commitIndex           commit index on which a snapshot is taken
     * @param snapshotChunkConsumer consumer object to which snapshot chunks
     *                              must be passed
     */
    void takeSnapshot(long commitIndex, Consumer<Object> snapshotChunkConsumer);

    /**
     * Installs the given snapshot chunks for the given commit index, which is
     * same with the commit index on which the snapshot is taken.
     *
     * @param commitIndex    commit index of the snapshot
     * @param snapshotChunks snapshot chunks provided by the state machine
     *                       implementation
     */
    void installSnapshot(long commitIndex, @Nonnull List<Object> snapshotChunks);

    /**
     * Returns and operation to be appended after a new leader is elected in
     * a new term. Null return value means no entry will be appended.
     * <p>
     * See <a href="https://groups.google.com/forum/#!msg/raft-dev/t4xj6dJTP6E/d2D9LrWRza8J">
     * <i>Bug in single-server membership changes</i></a> post by Diego Ongaro
     * for more information.
     * <p>
     * At least a NOP object is strongly recommended to be returned
     * on production because it has zero overhead.
     */
    @Nullable
    Object getNewTermOperation();

}
