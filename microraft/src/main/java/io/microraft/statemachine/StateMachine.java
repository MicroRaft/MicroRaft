/*
 * Original work Copyright (c) 2008-2020, Hazelcast, Inc.
 * Modified work Copyright (c) 2020, MicroRaft.
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

package io.microraft.statemachine;

import java.util.List;
import java.util.function.Consumer;

import javax.annotation.Nonnull;

import io.microraft.RaftConfig;
import io.microraft.RaftNode;
import io.microraft.executor.RaftNodeExecutor;
import io.microraft.lifecycle.RaftNodeLifecycleAware;

/**
 * The abstraction used by {@link RaftNode} instances to execute operations on
 * the user-supplied state machines.
 * <p>
 * Raft nodes do not deal with the actual logic of committed operations. Once a
 * given operation is committed by Raft, i.e., it is replicated to the majority
 * of the Raft nodes, the operation is passed to the given state machine
 * implementation. It is the state machine implementation's responsibility to
 * ensure deterministic execution of the committed operations. In other words, a
 * committed operation must produce the same result independent of when or on
 * which Raft node instance it is being executed.
 * <p>
 * The operations committed in a Raft node instance are run in the same thread
 * that runs the tasks submitted by that Raft node instance. Since
 * {@link RaftNodeExecutor} ensures the thread-safe execution of the tasks
 * submitted by a Raft node, state machine implementations do not need to be
 * thread-safe.
 * <p>
 * A {@link StateMachine} implementation can implement
 * {@link RaftNodeLifecycleAware} to perform initialization and clean up work
 * during {@link RaftNode} startup and termination. {@link RaftNode} calls
 * {@link RaftNodeLifecycleAware#onRaftNodeStart()} before calling any other
 * method on {@link StateMachine}, and finally calls
 * {@link RaftNodeLifecycleAware#onRaftNodeTerminate()} on termination.
 *
 * @see RaftNode
 * @see RaftNodeExecutor
 * @see RaftNodeLifecycleAware
 */
public interface StateMachine {

    /**
     * Executes the given operation on the state machine and returns result of the
     * operation.
     * <p>
     * Please note that the given operation must be deterministic and return the
     * same result on all Raft nodes of the Raft group.
     * <p>
     * An operation is executed when it is replicated to the majority of the Raft
     * group, hence committed. In addition to that, an operation can be replayed,
     * i.e., executed again, if a Raft node crashes and restarts with persisting its
     * internal state.
     * <p>
     * MicroRaft does not inform the state machines if the operation is being
     * executed for the first time or replayed, and it is the state machine
     * implementations' responsibility to ensure determinism in both cases. For
     * instance, if a state machine implementation creates some side effects on
     * operation execution, it can also persist the log index of the last executed
     * operation so that replays do not cause duplicate side effects. Another option
     * would be creating side effects idempotently, so that replays do not make any
     * difference.
     *
     * @param commitIndex
     *            the Raft log index on which the given operation is committed
     * @param operation
     *            the user-supplied operation to be executed
     *
     * @return the result of the operation execution
     */
    Object runOperation(long commitIndex, @Nonnull Object operation);

    /**
     * Takes a snapshot of the state machine for the given commit index which is the
     * current commit index at the local Raft node.
     * <p>
     * A snapshot must be immutable and must not change when new operations are
     * executed on the state machine.
     * <p>
     * If a state machine implementation maintains a large state, it can divide its
     * state into multiple chunks to help the local Raft node send the snapshot to
     * the other Raft nodes in the group without overloading the system.
     * <p>
     * Once a follower falls behind the leader and requires to install a snapshot,
     * it may fetch the latest snapshot chunks both from the leader and the other
     * followers. Moreover, snapshot chunks can be sent one by one or multiple at
     * one go to speed up the snapshot installation process. There is an important
     * caveat here. State machine implementations must populate the snapshot chunks
     * in a deterministic way, so that a slow follower always reaches to the same
     * state with the Raft leader and other followers independent of from which
     * nodes the chunks are fetched. If a state machine cannot create multiple
     * snapshot chunks in a deterministic way, then it can create a single big
     * snapshot chunk to disable optimization, or it can be directly disabled via
     * {@link RaftConfig}. However, disabling this optimization can cause longer
     * snapshot transfer durations since the bandwidth of followers is not utilized.
     *
     * @param commitIndex
     *            the commit index on which the current snapshot is being taken
     * @param snapshotChunkConsumer
     *            the consumer object to collect the snapshot chunks
     */
    void takeSnapshot(long commitIndex, Consumer<Object> snapshotChunkConsumer);

    /**
     * Installs the given snapshot chunks for the given commit index, which is same
     * with the commit index on which the last snapshot is taken. The snapshot
     * chunks are in the order they are provided to the chunk consumer parameter of
     * {@link #takeSnapshot(long, Consumer)} method.
     *
     * @param commitIndex
     *            the commit index on which the given snapshot is taken
     * @param snapshotChunks
     *            the list of snapshot chunk provided by the state machine when the
     *            snapshot is taken
     */
    void installSnapshot(long commitIndex, @Nonnull List<Object> snapshotChunks);

    /**
     * Returns the operation to be appended after a new leader is elected in a new
     * term.
     * <p>
     * See <a href=
     * "https://groups.google.com/forum/#!msg/raft-dev/t4xj6dJTP6E/d2D9LrWRza8J">
     * <i>Bug in single-server membership changes</i></a> post by Diego Ongaro for
     * more information.
     *
     * @return the operation to be appended after a new leader is elected in a new
     *         term.
     */
    @Nonnull
    Object getNewTermOperation();

}
