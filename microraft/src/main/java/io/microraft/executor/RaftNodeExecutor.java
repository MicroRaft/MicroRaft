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

package io.microraft.executor;

import io.microraft.RaftNode;
import io.microraft.executor.impl.DefaultRaftNodeExecutor;
import io.microraft.report.RaftNodeReportListener;

import javax.annotation.Nonnull;
import java.util.concurrent.TimeUnit;

/**
 * The abstraction used by {@link RaftNode} to execute the Raft consensus
 * algorithm with the Actor model. You can read about the Actor Model at
 * the following link: https://en.wikipedia.org/wiki/Actor_model
 * <p>
 * A Raft node runs by submitting tasks to its Raft node executor. All tasks
 * submitted by a Raft node must be executed serially, with maintaining the
 * happens-before relationship, so that the Raft consensus algorithm and the
 * user-provided state machine logic could be executed without synchronization.
 * <p>
 * A default implementation, {@link DefaultRaftNodeExecutor} is provided and
 * should be suitable for most of the use-cases.
 * <p>
 * Its implementations can also implement {@link RaftNodeReportListener} to
 * get notified about lifecycle events related to the execution of the Raft
 * consensus algorithm.
 *
 * @author mdogan
 * @author metanet
 * @see RaftNode
 * @see DefaultRaftNodeExecutor
 * @see RaftNodeReportListener
 */
public interface RaftNodeExecutor {

    /**
     * Executes the given task on the underlying platform.
     * <p>
     * Please note that all tasks of a single Raft node must be executed
     * in a single-threaded manner and the happens-before relationship
     * must be maintained between given tasks of the Raft node.
     * <p>
     * The underlying platform is free to execute the given task immediately
     * if it fits to the defined guarantees.
     *
     * @param task
     *         the task to be executed.
     */
    void execute(@Nonnull Runnable task);

    /**
     * Submits the given task for execution.
     * <p>
     * If the caller is already on the thread that runs the Raft node,
     * the given task cannot be executed immediately and it must be put into
     * the internal task queue for execution in future.
     *
     * @param task
     *         the task object to be executed later.
     */
    void submit(@Nonnull Runnable task);

    /**
     * Schedules the task on the underlying platform to be executed after
     * the given delay.
     * <p>
     * Please note that even though the scheduling can be offloaded to another
     * thread, the given task must be executed in a single-threaded manner and
     * the happens-before relationship must be maintained between given tasks
     * of the Raft node.
     *
     * @param task
     *         the task to be executed in future
     * @param delay
     *         the time from now to delay execution
     * @param timeUnit
     *         the time unit of the delay
     */
    void schedule(@Nonnull Runnable task, long delay, @Nonnull TimeUnit timeUnit);

}
