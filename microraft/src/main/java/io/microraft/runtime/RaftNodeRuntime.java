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

package io.microraft.runtime;

import io.microraft.RaftEndpoint;
import io.microraft.RaftNode;
import io.microraft.model.message.RaftMessage;
import io.microraft.report.RaftNodeReport;

import javax.annotation.Nonnull;
import java.util.concurrent.TimeUnit;

/**
 * The abstraction used by {@link RaftNode} instances as the integration point
 * between the Raft consensus algorithm implementation and the the underlying
 * platform which is responsible for task execution, scheduling, and
 * networking.
 * <p>
 * A Raft node runs in a single-threaded manner. Even if multiple threads are
 * utilized by the underlying platform, given tasks must be executed
 * in a single threaded manner and the happens-before relationship must be
 * maintained between tasks of a single Raft node.
 *
 * @author mdogan
 * @author metanet
 * @see RaftNode
 */
public interface RaftNodeRuntime {

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

    /**
     * Attempts to send the given {@link RaftMessage} object to the given endpoint
     * in a best-effort manner. This method should not block the caller.
     * <p>
     * This method should not throw an exception, for example if the given
     * {@link RaftMessage} object has not been sent to the given endpoint or
     * an internal error has occurred.
     *
     * @param target
     *         the target endpoint to send the Raft message
     * @param message
     *         the Raft message object to be sent
     */
    void send(@Nonnull RaftEndpoint target, @Nonnull RaftMessage message);

    /**
     * Returns true if the given endpoint is supposedly reachable by the time
     * this method is called, false otherwise.
     * <p>
     * This method is not required to return a precise information. For
     * instance, the local Raft node runtime does not need to ping the given
     * endpoint to check if it is reachable when this method is called.
     * Instead, the local Raft node could use a local information, such as
     * recency of a message sent by or having a TCP connection to the given
     * Raft endpoint.
     *
     * @param endpoint
     *         the Raft endpoint to check reachability
     *
     * @return true if given endpoint is reachable, false otherwise
     */
    boolean isReachable(@Nonnull RaftEndpoint endpoint);

    /**
     * Called when term, role, status, known leader, or member list
     * of the Raft node changes.
     *
     * @param report
     *         the report of the new state of the Raft node
     */
    void handleRaftNodeReport(@Nonnull RaftNodeReport report);

    /**
     * Called when a Raft group is terminated gracefully via
     * {@link RaftNode#terminateGroup()}.
     *
     * @see RaftNode#terminateGroup()
     */
    void onRaftGroupTerminated();

}
