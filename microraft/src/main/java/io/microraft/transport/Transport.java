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

package io.microraft.transport;

import io.microraft.RaftEndpoint;
import io.microraft.RaftNode;
import io.microraft.executor.RaftNodeExecutor;
import io.microraft.lifecycle.RaftNodeLifecycleAware;
import io.microraft.model.RaftModel;
import io.microraft.model.RaftModelFactory;
import io.microraft.model.message.RaftMessage;

import javax.annotation.Nonnull;

/**
 * Used for communicating Raft nodes with each other.
 * <p>
 * Transport implementations must be non-blocking. A Raft node must be able to
 * send a Raft message to another Raft node and continue without blocking. This
 * is required because Raft nodes run concurrently with the Actor model.
 * <p>
 * Transport implementations must be able to serialize {@link RaftMessage}
 * objects created by {@link RaftModelFactory}.
 * <p>
 * A {@link Transport} implementation can implement
 * {@link RaftNodeLifecycleAware} to perform initialization and clean up work
 * during {@link RaftNode} startup and termination. {@link RaftNode} calls
 * {@link RaftNodeLifecycleAware#onRaftNodeStart()} before calling any other
 * method on {@link Transport}, and finally calls
 * {@link RaftNodeLifecycleAware#onRaftNodeTerminate()} on termination.
 *
 * @see RaftModel
 * @see RaftMessage
 * @see RaftModelFactory
 * @see RaftNode
 * @see RaftNodeExecutor
 * @see RaftNodeLifecycleAware
 */
public interface Transport {

    /**
     * Sends the given {@link RaftMessage} object to the given endpoint.
     * This method must not block the caller Raft node instance and
     * return promptly so that the caller can continue its execution.
     * <p>
     * This method must not throw an exception, for example if the given
     * {@link RaftMessage} object has not been sent to the given endpoint or
     * an internal error has occurred. The handling of {@link RaftMessage}
     * objects are designed idempotently. Therefore, if a {@link RaftMessage}
     * object is not sent to the given endpoint, it implies that the source
     * Raft node will not receive a {@link RaftMessage} as response, hence it
     * will re-send the failed {@link RaftMessage} again.
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
     * instance, the Transport implementation does not need to ping the given
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

}
