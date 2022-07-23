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

package io.microraft.lifecycle;

import io.microraft.RaftNode;
import io.microraft.executor.RaftNodeExecutor;
import io.microraft.model.RaftModelFactory;
import io.microraft.persistence.RaftStore;
import io.microraft.report.RaftNodeReportListener;
import io.microraft.statemachine.StateMachine;
import io.microraft.transport.Transport;

/**
 * Used by {@link RaftNode} to notify its components for its lifecycle-related changes, such as startup and termination.
 * These components are {@link RaftNodeExecutor}, {@link StateMachine}, {@link RaftModelFactory}, {@link Transport},
 * {@link RaftStore}, and {@link RaftNodeReportListener}.
 * <p>
 * {@link RaftNode} does not manage the lifecycle of the components provided to it during construction. It only notifies
 * the components that implement this interface.
 * <p>
 * {@link RaftNode} calls the lifecycle-aware components in random order.
 *
 * @see RaftNode
 * @see RaftNodeExecutor
 * @see StateMachine
 * @see Transport
 * @see RaftStore
 * @see RaftModelFactory
 * @see RaftNodeReportListener
 */
public interface RaftNodeLifecycleAware {

    /**
     * Called by {@link RaftNode} during startup.
     * <p>
     * If an exception is thrown, {@link RaftNode} stops its start procedure and immediately terminates itself.
     */
    default void onRaftNodeStart() {
    }

    /**
     * Called by {@link RaftNode} during termination.
     * <p>
     * The component will not receive any API call after this call.
     * <p>
     * Exceptions thrown by implementations of this method are handled and logged by {@link RaftNode}.
     */
    default void onRaftNodeTerminate() {
    }

}
