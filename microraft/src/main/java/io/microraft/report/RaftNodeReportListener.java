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

package io.microraft.report;

import io.microraft.RaftNode;
import io.microraft.executor.RaftNodeExecutor;
import io.microraft.statemachine.StateMachine;
import io.microraft.transport.Transport;

import java.util.function.Consumer;

/**
 * Used for informing external systems about events related to the execution of
 * the Raft consensus algorithm.
 * <p>
 * {@link RaftNodeExecutor}, {@link Transport}, and {@link StateMachine}
 * implementations can also implement this interface, and get notified about
 * lifecycle events.
 * <p>
 * Called when term, role, status, known leader, or member list
 * of the Raft node changes.
 *
 * @author metanet
 * @see RaftNodeReport
 * @see RaftNode
 */
public interface RaftNodeReportListener
        extends Consumer<RaftNodeReport> {
}
