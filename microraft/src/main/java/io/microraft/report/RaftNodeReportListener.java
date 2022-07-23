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
import io.microraft.lifecycle.RaftNodeLifecycleAware;

import java.util.function.Consumer;

/**
 * Used for informing external systems about events related to the execution of the Raft consensus algorithm.
 * <p>
 * Called when term, role, status, known leader, or member list of the Raft node changes.
 * <p>
 * A {@link RaftNodeReportListener} implementation can implement {@link RaftNodeLifecycleAware} to perform
 * initialization and clean up work during {@link RaftNode} startup and termination. {@link RaftNode} calls
 * {@link RaftNodeLifecycleAware#onRaftNodeStart()} before calling any other method on {@link RaftNodeReportListener},
 * and finally calls {@link RaftNodeLifecycleAware#onRaftNodeTerminate()} on termination.
 *
 * @see RaftNodeReport
 * @see RaftNode
 */
public interface RaftNodeReportListener extends Consumer<RaftNodeReport> {
}
