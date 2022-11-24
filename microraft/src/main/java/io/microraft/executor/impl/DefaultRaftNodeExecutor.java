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

package io.microraft.executor.impl;

import static java.util.concurrent.Executors.newSingleThreadScheduledExecutor;

import io.microraft.RaftNode;
import io.microraft.executor.RaftNodeExecutor;
import io.microraft.lifecycle.RaftNodeLifecycleAware;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import javax.annotation.Nonnull;

/**
 * The default implementation of {@link RaftNodeExecutor}.
 * <p>
 * Internally uses a single-threaded {@link ScheduledExecutorService} to execute
 * tasks submitted and scheduled by {@link RaftNode}.
 *
 * @see RaftNode
 * @see RaftNodeExecutor
 */
public class DefaultRaftNodeExecutor implements RaftNodeExecutor, RaftNodeLifecycleAware {

    private static final AtomicInteger RAFT_THREAD_ID = new AtomicInteger(0);

    private final ThreadGroup threadGroup = new ThreadGroup("RaftThread");
    private final ScheduledExecutorService executor = newSingleThreadScheduledExecutor(
            r -> new Thread(threadGroup, r, "RaftThread-" + RAFT_THREAD_ID.getAndIncrement()));

    @Override
    public void execute(@Nonnull Runnable task) {
        submit(task);
    }

    @Override
    public void submit(@Nonnull Runnable task) {
        executor.submit(task);
    }

    @Override
    public void schedule(@Nonnull Runnable task, long delay, @Nonnull TimeUnit timeUnit) {
        executor.schedule(task, delay, timeUnit);
    }

    @Override
    public void onRaftNodeTerminate() {
        executor.shutdown();
    }

    public ScheduledExecutorService getExecutor() {
        return executor;
    }
}
