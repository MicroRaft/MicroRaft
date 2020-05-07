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

import io.microraft.RaftEndpoint;
import io.microraft.RaftNode;
import io.microraft.executor.RaftNodeExecutor;
import io.microraft.report.RaftNodeReport;
import io.microraft.report.RaftNodeReportListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static io.microraft.RaftNodeStatus.isTerminal;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.Executors.newSingleThreadScheduledExecutor;

/**
 * The default implementation of {@link RaftNodeExecutor}.
 * <p>
 * Internally uses a single-threaded {@link ScheduledExecutorService} to
 * execute tasks submitted and scheduled by {@link RaftNode}.
 *
 * @author metanet
 * @see RaftNode
 * @see RaftNodeExecutor
 */
public class DefaultRaftNodeExecutor
        implements RaftNodeExecutor, RaftNodeReportListener {

    private static final Logger LOGGER = LoggerFactory.getLogger(RaftNodeExecutor.class);

    private final ThreadGroup threadGroup = new ThreadGroup("RaftThread");
    private final RaftEndpoint localEndpoint;
    private final ScheduledExecutorService executor;

    public DefaultRaftNodeExecutor(RaftEndpoint localEndpoint) {
        this.localEndpoint = requireNonNull(localEndpoint);
        this.executor = newSingleThreadScheduledExecutor(r -> new Thread(threadGroup, r));
    }

    @Override
    public void execute(@Nonnull Runnable task) {
        submit(task);
    }

    @Override
    public void submit(@Nonnull Runnable task) {
        try {
            executor.submit(task);
        } catch (RejectedExecutionException e) {
            LOGGER.error(localEndpoint + " failed", e);
        }
    }

    @Override
    public void schedule(@Nonnull Runnable task, long delay, @Nonnull TimeUnit timeUnit) {
        try {
            executor.schedule(task, delay, timeUnit);
        } catch (RejectedExecutionException e) {
            LOGGER.error(localEndpoint + " failed", e);
        }
    }

    @Override
    public void accept(@Nonnull RaftNodeReport report) {
        if (isTerminal(report.getStatus())) {
            shutdownExecutor();
        }
    }

    public ScheduledExecutorService getExecutor() {
        return executor;
    }

    private void shutdownExecutor() {
        executor.shutdown();
    }

}
