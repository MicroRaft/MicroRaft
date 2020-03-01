package com.hazelcast.raft.impl.task;

import com.hazelcast.raft.RaftEndpoint;
import com.hazelcast.raft.impl.RaftNodeImpl;
import com.hazelcast.raft.impl.state.RaftState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Base class for tasks need to know current {@link RaftNodeImpl}.
 * If this RaftNode is terminated or stepped down, the task will be skipped.
 * <p>
 * Subclasses must implement {@link #doRun()} method.
 *
 * @author mdogan
 * @author metanet
 */
public abstract class RaftNodeStatusAwareTask
        implements Runnable {

    protected final RaftNodeImpl node;
    protected final RaftState state;

    protected RaftNodeStatusAwareTask(RaftNodeImpl node) {
        this.node = node;
        this.state = node.state();
    }

    @Override
    public final void run() {
        if (node.getStatus().isTerminal()) {
            getLogger().debug("{} Won't run, since Raft node is terminated.", node.localEndpointName());
            return;
        }

        try {
            doRun();
        } catch (Throwable e) {
            getLogger().error(node.localEndpointName() + " got a failure.", e);
        }
    }

    protected final RaftEndpoint localEndpoint() {
        return node.getLocalEndpoint();
    }

    protected final String localEndpointName() {
        return node.localEndpointName();
    }

    protected abstract void doRun();

    private Logger getLogger() {
        return LoggerFactory.getLogger(getClass());
    }

}
