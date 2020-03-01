package com.hazelcast.raft.impl.handler;

import com.hazelcast.raft.RaftEndpoint;
import com.hazelcast.raft.impl.RaftNodeImpl;
import com.hazelcast.raft.impl.task.RaftNodeStatusAwareTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Base class for response handlers.
 * <p>
 * If {@link #sender()} is not a known Raft group member,
 * then the response is ignored.
 *
 * @author mdogan
 * @author metanet
 */
public abstract class BaseResponseHandler
        extends RaftNodeStatusAwareTask {

    BaseResponseHandler(RaftNodeImpl raftNode) {
        super(raftNode);
    }

    @Override
    protected final void doRun() {
        RaftEndpoint sender = sender();
        if (!node.state().isKnownMember(sender)) {
            Logger logger = LoggerFactory.getLogger(getClass());
            logger.warn("{} Won't run, since {} is unknown to us.", node.localEndpointName(), sender);
            return;
        }

        handleResponse();
    }

    protected abstract void handleResponse();

    protected abstract RaftEndpoint sender();

}
