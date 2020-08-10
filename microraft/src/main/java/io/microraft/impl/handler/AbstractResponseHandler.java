/*
 * Original work Copyright (c) 2008-2020, Hazelcast, Inc.
 * Modified work Copyright (c) 2020, MicroRaft.
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

package io.microraft.impl.handler;

import io.microraft.impl.RaftNodeImpl;
import io.microraft.model.message.RaftMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;

import static java.util.Objects.requireNonNull;

/**
 * Base class for Raft RPC response handlers.
 * <p>
 * If {@link RaftMessage#getSender()} is not a known Raft group member,
 * then the response is ignored.
 */
public abstract class AbstractResponseHandler<T extends RaftMessage>
        extends AbstractMessageHandler<T> {

    AbstractResponseHandler(RaftNodeImpl raftNode, T response) {
        super(raftNode, response);
    }

    @Override
    protected final void handle(@Nonnull T response) {
        requireNonNull(response);

        if (!node.state().isKnownMember(response.getSender())) {
            Logger logger = LoggerFactory.getLogger(getClass());
            logger.warn("{} Won't run, since {} is unknown to us.", localEndpointStr(), response.getSender().getId());
            return;
        }

        handleResponse(response);
    }

    protected abstract void handleResponse(@Nonnull T response);

}
