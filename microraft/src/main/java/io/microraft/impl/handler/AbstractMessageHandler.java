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

package io.microraft.impl.handler;

import io.microraft.impl.RaftNodeImpl;
import io.microraft.impl.task.RaftNodeStatusAwareTask;
import io.microraft.model.message.RaftMessage;

import javax.annotation.Nonnull;

/**
 * Base class for {@link RaftMessage} handlers.
 */
public abstract class AbstractMessageHandler<T extends RaftMessage>
        extends RaftNodeStatusAwareTask {

    protected final T message;

    AbstractMessageHandler(RaftNodeImpl raftNode, T message) {
        super(raftNode);
        this.message = message;
    }

    @Override protected final void doRun() {
        handle(message);
    }

    protected abstract void handle(@Nonnull T message);

}
