/*
 * Copyright (c) 2020, AfloatDB.
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

package io.microraft.afloatdb.internal.utils;

import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.microraft.RaftEndpoint;
import io.microraft.exception.CannotReplicateException;
import io.microraft.exception.IndeterminateStateException;
import io.microraft.exception.LaggingCommitIndexException;
import io.microraft.exception.MismatchingRaftGroupMembersCommitIndexException;
import io.microraft.exception.NotLeaderException;
import io.microraft.exception.RaftException;

import java.io.PrintWriter;
import java.io.StringWriter;

import static io.grpc.Status.DEADLINE_EXCEEDED;
import static io.grpc.Status.FAILED_PRECONDITION;
import static io.grpc.Status.INTERNAL;
import static io.grpc.Status.INVALID_ARGUMENT;
import static io.grpc.Status.RESOURCE_EXHAUSTED;

public final class Exceptions {

    private final static String RAFT_EXCEPTION_KEYWORD = "RAFT_ERROR";

    private Exceptions() {
    }

    public static boolean isRaftException(String message) {
        return message != null && (message
                .contains(RAFT_EXCEPTION_KEYWORD + ":" + NotLeaderException.class.getSimpleName())
                || message.contains(RAFT_EXCEPTION_KEYWORD + ":" + CannotReplicateException.class.getSimpleName())
                || message.contains(RAFT_EXCEPTION_KEYWORD + ":" + IndeterminateStateException.class.getSimpleName())
                || message.contains(RAFT_EXCEPTION_KEYWORD + ":"
                        + MismatchingRaftGroupMembersCommitIndexException.class.getSimpleName())
                || message.contains(RAFT_EXCEPTION_KEYWORD + ":" + LaggingCommitIndexException.class.getSimpleName()));
    }

    public static boolean isRaftException(String message, Class<? extends RaftException> e) {
        return message != null && message.contains(RAFT_EXCEPTION_KEYWORD + ":" + e.getSimpleName());
    }

    public static StatusRuntimeException wrap(Throwable t) {
        if (t instanceof StatusRuntimeException) {
            return (StatusRuntimeException) t;
        }

        Status status;
        if (t instanceof MismatchingRaftGroupMembersCommitIndexException) {
            status = INVALID_ARGUMENT;
        } else if (t instanceof NotLeaderException || t instanceof LaggingCommitIndexException) {
            status = FAILED_PRECONDITION;
        } else if (t instanceof CannotReplicateException) {
            status = RESOURCE_EXHAUSTED;
        } else if (t instanceof IndeterminateStateException) {
            status = DEADLINE_EXCEEDED;
        } else {
            status = INTERNAL;
        }

        return wrap(status, t);
    }

    private static StatusRuntimeException wrap(Status status, Throwable t) {
        String stackTrace = getStackTraceString(t);
        StringBuilder sb = new StringBuilder();
        boolean isRaftException = t instanceof RaftException;
        sb.append(isRaftException ? RAFT_EXCEPTION_KEYWORD : "OTHER").append(":").append(t.getClass().getSimpleName())
                .append("\n");
        if (isRaftException) {
            RaftEndpoint leader = ((RaftException) t).getLeader();
            sb.append(leader != null ? leader.getId() : "").append("\n");
        }

        sb.append(t.getMessage());

        return status.withDescription(sb.toString()).augmentDescription(stackTrace).withCause(t).asRuntimeException();
    }

    private static String getStackTraceString(Throwable e) {
        StringWriter writer = new StringWriter();
        PrintWriter printWriter = new PrintWriter(writer);
        e.printStackTrace(printWriter);
        return writer.toString();
    }

    public static void runSilently(Runnable r) {
        try {
            r.run();
        } catch (Throwable ignored) {
        }
    }

}
