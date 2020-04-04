/*
 * Original work Copyright (c) 2008-2020, Hazelcast, Inc.
 * Modified work Copyright 2020, MicroRaft.
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

package io.microraft.impl.local;

import io.microraft.RaftEndpoint;
import io.microraft.model.message.RaftMessage;

import javax.annotation.Nonnull;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

import static java.util.Collections.newSetFromMap;

/**
 * Used for blocking and altering Raft messages sent between Raft nodes
 * during local testing.
 *
 * @author metanet
 */
public class Firewall {

    private final Set<DropRule> dropRules = newSetFromMap(new ConcurrentHashMap<>());
    private final Map<RaftEndpoint, Function<RaftMessage, RaftMessage>> alterFunctions = new ConcurrentHashMap<>();

    /**
     * Adds a drop-message rule for the given target Raft endpoint
     * and the Raft message type.
     * <p>
     * After this call, Raft messages of the given type sent to the given Raft
     * endpoint are silently dropped.
     *
     * @param target
     *         the target Raft endpoint to drop Raft messages of the given type
     * @param type
     *         the type of the Raft messages to be dropped
     */
    void dropMessagesTo(RaftEndpoint target, Class<? extends RaftMessage> type) {
        dropRules.add(new DropRule(target, type));
    }

    /**
     * Adds a drop-all-messages rule for the given target Raft endpoint.
     * <p>
     * After this call, all Raft messages sent to the given Raft endpoint are
     * silently dropped.
     * <p>
     * If there were drop-message rules for the given target Raft endpoint,
     * they are replaced with a drop-all-messages rule.
     *
     * @param target
     *         the target Raft endpoint to drop all Raft messages
     */
    void dropAllMessagesTo(RaftEndpoint target) {
        dropRules.add(new DropRule(target, null));
    }

    /**
     * Deletes the drop-message rule for the given target Raft endpoint
     * and the Raft message type.
     *
     * @param target
     *         the target Raft endpoint to remove the drop-message rule
     * @param messageType
     *         the type of the Raft messages
     */
    void allowMessagesTo(RaftEndpoint target, Class<? extends RaftMessage> messageType) {
        dropRules.remove(new DropRule(target, messageType));
    }

    /**
     * Deletes all drop-message and drop-all-messages rules created for
     * the given target Raft endpoint.
     *
     * @param target
     *         the target Raft endpoint to remove all rules
     */
    void allowAllMessagesTo(RaftEndpoint target) {
        dropRules.removeIf(entry -> target.equals(entry.endpoint));
    }

    /**
     * Drops Raft messages of the given type to all Raft endpoints.
     *
     * @param messageType
     *         the type of the Raft messages to be dropped
     */
    void dropMessagesToAll(Class<? extends RaftMessage> messageType) {
        dropRules.add(new DropRule(null, messageType));
    }

    /**
     * Deletes the drop-message rule for the given Raft message type
     * to any Raft endpoint.
     *
     * @param messageType
     *         the type of the Raft message to delete the rule
     */
    void allowMessagesToAll(Class<? extends RaftMessage> messageType) {
        dropRules.remove(new DropRule(null, messageType));
    }

    /**
     * Applies the given function an all Raft messages sent to the given Raft
     * endpoint.
     * <p>
     * If the given function is not altering a given Raft message, it should
     * return it as it is, instead of returning null.
     * <p>
     * Only a single alter rule can be created for a given Raft endpoint and
     * a new alter rule overwrites the previous one.
     *
     * @param endpoint
     *         the target Raft endpoint to apply the alter function
     * @param function
     *         the alter function to apply to Raft messages
     */
    void alterMessagesTo(RaftEndpoint endpoint, Function<RaftMessage, RaftMessage> function) {
        alterFunctions.put(endpoint, function);
    }

    /**
     * Deletes the alter-message rule for the given Raft endpoint.
     *
     * @param target
     *         the target Raft endpoint to delete the alter function
     */
    void removeAlterMessageFunctionTo(RaftEndpoint target) {
        alterFunctions.remove(target);
    }

    /**
     * Returns true if the given Raft message sent to the given Raft endpoint
     * should be dropped.
     *
     * @param target
     *         the target Raft endpoint to check if the Raft message should
     *         be dropped
     * @param message
     *         the Raft message to check if it should be dropped
     *
     * @return true if the given Raft message sent to the given Raft endpoint
     *         should be dropped
     */
    boolean shouldDrop(RaftEndpoint target, RaftMessage message) {
        return dropRules.stream().anyMatch(rule -> rule.shouldDrop(target, message));
    }

    /**
     * Alters the given Raft message sent to the given Raft endpoint if there
     * is an alter function for the Raft message type. If there exists no alter
     * function for the given Raft message type, then the given Raft message is
     * returned as it is.
     *
     * @param target
     *         the target Raft endpoint to which the given Raft message is sent
     * @param message
     *         the Raft message to alter if there exists an alter function for
     *         its type
     *
     * @return the altered or the original Raft message
     */
    RaftMessage tryAlterMessage(RaftEndpoint target, RaftMessage message) {
        return alterFunctions.getOrDefault(target, Function.identity()).apply(message);
    }

    /**
     * Resets all drop-message and alter-message rules.
     */
    void resetAllRules() {
        dropRules.clear();
        alterFunctions.clear();
    }

    private static final class DropRule {
        private static final RaftEndpoint ALL_ENDPOINTS = LocalRaftEndpoint.newEndpoint();
        private static final RaftMessage ALL_MESSAGES = new RaftMessage() {
            @Override
            public Object getGroupId() {
                return null;
            }

            @Nonnull
            @Override
            public RaftEndpoint getSender() {
                return null;
            }

            @Override
            public int getTerm() {
                return 0;
            }
        };

        final RaftEndpoint endpoint;
        final Class<? extends RaftMessage> type;

        DropRule(RaftEndpoint endpoint, Class<? extends RaftMessage> type) {
            if (endpoint == null && type == null) {
                throw new IllegalArgumentException("Cannot create a drop-all-messages to drop-all-endpoints rule!");
            }

            this.endpoint = endpoint != null ? endpoint : ALL_ENDPOINTS;
            this.type = type != null ? type : ALL_MESSAGES.getClass();
        }

        boolean shouldDrop(RaftEndpoint endpoint, RaftMessage type) {
            return (this.endpoint.equals(ALL_ENDPOINTS) || this.endpoint.equals(endpoint)) && (
                    this.type.isAssignableFrom(ALL_MESSAGES.getClass()) || this.type.isAssignableFrom(type.getClass()));
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            DropRule dropRule = (DropRule) o;

            if (!Objects.equals(endpoint, dropRule.endpoint)) {
                return false;
            }
            return type.equals(dropRule.type);
        }

        @Override
        public int hashCode() {
            int result = endpoint != null ? endpoint.hashCode() : 0;
            result = 31 * result + type.hashCode();
            return result;
        }

        @Override
        public String toString() {
            return "DropRule{" + "endpoint=" + endpoint + ", type=" + type + '}';
        }
    }

}
