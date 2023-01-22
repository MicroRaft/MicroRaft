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

package io.microraft.afloatdb.config;

import com.typesafe.config.Config;
import io.microraft.afloatdb.AfloatDBException;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import static java.util.Collections.unmodifiableList;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;

public class RaftGroupConfig {

    private String id;
    private List<AfloatDBEndpointConfig> initialEndpoints = Collections.emptyList();
    private String joinTo;

    private RaftGroupConfig() {
    }

    @Nonnull
    public static RaftGroupConfig from(@Nonnull Config config) {
        requireNonNull(config);

        try {
            RaftGroupConfigBuilder builder = newBuilder();

            if (config.hasPath("id")) {
                builder.setId(config.getString("id"));
            }

            if (config.hasPath("initial-endpoints")) {
                List<AfloatDBEndpointConfig> initialEndpoints = config.getList("initial-endpoints").stream()
                        .map(configVal -> configVal.atKey("endpoint").getConfig("endpoint"))
                        .map(AfloatDBEndpointConfig::from).collect(toList());
                builder.setInitialEndpoints(initialEndpoints);
            }

            if (config.hasPath("join-to")) {
                builder.setJoinTo(config.getString("join-to"));
            }

            return builder.build();
        } catch (Exception e) {
            throw new AfloatDBException("Invalid configuration: " + config, e);
        }
    }

    @Nonnull
    public static RaftGroupConfigBuilder newBuilder() {
        return new RaftGroupConfigBuilder();
    }

    @Nonnull
    public String getId() {
        return id;
    }

    @Nullable
    public List<AfloatDBEndpointConfig> getInitialEndpoints() {
        return initialEndpoints;
    }

    @Nullable
    public String getJoinTo() {
        return joinTo;
    }

    @Override
    public String toString() {
        return "RaftGroupConfig{" + "id='" + id + '\'' + ", initialEndpoints=" + initialEndpoints + ", joinTo='"
                + joinTo + '\'' + '}';
    }

    public static class RaftGroupConfigBuilder {

        private RaftGroupConfig config = new RaftGroupConfig();

        private RaftGroupConfigBuilder() {
        }

        @Nonnull
        public RaftGroupConfigBuilder setId(@Nonnull String id) {
            config.id = requireNonNull(id);
            return this;
        }

        @Nonnull
        public RaftGroupConfigBuilder setInitialEndpoints(@Nonnull List<AfloatDBEndpointConfig> endpointConfigs) {
            requireNonNull(endpointConfigs);
            config.initialEndpoints = unmodifiableList(new ArrayList<>(endpointConfigs));
            return this;
        }

        @Nonnull
        public RaftGroupConfigBuilder setJoinTo(@Nonnull String joinTo) {
            config.joinTo = requireNonNull(joinTo);
            return this;
        }

        @Nonnull
        public RaftGroupConfig build() {
            if (config == null) {
                throw new AfloatDBException("RaftGroupConfig already built!");
            }

            if (config.id == null) {
                throw new AfloatDBException("Cannot build RaftGroupConfig without id!");
            }

            if ((config.initialEndpoints.isEmpty() && config.joinTo == null)
                    || (!config.initialEndpoints.isEmpty() && config.joinTo != null)) {
                throw new AfloatDBException("Either initial-endpoints or join-to must be set!");
            }

            if (config.joinTo == null && config.initialEndpoints.size() < 2) {
                throw new IllegalArgumentException(
                        "Could not bootstrap new cluster with " + config.initialEndpoints.size() + " endpoint!");
            }

            Set<String> nodeIds = config.initialEndpoints.stream().map(AfloatDBEndpointConfig::getId).collect(toSet());
            if (nodeIds.size() != config.initialEndpoints.size()) {
                throw new AfloatDBException("Duplicate endpoint ids!");
            }

            Set<String> addresses = config.initialEndpoints.stream().map(AfloatDBEndpointConfig::getAddress)
                    .collect(toSet());
            if (addresses.size() != config.initialEndpoints.size()) {
                throw new AfloatDBException("Duplicate addresses in initial endpoints!");
            }

            RaftGroupConfig config = this.config;
            this.config = null;
            return config;
        }

    }

}
