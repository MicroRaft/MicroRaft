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
import com.typesafe.config.ConfigFactory;
import io.microraft.afloatdb.AfloatDBException;
import io.microraft.RaftConfig;

import javax.annotation.Nonnull;

import static io.microraft.HoconRaftConfigParser.parseConfig;
import static io.microraft.RaftConfig.DEFAULT_RAFT_CONFIG;
import static java.util.Objects.requireNonNull;

public final class AfloatDBConfig {

    private Config config;
    private AfloatDBEndpointConfig localEndpointConfig;
    private RaftGroupConfig raftGroupConfig;
    private RaftConfig raftConfig;
    private RpcConfig rpcConfig;
    private PersistenceConfig persistenceConfig;

    private AfloatDBConfig() {
    }

    @Nonnull
    public static AfloatDBConfig from(@Nonnull Config config) {
        return newBuilder().setConfig(requireNonNull(config)).build();
    }

    @Nonnull
    public static AfloatDBConfigBuilder newBuilder() {
        return new AfloatDBConfigBuilder();
    }

    @Nonnull
    public Config getConfig() {
        return config;
    }

    @Nonnull
    public AfloatDBEndpointConfig getLocalEndpointConfig() {
        return localEndpointConfig;
    }

    @Nonnull
    public RaftGroupConfig getRaftGroupConfig() {
        return raftGroupConfig;
    }

    @Nonnull
    public RaftConfig getRaftConfig() {
        return raftConfig;
    }

    @Nonnull
    public RpcConfig getRpcConfig() {
        return rpcConfig;
    }

    @Nonnull
    public PersistenceConfig getPersistenceConfig() {
        return persistenceConfig;
    }

    @Override
    public String toString() {
        return "AfloatDBConfig{" + "config=" + config + ", localEndpointConfig=" + localEndpointConfig
                + ", raftGroupConfig=" + raftGroupConfig + ", raftConfig=" + raftConfig + ", rpcConfig=" + rpcConfig
                + ", persistenceConfig=" + persistenceConfig + '}';
    }

    public static class AfloatDBConfigBuilder {

        private AfloatDBConfig afloatDBConfig = new AfloatDBConfig();

        @Nonnull
        public AfloatDBConfigBuilder setConfig(@Nonnull Config config) {
            afloatDBConfig.config = requireNonNull(config);
            return this;
        }

        @Nonnull
        public AfloatDBConfigBuilder setLocalEndpointConfig(@Nonnull AfloatDBEndpointConfig localEndpointConfig) {
            afloatDBConfig.localEndpointConfig = requireNonNull(localEndpointConfig);
            return this;
        }

        @Nonnull
        public AfloatDBConfigBuilder setRaftGroupConfig(@Nonnull RaftGroupConfig raftGroupConfig) {
            afloatDBConfig.raftGroupConfig = requireNonNull(raftGroupConfig);
            return this;
        }

        @Nonnull
        public AfloatDBConfigBuilder setRaftConfig(@Nonnull RaftConfig raftConfig) {
            afloatDBConfig.raftConfig = requireNonNull(raftConfig);
            return this;
        }

        @Nonnull
        public AfloatDBConfigBuilder setRpcConfig(@Nonnull RpcConfig rpcConfig) {
            afloatDBConfig.rpcConfig = requireNonNull(rpcConfig);
            return this;
        }

        @Nonnull
        public AfloatDBConfigBuilder setPersistenceConfig(@Nonnull PersistenceConfig persistenceConfig) {
            afloatDBConfig.persistenceConfig = requireNonNull(persistenceConfig);
            return this;
        }

        @Nonnull
        public AfloatDBConfig build() {
            if (afloatDBConfig == null) {
                throw new AfloatDBException("AfloatDBConfig already built!");
            }

            if (afloatDBConfig.config == null) {
                try {
                    afloatDBConfig.config = ConfigFactory.load();
                } catch (Exception e) {
                    throw new AfloatDBException("Could not load Config!", e);
                }
            }

            try {
                if (afloatDBConfig.config.hasPath("afloatdb")) {
                    Config config = afloatDBConfig.config.getConfig("afloatdb");

                    if (afloatDBConfig.localEndpointConfig == null && config.hasPath("local-endpoint")) {
                        afloatDBConfig.localEndpointConfig = AfloatDBEndpointConfig
                                .from(config.getConfig("local" + "-endpoint"));
                    }

                    if (afloatDBConfig.raftGroupConfig == null && config.hasPath("group")) {
                        afloatDBConfig.raftGroupConfig = RaftGroupConfig.from(config.getConfig("group"));
                    }

                    if (afloatDBConfig.raftConfig == null) {
                        afloatDBConfig.raftConfig = config.hasPath("raft") ? parseConfig(config) : DEFAULT_RAFT_CONFIG;
                    }

                    if (afloatDBConfig.rpcConfig == null && config.hasPath("rpc")) {
                        afloatDBConfig.rpcConfig = RpcConfig.from(config.getConfig("rpc"));
                    }

                    if (afloatDBConfig.persistenceConfig == null && config.hasPath("persistence")) {
                        afloatDBConfig.persistenceConfig = PersistenceConfig.from(config.getConfig("persistence"));
                    }
                }
            } catch (Exception e) {
                if (e instanceof AfloatDBException) {
                    throw (AfloatDBException) e;
                }

                throw new AfloatDBException("Could not build AfloatDBConfig!", e);
            }

            if (afloatDBConfig.localEndpointConfig == null) {
                throw new AfloatDBException("Local endpoint config is missing!");
            }

            if (afloatDBConfig.raftGroupConfig == null) {
                throw new AfloatDBException("Raft group config is missing!");
            }

            if (afloatDBConfig.raftConfig == null) {
                throw new AfloatDBException("Raft config is missing!");
            }

            if (afloatDBConfig.rpcConfig == null) {
                afloatDBConfig.rpcConfig = RpcConfig.newBuilder().build();
            }

            if (afloatDBConfig.persistenceConfig == null) {
                throw new AfloatDBException("Persistence config is missing!");
            }

            AfloatDBConfig afloatDBConfig = this.afloatDBConfig;
            this.afloatDBConfig = null;
            return afloatDBConfig;
        }

    }

}
