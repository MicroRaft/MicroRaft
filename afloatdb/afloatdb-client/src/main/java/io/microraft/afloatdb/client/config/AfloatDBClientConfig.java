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

package io.microraft.afloatdb.client.config;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import io.microraft.afloatdb.client.AfloatDBClientException;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.UUID;

import static java.util.Objects.requireNonNull;

public class AfloatDBClientConfig {

    public static final boolean DEFAULT_SINGLE_CONNECTION = false;
    public static final int DEFAULT_RPC_TIMEOUT_SECS = 10;

    private Config config;
    private String clientId;
    private String serverAddress;
    private boolean singleConnection;
    private int rpcTimeoutSecs;

    private AfloatDBClientConfig() {
    }

    public static AfloatDBClientConfig from(Config config) {
        return newBuilder().setConfig(config).build();
    }

    public static AfloatDBClientConfigBuilder newBuilder() {
        return new AfloatDBClientConfigBuilder();
    }

    @Nonnull
    public Config getConfig() {
        return config;
    }

    @Nullable
    public String getClientId() {
        return clientId;
    }

    public String getServerAddress() {
        return serverAddress;
    }

    public int getRpcTimeoutSecs() {
        return rpcTimeoutSecs;
    }

    @Override
    public String toString() {
        return "AfloatDBClientConfig{" + "config=" + config + ", clientId='" + clientId + '\'' + ", serverAddress='"
                + serverAddress + '\'' + ", singleConnection=" + singleConnection + ", rpcTimeoutSecs=" + rpcTimeoutSecs
                + '}';
    }

    public boolean isSingleConnection() {
        return singleConnection;
    }

    public static class AfloatDBClientConfigBuilder {

        private AfloatDBClientConfig clientConfig = new AfloatDBClientConfig();
        private Boolean singleConnection;
        private Integer rpcTimeoutSecs;

        public AfloatDBClientConfigBuilder setConfig(@Nonnull Config config) {
            clientConfig.config = requireNonNull(config);
            return this;
        }

        public AfloatDBClientConfigBuilder setClientId(@Nonnull String clientId) {
            clientConfig.clientId = requireNonNull(clientId);
            return this;
        }

        public AfloatDBClientConfigBuilder setServerAddress(@Nonnull String serverAddress) {
            clientConfig.serverAddress = requireNonNull(serverAddress);
            return this;
        }

        public AfloatDBClientConfigBuilder setSingleConnection(boolean singleConnection) {
            this.singleConnection = singleConnection;
            return this;
        }

        public AfloatDBClientConfigBuilder setRpcTimeoutSecs(int rpcTimeoutSecs) {
            if (rpcTimeoutSecs < 1) {
                throw new IllegalArgumentException(
                        "Rpc timeout seconds: " + rpcTimeoutSecs + " cannot be non-positive!");
            }
            this.rpcTimeoutSecs = rpcTimeoutSecs;
            return this;
        }

        public AfloatDBClientConfig build() {
            if (clientConfig == null) {
                throw new AfloatDBClientException("AfloatDBClientConfig is already built!");
            }

            if (clientConfig.config == null) {
                try {
                    clientConfig.config = ConfigFactory.load();
                } catch (Exception e) {
                    throw new AfloatDBClientException("Could not load Config!", e);
                }
            }

            try {
                if (clientConfig.config.hasPath("afloatdb.client")) {
                    Config config = clientConfig.config.getConfig("afloatdb.client");

                    if (clientConfig.clientId == null && config.hasPath("id")) {
                        clientConfig.clientId = config.getString("id");
                    }

                    if (clientConfig.serverAddress == null && config.hasPath("server-address")) {
                        clientConfig.serverAddress = config.getString("server-address");
                    }

                    if (singleConnection == null && config.hasPath("single-connection")) {
                        singleConnection = config.getBoolean("single-connection");
                    }

                    if (rpcTimeoutSecs == null && config.hasPath("rpc-timeout-secs")) {
                        rpcTimeoutSecs = config.getInt("rpc-timeout-secs");
                    }
                }
            } catch (Exception e) {
                if (e instanceof AfloatDBClientException) {
                    throw (AfloatDBClientException) e;
                }

                throw new AfloatDBClientException("Could not build AfloatDBClientConfig!", e);
            }

            if (clientConfig.clientId == null) {
                clientConfig.clientId = "Client<" + UUID.randomUUID().toString() + ">";
            }

            if (clientConfig.serverAddress == null) {
                throw new AfloatDBClientException("Server address is missing!");
            }

            if (singleConnection == null) {
                singleConnection = DEFAULT_SINGLE_CONNECTION;
            }
            clientConfig.singleConnection = singleConnection;

            if (rpcTimeoutSecs == null) {
                rpcTimeoutSecs = DEFAULT_RPC_TIMEOUT_SECS;
            }
            clientConfig.rpcTimeoutSecs = rpcTimeoutSecs;

            AfloatDBClientConfig clientConfig = this.clientConfig;
            this.clientConfig = null;
            return clientConfig;
        }

    }

}
