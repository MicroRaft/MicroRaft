package io.afloatdb.config;

import com.typesafe.config.Config;
import io.afloatdb.AfloatDBException;

import javax.annotation.Nonnull;

import static java.util.Objects.requireNonNull;

public class RpcConfig {

    public static final int DEFAULT_RPC_TIMEOUT_SECONDS = 10;
    public static final int DEFAULT_RETRY_LIMIT = 20;

    private long rpcTimeoutSecs = DEFAULT_RPC_TIMEOUT_SECONDS;

    private RpcConfig() {
    }

    public static RpcConfig from(@Nonnull Config config) {
        requireNonNull(config);
        try {
            RpcConfigBuilder builder = new RpcConfigBuilder();

            if (config.hasPath("rpc-timeout-secs")) {
                builder.setRpcTimeoutSecs(config.getInt("rpc-timeout-secs"));
            }

            return builder.build();
        } catch (Exception e) {
            throw new AfloatDBException("Invalid configuration: " + config, e);
        }
    }

    public static RpcConfigBuilder newBuilder() {
        return new RpcConfigBuilder();
    }

    public long getRpcTimeoutSecs() {
        return rpcTimeoutSecs;
    }

    @Override
    public String toString() {
        return "RpcConfig{" + "rpcTimeoutSecs=" + rpcTimeoutSecs + '}';
    }

    public static class RpcConfigBuilder {

        private RpcConfig config = new RpcConfig();

        private RpcConfigBuilder() {
        }

        public RpcConfigBuilder setRpcTimeoutSecs(long rpcTimeoutSecs) {
            if (rpcTimeoutSecs < 1) {
                throw new IllegalArgumentException(
                        "Rpc timeout seconds: " + rpcTimeoutSecs + " cannot be non-positive!");
            }

            config.rpcTimeoutSecs = rpcTimeoutSecs;
            return this;
        }

        public RpcConfig build() {
            RpcConfig config = this.config;
            this.config = null;
            return config;
        }

    }

}
