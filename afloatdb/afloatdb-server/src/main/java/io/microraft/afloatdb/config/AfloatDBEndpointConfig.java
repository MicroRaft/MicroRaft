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
import java.net.InetSocketAddress;

import static java.util.Objects.requireNonNull;

public class AfloatDBEndpointConfig {

    private String id;
    private String address;

    private AfloatDBEndpointConfig() {
    }

    public static AfloatDBEndpointConfig from(@Nonnull Config config) {
        requireNonNull(config);
        try {
            return new AfloatDBEndpointConfigBuilder().setId(config.getString("id"))
                    .setAddress(config.getString("address")).build();
        } catch (Exception e) {
            throw new AfloatDBException("Invalid configuration: " + config, e);
        }
    }

    public static AfloatDBEndpointConfigBuilder newBuilder() {
        return new AfloatDBEndpointConfigBuilder();
    }

    @Nonnull
    public String getId() {
        return id;
    }

    @Nonnull
    public String getAddress() {
        return address;
    }

    @Nonnull
    public InetSocketAddress getSocketAddress() {
        String[] addressTokens = address.split(":");
        return new InetSocketAddress(addressTokens[0], Integer.parseInt(addressTokens[1]));
    }

    @Override
    public String toString() {
        return "AfloatDBEndpointConfig{" + "id='" + id + '\'' + ", address='" + address + '\'' + '}';
    }

    public static class AfloatDBEndpointConfigBuilder {
        private AfloatDBEndpointConfig config = new AfloatDBEndpointConfig();

        private AfloatDBEndpointConfigBuilder() {
        }

        @Nonnull
        public AfloatDBEndpointConfigBuilder setId(@Nonnull String id) {
            config.id = requireNonNull(id);
            return this;
        }

        @Nonnull
        public AfloatDBEndpointConfigBuilder setAddress(@Nonnull String address) {
            config.address = requireNonNull(address);
            return this;
        }

        @Nonnull
        public AfloatDBEndpointConfig build() {
            if (config == null) {
                throw new AfloatDBException("AfloatDBEndpointConfig already built!");
            }

            if (config.id == null) {
                throw new AfloatDBException("Cannot build AfloatDBEndpointConfig without unique id!");
            }

            if (config.address == null) {
                throw new AfloatDBException("Cannot build AfloatDBEndpointConfig without address!");
            }

            AfloatDBEndpointConfig config = this.config;
            this.config = null;
            return config;
        }

    }

}
