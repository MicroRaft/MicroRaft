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

package io.afloatdb.config;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import io.afloatdb.AfloatDBException;
import io.microraft.test.util.BaseTest;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class AfloatDBEndpointConfigTest extends BaseTest {

    @Test(expected = AfloatDBException.class)
    public void when_emptyConfigStringProvided_then_shouldNotCreateConfig() {
        Config config = ConfigFactory.parseString("");

        AfloatDBEndpointConfig.from(config);
    }

    @Test(expected = AfloatDBException.class)
    public void when_addressIsMissingInConfigString_then_shouldNotCreateConfig() {
        Config config = ConfigFactory.parseString("id: \"node1\"");

        AfloatDBEndpointConfig.from(config);
    }

    @Test(expected = AfloatDBException.class)
    public void when_idIsMissingInConfigString_then_shouldNotCreateConfig() {
        Config config = ConfigFactory.parseString("address: \"localhost:6767\"");

        AfloatDBEndpointConfig.from(config);
    }

    @Test
    public void when_configStringContainsIdAndAddress_then_shouldCreateConfig() {
        Config config = ConfigFactory.parseString("id: \"node1\"\naddress: \"localhost:6767\"");

        AfloatDBEndpointConfig endpointConfig = AfloatDBEndpointConfig.from(config);

        assertThat(endpointConfig.getId()).isEqualTo("node1");
        assertThat(endpointConfig.getAddress()).isEqualTo("localhost:6767");
    }

    @Test(expected = AfloatDBException.class)
    public void when_builderIsEmpty_then_shouldNotCreateConfig() {
        AfloatDBEndpointConfig.newBuilder().build();
    }

    @Test(expected = AfloatDBException.class)
    public void when_addressIsMissingInBuilder_then_shouldNotCreateConfig() {
        AfloatDBEndpointConfig.newBuilder().setId("node1").build();
    }

    @Test(expected = AfloatDBException.class)
    public void when_idIsMissingInBuilder_then_shouldNotCreateConfig() {
        AfloatDBEndpointConfig.newBuilder().setAddress("localhost:6767").build();
    }

    @Test
    public void when_builderContainsIdAndAddress_then_shouldCreateConfig() {
        AfloatDBEndpointConfig endpointConfig = AfloatDBEndpointConfig.newBuilder().setId("node1")
                .setAddress("localhost:6767").build();

        assertThat(endpointConfig.getId()).isEqualTo("node1");
        assertThat(endpointConfig.getAddress()).isEqualTo("localhost:6767");
    }

    @Test(expected = AfloatDBException.class)
    public void when_configIsAlreadyCreated_then_shouldNotCreateConfig() {
        AfloatDBEndpointConfig.AfloatDBEndpointConfigBuilder builder = AfloatDBEndpointConfig.newBuilder();
        builder.setId("node1").setAddress("localhost:6767").build();

        builder.build();
    }

}
