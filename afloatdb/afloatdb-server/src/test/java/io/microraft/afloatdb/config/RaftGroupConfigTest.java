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
import io.microraft.test.util.BaseTest;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Set;

import static java.util.stream.Collectors.toSet;
import static org.assertj.core.api.Assertions.assertThat;

public class RaftGroupConfigTest extends BaseTest {

    @Test(expected = AfloatDBException.class)
    public void when_emptyConfigStringProvided_then_shouldNotCreateConfig() {
        Config config = ConfigFactory.parseString("");

        RaftGroupConfig.from(config);
    }

    @Test(expected = AfloatDBException.class)
    public void when_initialEndpointsAndJoinToMissingInConfigString_then_shouldNotCreateConfig() {
        Config config = ConfigFactory.parseString("id: \"group1\"");

        RaftGroupConfig.from(config);
    }

    @Test(expected = AfloatDBException.class)
    public void when_initialEndpointsAndJoinToPresentInConfigString_then_shouldNotCreateConfig() {
        Config config = ConfigFactory.parseString("id: \"group1\"\ninitial-endpoints: [{id: \"node1\", address: "
                + "\"localhost:6767\"}, {id: \"node2\", address: " + "\"localhost:6768\"}, {id: \"node3\", address: "
                + "\"localhost:6769\"}]\njoin-to:\"localhost:6767\"");

        RaftGroupConfig.from(config);
    }

    @Test
    public void when_initialEndpointsPresent_then_shouldCreateConfig() {
        Config config = ConfigFactory.parseString("id: \"group1\"\ninitial-endpoints: [{id: \"node1\", address: "
                + "\"localhost:6767\"}, {id: \"node2\", address: " + "\"localhost:6768\"}, {id: \"node3\", address: "
                + "\"localhost:6769\"}]");

        RaftGroupConfig groupConfig = RaftGroupConfig.from(config);

        assertThat(groupConfig.getId()).isEqualTo("group1");
        assertThat(groupConfig.getJoinTo()).isNull();
        List<AfloatDBEndpointConfig> initialMembers = groupConfig.getInitialEndpoints();
        assertThat(initialMembers).hasSize(3);

        Set<String> nodeIds = initialMembers.stream().map(AfloatDBEndpointConfig::getId).collect(toSet());
        assertThat(nodeIds).hasSize(3);
        assertThat(nodeIds).contains("node1", "node2", "node3");

        Set<String> addresses = initialMembers.stream().map(AfloatDBEndpointConfig::getAddress).collect(toSet());
        assertThat(addresses).hasSize(3);
        assertThat(addresses).contains("localhost:6767", "localhost:6768", "localhost:6769");
    }

    @Test
    public void when_joinToPresent_then_shouldCreateConfig() {
        Config config = ConfigFactory.parseString("id: \"group1\"\njoin-to:\"localhost:6767\"");

        RaftGroupConfig groupConfig = RaftGroupConfig.from(config);

        assertThat(groupConfig.getId()).isEqualTo("group1");
        assertThat(groupConfig.getJoinTo()).isEqualTo("localhost:6767");
    }

    @Test(expected = AfloatDBException.class)
    public void when_duplicateIdExistsInConfig_then_shouldNotCreateConfig() {
        Config config = ConfigFactory.parseString("id: \"group1\"\ninitial-endpoints: [{id: \"node1\", address: "
                + "\"localhost:6767\"}, {id: \"node2\", address: " + "\"localhost:6768\"}, {id: \"node1\", address: "
                + "\"localhost:6769\"}]");

        RaftGroupConfig.from(config);
    }

    @Test(expected = AfloatDBException.class)
    public void when_duplicateAddressExistsInConfig_then_shouldNotCreateConfig() {
        Config config = ConfigFactory.parseString("id: \"group1\"\ninitial-endpoints: [{id: \"node1\", address: "
                + "\"localhost:6767\"}, {id: \"node2\", address: " + "\"localhost:6768\"}, {id: \"node3\", address: "
                + "\"localhost:6767\"}]");

        RaftGroupConfig.from(config);
    }

    @Test(expected = AfloatDBException.class)
    public void when_builderEmpty_then_shouldNotCreateConfig() {
        RaftGroupConfig.newBuilder().build();
    }

    @Test(expected = AfloatDBException.class)
    public void when_initialEndpointsAndJoinToMissingInBuilder_then_shouldNotCreateConfig() {
        RaftGroupConfig.newBuilder().setId("group1").build();
    }

    @Test
    public void when_joinToPresentInBuilder_then_shouldCreateConfig() {
        RaftGroupConfig groupConfig = RaftGroupConfig.newBuilder().setId("group1").setJoinTo("localhost:6767").build();

        assertThat(groupConfig.getId()).isEqualTo("group1");
        assertThat(groupConfig.getJoinTo()).isEqualTo("localhost:6767");
    }

    @Test
    public void when_initialEndpointsPresentInBuilder_then_shouldCreateConfig() {
        AfloatDBEndpointConfig endpointConfig1 = AfloatDBEndpointConfig.newBuilder().setId("node1")
                .setAddress("localhost" + ":6767").build();
        AfloatDBEndpointConfig endpointConfig2 = AfloatDBEndpointConfig.newBuilder().setId("node2")
                .setAddress("localhost" + ":6768").build();
        AfloatDBEndpointConfig endpointConfig3 = AfloatDBEndpointConfig.newBuilder().setId("node3")
                .setAddress("localhost" + ":6769").build();

        RaftGroupConfig groupConfig = RaftGroupConfig.newBuilder().setId("group1")
                .setInitialEndpoints(Arrays.asList(endpointConfig1, endpointConfig2, endpointConfig3)).build();

        assertThat(groupConfig.getId()).isEqualTo("group1");
        assertThat(groupConfig.getInitialEndpoints()).hasSize(3);
        assertThat(groupConfig.getInitialEndpoints()).contains(endpointConfig1, endpointConfig2, endpointConfig3);
    }

    @Test(expected = AfloatDBException.class)
    public void when_duplicateIdExistsInBuilder_then_shouldNotCreateConfig() {
        AfloatDBEndpointConfig endpointConfig1 = AfloatDBEndpointConfig.newBuilder().setId("node1")
                .setAddress("localhost" + ":6767").build();
        AfloatDBEndpointConfig endpointConfig2 = AfloatDBEndpointConfig.newBuilder().setId("node2")
                .setAddress("localhost" + ":6768").build();
        AfloatDBEndpointConfig endpointConfig3 = AfloatDBEndpointConfig.newBuilder().setId("node1")
                .setAddress("localhost" + ":6769").build();

        RaftGroupConfig.newBuilder().setId("group1")
                .setInitialEndpoints(Arrays.asList(endpointConfig1, endpointConfig2, endpointConfig3)).build();
    }

    @Test(expected = AfloatDBException.class)
    public void when_duplicateAddressExistsInBuilder_then_shouldNotCreateConfig() {
        AfloatDBEndpointConfig endpointConfig1 = AfloatDBEndpointConfig.newBuilder().setId("node1")
                .setAddress("localhost" + ":6767").build();
        AfloatDBEndpointConfig endpointConfig2 = AfloatDBEndpointConfig.newBuilder().setId("node2")
                .setAddress("localhost" + ":6768").build();
        AfloatDBEndpointConfig endpointConfig3 = AfloatDBEndpointConfig.newBuilder().setId("node3")
                .setAddress("localhost" + ":6767").build();

        RaftGroupConfig.newBuilder().setId("group1")
                .setInitialEndpoints(Arrays.asList(endpointConfig1, endpointConfig2, endpointConfig3)).build();
    }

}
