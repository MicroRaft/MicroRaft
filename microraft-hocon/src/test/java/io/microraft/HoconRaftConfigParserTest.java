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

package io.microraft;

import static io.microraft.HoconRaftConfigParser.parseConfig;
import static io.microraft.test.util.ParserTestUtils.assertConfigException;
import static io.microraft.test.util.ParserTestUtils.assertLongTimeoutConfig;
import static io.microraft.test.util.ParserTestUtils.assertSampleConfig;

import org.junit.jupiter.api.Test;

import com.typesafe.config.ConfigFactory;

import io.microraft.test.util.BaseTest;

public class HoconRaftConfigParserTest extends BaseTest {

    @Test
    public void parseConfig_whenValidHoconProvided_returnsConfig() {
        StringBuilder sb = new StringBuilder();
        sb.append("raft {").append(System.lineSeparator())
          .append("  leader-election-timeout-millis: 750").append(System.lineSeparator())
          .append("  leader-heartbeat-period-secs: 15").append(System.lineSeparator())
          .append("  leader-heartbeat-timeout-secs: 45").append(System.lineSeparator())
          .append("  append-entries-request-batch-size: 750").append(System.lineSeparator())
          .append("  commit-count-to-take-snapshot: 7500").append(System.lineSeparator())
          .append("  max-pending-log-entry-count: 1500").append(System.lineSeparator())
          .append("  transfer-snapshots-from-followers-enabled: false").append(System.lineSeparator())
          .append("  raft-node-report-publish-period-secs: 20").append(System.lineSeparator())
          .append("}").append(System.lineSeparator());
        String configString = sb.toString();
        RaftConfig config = parseConfig(ConfigFactory.parseString(configString));

        assertSampleConfig(config);
    }

    @Test
    public void parseConfig_whenLongTimeoutProvided_returnsConfig() {
        StringBuilder sb = new StringBuilder();
        sb.append("raft {").append(System.lineSeparator())
          .append("  leader-election-timeout-millis: 5000000000").append(System.lineSeparator())
          .append("  leader-heartbeat-period-secs: 15").append(System.lineSeparator())
          .append("  leader-heartbeat-timeout-secs: 45").append(System.lineSeparator())
          .append("}").append(System.lineSeparator());
        String configString = sb.toString();
        RaftConfig config = parseConfig(ConfigFactory.parseString(configString));

        assertLongTimeoutConfig(config);
    }

    @Test
    public void parseConfig_whenFractionalLongProvided_throwsIllegalArgumentException() {
        StringBuilder sb = new StringBuilder();
        sb.append("raft {").append(System.lineSeparator())
          .append("  leader-election-timeout-millis: 1.5").append(System.lineSeparator())
          .append("}").append(System.lineSeparator());
        String configString = sb.toString();
        assertConfigException(() -> parseConfig(ConfigFactory.parseString(configString)), IllegalArgumentException.class,
                "Config field 'raft.leader-election-timeout-millis' must be an integer in the long range: 1.5");
    }

    @Test
    public void parseConfig_whenOutOfRangeIntProvided_throwsIllegalArgumentException() {
        StringBuilder sb = new StringBuilder();
        sb.append("raft {").append(System.lineSeparator())
          .append("  append-entries-request-batch-size: 2147483648").append(System.lineSeparator())
          .append("}").append(System.lineSeparator());
        String configString = sb.toString();
        assertConfigException(() -> parseConfig(ConfigFactory.parseString(configString)), IllegalArgumentException.class,
                "Config field 'raft.append-entries-request-batch-size' must be an integer in the int range: 2147483648");
    }

    @Test
    public void parseConfig_whenRaftSectionMissing_throwsIllegalArgumentException() {
        assertConfigException(() -> parseConfig(ConfigFactory.parseString("")), IllegalArgumentException.class,
                "No raft config provided!");
    }

    @Test
    public void parseConfig_whenConfigIsNull_throwsNullPointerException() {
        assertConfigException(() -> parseConfig(null), NullPointerException.class, null);
    }

}


