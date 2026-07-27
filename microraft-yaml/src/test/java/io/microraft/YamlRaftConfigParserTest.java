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

import static io.microraft.test.util.ParserTestUtils.assertConfigException;
import static io.microraft.test.util.ParserTestUtils.assertLongTimeoutConfig;
import static io.microraft.test.util.ParserTestUtils.assertMaxIntConfigValue;
import static io.microraft.test.util.ParserTestUtils.assertSampleConfig;
import static io.microraft.test.util.ParserTestUtils.createTempFile;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.StringReader;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import java.nio.file.Path;
import org.yaml.snakeyaml.Yaml;

import io.microraft.test.util.BaseTest;

public class YamlRaftConfigParserTest extends BaseTest {

    @TempDir
    public Path folder;

    private final String yamlString = "raft:\n" + " leader-election-timeout-millis: 750\n"
            + " leader-heartbeat-period-secs: 15\n" + " leader-heartbeat-timeout-secs: 45\n"
            + " append-entries-request-batch-size: 750\n" + " commit-count-to-take-snapshot: 7500\n"
            + " max-pending-log-entry-count: 1500\n" + " transfer-snapshots-from-followers-enabled: false\n"
            + " raft-node-report-publish-period-secs: 20";

    @Test
    public void parseString_whenValidYamlProvided_returnsConfig() {
        RaftConfig config = YamlRaftConfigParser.parseString(new Yaml(), yamlString);

        assertSampleConfig(config);
    }

    @Test
    public void parseString_whenLongTimeoutProvided_returnsConfig() {
        String yamlWithLongTimeout = "raft:\n" + " leader-election-timeout-millis: 5000000000\n"
                + " leader-heartbeat-period-secs: 15\n" + " leader-heartbeat-timeout-secs: 45\n";

        RaftConfig config = YamlRaftConfigParser.parseString(new Yaml(), yamlWithLongTimeout);

        assertLongTimeoutConfig(config);
    }

    @Test
    public void parseString_whenMaxIntProvidedForIntField_returnsConfig() {
        String yamlWithLongTypedInt = "raft:\n" + " append-entries-request-batch-size: 2147483647\n";

        RaftConfig config = YamlRaftConfigParser.parseString(new Yaml(), yamlWithLongTypedInt);

        assertMaxIntConfigValue(config.getAppendEntriesRequestBatchSize());
    }

    @Test
    public void parseString_whenOutOfRangeIntProvided_throwsIllegalArgumentException() {
        String yamlWithOutOfRangeInt = "raft:\n" + " append-entries-request-batch-size: 2147483648\n";

        assertConfigException(() -> YamlRaftConfigParser.parseString(new Yaml(), yamlWithOutOfRangeInt),
                IllegalArgumentException.class,
                "Config field 'append-entries-request-batch-size' must be an integer in the int range: 2147483648");
    }

    @Test
    public void parseString_whenFractionalLongProvided_throwsIllegalArgumentException() {
        String yamlWithFractionalLong = "raft:\n" + " leader-election-timeout-millis: 1.5\n";

        assertConfigException(() -> YamlRaftConfigParser.parseString(new Yaml(), yamlWithFractionalLong),
                IllegalArgumentException.class,
                "Config field 'leader-election-timeout-millis' must be an integer in the long range: 1.5");
    }

    @Test
    public void parseString_whenFractionalIntProvided_throwsIllegalArgumentException() {
        String yamlWithFractionalInt = "raft:\n" + " append-entries-request-batch-size: 1.5\n";

        assertConfigException(() -> YamlRaftConfigParser.parseString(new Yaml(), yamlWithFractionalInt),
                IllegalArgumentException.class,
                "Config field 'append-entries-request-batch-size' must be an integer in the int range: 1.5");
    }

    @Test
    public void parseString_whenRootIsNotMap_throwsIllegalArgumentException() {
        assertConfigException(() -> YamlRaftConfigParser.parseString(new Yaml(), "- not-a-map"),
                IllegalArgumentException.class,
                "YAML root must be a map!");
    }

    @Test
    public void parseString_whenRaftSectionIsNotMap_throwsIllegalArgumentException() {
        assertConfigException(() -> YamlRaftConfigParser.parseString(new Yaml(), "raft: 1"),
                IllegalArgumentException.class,
                "RaftConfig must be a map!");
    }

    @Test
    public void parseReader_whenValidYamlProvided_returnsConfig() {
        RaftConfig config = YamlRaftConfigParser.parseReader(new Yaml(), new StringReader(yamlString));

        assertSampleConfig(config);
    }

    @Test
    public void parseInputStream_whenValidYamlProvided_returnsConfig() {
        RaftConfig config = YamlRaftConfigParser.parseInputStream(new Yaml(),
                new ByteArrayInputStream(yamlString.getBytes()));

        assertSampleConfig(config);
    }

    @Test
    public void parseFile_whenValidYamlFileProvided_returnsConfig() throws IOException {
        File file = createTempFile(folder, yamlString);

        RaftConfig config = YamlRaftConfigParser.parseFile(new Yaml(), file);

        assertSampleConfig(config);
    }

    @Test
    public void parseFile_whenValidYamlFilePathProvided_returnsConfig() throws IOException {
        File file = createTempFile(folder, yamlString);

        RaftConfig config = YamlRaftConfigParser.parseFile(new Yaml(), file.getPath());

        assertSampleConfig(config);
    }

}
