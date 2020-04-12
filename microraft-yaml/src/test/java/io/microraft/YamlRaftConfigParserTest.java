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

import io.microraft.test.util.BaseTest;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.yaml.snakeyaml.Yaml;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.StringReader;

import static org.assertj.core.api.Assertions.assertThat;

public class YamlRaftConfigParserTest
        extends BaseTest {

    @Rule
    public final TemporaryFolder folder = new TemporaryFolder();

    private final String yamlString = "raft:\n" + " leader-election-timeout-millis: 750\n" + " leader-heartbeat-period-secs: 15\n"
            + " leader-heartbeat-timeout-secs: 45\n" + " append-entries-request-batch-size: 750\n"
            + " commit-count-to-take-snapshot: 7500\n" + " max-uncommitted-log-entry-count: 1500\n"
            + " transfer-snapshots-from-followers-enabled: false\n" + " raft-node-report-publish-period-secs: 20";

    @Test
    public void test_parseValidYamlString() {
        RaftConfig config = YamlRaftConfigParser.parseString(new Yaml(), yamlString);

        assertConfig(config);
    }

    @Test
    public void test_parseValidYamlReader() {
        RaftConfig config = YamlRaftConfigParser.parseReader(new Yaml(), new StringReader(yamlString));

        assertConfig(config);
    }

    @Test
    public void test_parseValidYamlInputReader() {
        RaftConfig config = YamlRaftConfigParser.parseInputStream(new Yaml(), new ByteArrayInputStream(yamlString.getBytes()));

        assertConfig(config);
    }

    @Test
    public void test_parseValidFile()
            throws IOException {
        File file = folder.newFile();
        FileWriter writer = new FileWriter(file);
        writer.write(yamlString);
        writer.close();

        RaftConfig config = YamlRaftConfigParser.parseFile(new Yaml(), file);

        assertConfig(config);
    }

    @Test
    public void test_parseValidFileName()
            throws IOException {
        File file = folder.newFile();
        FileWriter writer = new FileWriter(file);
        writer.write(yamlString);
        writer.close();

        RaftConfig config = YamlRaftConfigParser.parseFile(new Yaml(), file.getPath());

        assertConfig(config);
    }

    private void assertConfig(RaftConfig config) {
        assertThat(config.getLeaderElectionTimeoutMillis()).isEqualTo(750L);
        assertThat(config.getLeaderHeartbeatPeriodSecs()).isEqualTo(15L);
        assertThat(config.getLeaderHeartbeatTimeoutSecs()).isEqualTo(45L);
        assertThat(config.getAppendEntriesRequestBatchSize()).isEqualTo(750);
        assertThat(config.getCommitCountToTakeSnapshot()).isEqualTo(7500);
        assertThat(config.getMaxUncommittedLogEntryCount()).isEqualTo(1500);
        assertThat(config.isTransferSnapshotsFromFollowersEnabled()).isFalse();
        assertThat(config.getRaftNodeReportPublishPeriodSecs()).isEqualTo(20);
    }

}
