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

import com.typesafe.config.ConfigFactory;
import io.microraft.test.util.BaseTest;
import org.junit.Test;

import static io.microraft.HoconRaftConfigParser.parseConfig;
import static org.assertj.core.api.Assertions.assertThat;

public class HoconRaftConfigParserTest
        extends BaseTest {

    @Test
    public void test_parseValidHoconString() {
        String configString = "raft {\n" + "  leader-election-timeout-millis: 750\n" + "  leader-heartbeat-period-secs: 15\n"
                + "  leader-heartbeat-timeout-secs: 45\n" + "  append-entries-request-batch-size: 750\n"
                + "  commit-count-to-take-snapshot: 7500\n" + "  max-uncommitted-log-entry-count: 1500\n"
                + "  transfer-snapshots-from-followers-enabled: false\n" + "  raft-node-report-publish-period-secs: 20\n" + "}\n";

        RaftConfig config = parseConfig(ConfigFactory.parseString(configString));

        assertThat(config.getLeaderElectionTimeoutMillis()).isEqualTo(750L);
        assertThat(config.getLeaderHeartbeatPeriodSecs()).isEqualTo(15L);
        assertThat(config.getLeaderHeartbeatTimeoutSecs()).isEqualTo(45L);
        assertThat(config.getAppendEntriesRequestBatchSize()).isEqualTo(750);
        assertThat(config.getCommitCountToTakeSnapshot()).isEqualTo(7500);
        assertThat(config.getMaxUncommittedLogEntryCount()).isEqualTo(1500);
        assertThat(config.isTransferSnapshotsFromFollowersEnabled()).isFalse();
        assertThat(config.getRaftNodeReportPublishPeriodSecs()).isEqualTo(20);
    }

    @Test(expected = IllegalArgumentException.class)
    public void test_nonExistingConfig() {
        parseConfig(ConfigFactory.parseString(""));
    }

    @Test(expected = NullPointerException.class)
    public void test_nullConfig() {
        parseConfig(null);
    }

}
