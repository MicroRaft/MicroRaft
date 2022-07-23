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

package io.microraft.metrics;

import io.micrometer.core.instrument.Clock;
import io.micrometer.core.instrument.logging.LoggingMeterRegistry;
import io.micrometer.core.instrument.logging.LoggingRegistryConfig;
import io.microraft.RaftConfig;
import io.microraft.RaftEndpoint;
import io.microraft.RaftNode;
import io.microraft.RaftNodeStatus;
import io.microraft.impl.local.LocalRaftEndpoint;
import io.microraft.impl.local.LocalTransport;
import io.microraft.impl.local.SimpleStateMachine;
import io.microraft.report.RaftTerm;
import io.microraft.statemachine.StateMachine;
import io.microraft.test.util.BaseTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static io.microraft.test.util.AssertionUtils.sleepMillis;

public class RaftNodeMetricsTest extends BaseTest {

    List<RaftEndpoint> initialMembers = Arrays.asList(LocalRaftEndpoint.newEndpoint(), LocalRaftEndpoint.newEndpoint(),
            LocalRaftEndpoint.newEndpoint());
    List<LocalTransport> transports = new ArrayList<>();
    List<RaftNode> raftNodes = new ArrayList<>();
    List<RaftNodeMetrics> metricsList = new ArrayList<>();

    @Before
    public void init() {
        initialMembers.forEach(this::createRaftNode);
    }

    @After
    public void tearDown() {
        raftNodes.forEach(RaftNode::terminate);
    }

    @Ignore
    @Test
    public void test() {
        LoggingMeterRegistry registry = new LoggingMeterRegistry(new LoggingRegistryConfig() {
            @Override
            public Duration step() {
                return Duration.ofSeconds(5);
            }

            @Override
            public String get(String key) {
                return null;
            }
        }, Clock.SYSTEM);

        metricsList.forEach(metrics -> metrics.bindTo(registry));

        raftNodes.forEach(RaftNode::start);

        RaftNode leader = waitUntilLeaderElected();

        long start = System.currentTimeMillis();
        long duration = 60_000;
        while (System.currentTimeMillis() - start < duration) {
            leader.replicate(SimpleStateMachine.applyValue("val")).join();
            sleepMillis(1);
        }
    }

    private void createRaftNode(RaftEndpoint endpoint) {
        RaftConfig config = RaftConfig.newBuilder().setRaftNodeReportPublishPeriodSecs(1)
                .setCommitCountToTakeSnapshot(5000).build();
        LocalTransport transport = new LocalTransport(endpoint);
        StateMachine stateMachine = new SimpleStateMachine();
        RaftNodeMetrics metrics = new RaftNodeMetrics("default", endpoint.getId().toString());
        RaftNode raftNode = RaftNode.newBuilder().setGroupId("default").setLocalEndpoint(endpoint)
                .setInitialGroupMembers(initialMembers).setConfig(config).setTransport(transport)
                .setStateMachine(stateMachine).setRaftNodeReportListener(metrics).build();

        raftNodes.add(raftNode);
        transports.add(transport);
        metricsList.add(metrics);
        enableDiscovery(raftNode, transport);
    }

    private void enableDiscovery(RaftNode raftNode, LocalTransport transport) {
        for (int i = 0; i < raftNodes.size(); i++) {
            RaftNode otherNode = raftNodes.get(i);
            if (otherNode != raftNode) {
                transports.get(i).discoverNode(raftNode);
                transport.discoverNode(otherNode);
            }
        }
    }

    protected final RaftNode waitUntilLeaderElected() {
        long deadline = System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(60);
        while (System.currentTimeMillis() < deadline) {
            RaftEndpoint leaderEndpoint = getLeaderEndpoint();
            if (leaderEndpoint != null) {
                return raftNodes.stream().filter(node -> node.getLocalEndpoint().equals(leaderEndpoint)).findFirst()
                        .orElseThrow(IllegalStateException::new);
            }

            sleepMillis(100);
        }

        throw new AssertionError("Could not elect a leader on time!");
    }

    private RaftEndpoint getLeaderEndpoint() {
        RaftEndpoint leaderEndpoint = null;
        int leaderTerm = 0;
        for (RaftNode raftNode : raftNodes) {
            if (raftNode.getStatus() == RaftNodeStatus.TERMINATED) {
                continue;
            }

            RaftTerm term = raftNode.getTerm();
            if (term.getLeaderEndpoint() != null) {
                if (leaderEndpoint == null) {
                    leaderEndpoint = term.getLeaderEndpoint();
                    leaderTerm = term.getTerm();
                } else if (!(leaderEndpoint.equals(term.getLeaderEndpoint()) && leaderTerm == term.getTerm())) {
                    leaderEndpoint = null;
                    break;
                }
            } else {
                leaderEndpoint = null;
                break;
            }
        }

        return leaderEndpoint;
    }

}
