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

package io.microraft.afloatdb.utils;

import io.microraft.afloatdb.AfloatDB;
import io.microraft.afloatdb.config.AfloatDBConfig;
import io.microraft.afloatdb.internal.AfloatDBImpl;
import io.microraft.RaftEndpoint;
import io.microraft.RaftNode;
import io.microraft.report.RaftGroupMembers;

import java.util.List;

import static com.typesafe.config.ConfigFactory.load;
import static io.microraft.test.util.AssertionUtils.eventually;
import static java.util.stream.Collectors.toList;
import static org.assertj.core.api.Assertions.assertThat;

public final class AfloatDBTestUtils {

    public static final AfloatDBConfig CONFIG_1 = AfloatDBConfig.from(load("node1.conf"));
    public static final AfloatDBConfig CONFIG_2 = AfloatDBConfig.from(load("node2.conf"));
    public static final AfloatDBConfig CONFIG_3 = AfloatDBConfig.from(load("node3.conf"));

    private AfloatDBTestUtils() {
    }

    public static AfloatDB getAnyFollower(List<AfloatDB> servers) {
        RaftEndpoint leaderEndpoint = getLeaderEndpoint(servers);
        if (leaderEndpoint == null) {
            throw new AssertionError("Group doesn't have a leader yet!");
        }

        for (AfloatDB server : servers) {
            if (!server.isShutdown() && !leaderEndpoint.equals(server.getLocalEndpoint())) {
                return server;
            }
        }

        throw new AssertionError("No follower server available!");
    }

    public static RaftEndpoint getLeaderEndpoint(List<AfloatDB> servers) {
        RaftEndpoint leader = null;
        for (AfloatDB server : servers) {
            if (server.isShutdown()) {
                continue;
            }

            RaftEndpoint leaderEndpoint = getLeaderEndpoint(server);
            if (leader == null) {
                leader = leaderEndpoint;
            } else if (!leader.equals(leaderEndpoint)) {
                throw new AssertionError("Raft group doesn't have a single leader endpoint yet!");
            }
        }

        return leader;
    }

    public static RaftEndpoint getLeaderEndpoint(AfloatDB server) {
        return ((AfloatDBImpl) server).getRaftNode().getTerm().getLeaderEndpoint();
    }

    public static List<AfloatDB> getFollowers(List<AfloatDB> servers) {
        AfloatDB leader = waitUntilLeaderElected(servers);
        return servers.stream().filter(server -> server != leader).collect(toList());
    }

    public static AfloatDB waitUntilLeaderElected(List<AfloatDB> servers) {
        AfloatDB[] leaderRef = new AfloatDB[1];
        eventually(() -> {
            AfloatDB leaderServer = getLeader(servers);
            assertThat(leaderServer).isNotNull();

            int leaderTerm = getTerm(leaderServer);

            for (AfloatDB server : servers) {
                if (server.isShutdown()) {
                    continue;
                }

                RaftEndpoint leader = getLeaderEndpoint(server);
                assertThat(leader).isEqualTo(leaderServer.getLocalEndpoint());
                assertThat(getTerm(server)).isEqualTo(leaderTerm);
            }

            leaderRef[0] = leaderServer;
        });

        return leaderRef[0];
    }

    public static AfloatDB getLeader(List<AfloatDB> servers) {
        RaftEndpoint leaderEndpoint = getLeaderEndpoint(servers);
        if (leaderEndpoint == null) {
            return null;
        }

        for (AfloatDB server : servers) {
            if (!server.isShutdown() && leaderEndpoint.equals(server.getLocalEndpoint())) {
                return server;
            }
        }

        throw new AssertionError("Leader endpoint is " + leaderEndpoint + ", but leader server could not be found!");
    }

    public static int getTerm(AfloatDB server) {
        return ((AfloatDBImpl) server).getRaftNode().getTerm().getTerm();
    }

    public static RaftNode getRaftNode(AfloatDB server) {
        return ((AfloatDBImpl) server).getRaftNode();
    }

    public static RaftGroupMembers getRaftGroupMembers(AfloatDB server) {
        return ((AfloatDBImpl) server).getRaftNode().getCommittedMembers();
    }

}
