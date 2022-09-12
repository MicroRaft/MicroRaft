package io.afloatdb.client;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import io.afloatdb.AfloatDB;
import io.afloatdb.client.config.AfloatDBClientConfig;
import io.afloatdb.client.kv.KV;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.microraft.RaftNode;
import io.microraft.test.util.BaseTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.typesafe.config.ConfigFactory.load;
import static io.afloatdb.utils.AfloatDBTestUtils.CONFIG_1;
import static io.afloatdb.utils.AfloatDBTestUtils.CONFIG_2;
import static io.afloatdb.utils.AfloatDBTestUtils.CONFIG_3;
import static io.afloatdb.utils.AfloatDBTestUtils.getAnyFollower;
import static io.afloatdb.utils.AfloatDBTestUtils.getRaftNode;
import static io.afloatdb.utils.AfloatDBTestUtils.waitUntilLeaderElected;
import static io.microraft.impl.util.RandomPicker.getRandomInt;
import static io.microraft.test.util.AssertionUtils.eventually;
import static io.microraft.test.util.AssertionUtils.sleepMillis;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

public class LeaderServerChangeTest extends BaseTest {

    private static List<AfloatDB> servers = new ArrayList<>();
    private static AfloatDBClient client;
    private static KV kv;

    @Before
    public void init() {
        servers.add(AfloatDB.bootstrap(CONFIG_1));
        servers.add(AfloatDB.bootstrap(CONFIG_2));
        servers.add(AfloatDB.bootstrap(CONFIG_3));

        String serverAddress = servers.get(getRandomInt(servers.size())).getConfig().getLocalEndpointConfig()
                .getAddress();
        Config config = ConfigFactory.parseString("afloatdb.client.server-address: \"" + serverAddress + "\"")
                .withFallback(load("client.conf"));
        client = AfloatDBClient.newInstance(AfloatDBClientConfig.from(config));
        kv = client.getKV();
    }

    @After
    public void tearDown() {
        servers.forEach(AfloatDB::shutdown);
        if (client != null) {
            client.shutdown();
        }
    }

    @Test(timeout = 300_000)
    public void when_leaderFails_then_kvCallsSucceedAfterNewLeaderElected() {
        AfloatDB leader = waitUntilLeaderElected(servers);
        leader.shutdown();

        eventually(() -> {
            try {
                kv.put("key", "val");
            } catch (Throwable t) {
                fail(t.getMessage());
                sleepMillis(100);
            }
        });
    }

    @Test(timeout = 300_000)
    public void when_clusterMovesToNewTermAndLeader_then_kvCallsSucceed() {
        AfloatDB leader = waitUntilLeaderElected(servers);
        RaftNode leaderRaftNode = getRaftNode(leader);
        AfloatDB follower = getAnyFollower(servers);

        CountDownLatch startLatch = new CountDownLatch(1);
        AtomicBoolean transferDone = new AtomicBoolean(false);

        Thread thread = new Thread(() -> {
            try {
                startLatch.await();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            try {
                leaderRaftNode.transferLeadership(follower.getLocalEndpoint()).join();
                transferDone.set(true);
            } catch (Exception e) {
                e.printStackTrace();
            }
        });

        thread.start();

        while (!transferDone.get()) {
            try {
                kv.put("key", "val");
            } catch (StatusRuntimeException e) {
                assertThat(e.getStatus().getCode()).isEqualTo(Status.RESOURCE_EXHAUSTED.getCode());
            }

            sleepMillis(10);
            startLatch.countDown();
        }
    }

}
