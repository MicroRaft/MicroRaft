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

package io.afloatdb.internal.raft.impl;

import io.afloatdb.cluster.proto.AfloatDBClusterEndpoints;
import io.afloatdb.cluster.proto.AfloatDBClusterEndpointsRequest;
import io.afloatdb.cluster.proto.AfloatDBClusterEndpointsResponse;
import io.afloatdb.cluster.proto.AfloatDBClusterServiceGrpc.AfloatDBClusterServiceImplBase;
import io.afloatdb.config.AfloatDBConfig;
import io.afloatdb.internal.raft.RaftNodeReportSupplier;
import io.afloatdb.internal.rpc.RaftRpcService;
import io.grpc.stub.StreamObserver;
import io.microraft.RaftEndpoint;
import io.microraft.report.RaftGroupMembers;
import io.microraft.report.RaftNodeReport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.PreDestroy;
import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Singleton;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;

import static io.afloatdb.internal.di.AfloatDBModule.CONFIG_KEY;
import static io.afloatdb.internal.di.AfloatDBModule.LOCAL_ENDPOINT_KEY;
import static io.afloatdb.internal.utils.Exceptions.runSilently;
import static io.microraft.report.RaftNodeReport.RaftNodeReportReason.PERIODIC;
import static java.util.concurrent.TimeUnit.SECONDS;

@Singleton
public class AfloatDBClusterEndpointsPublisher extends AfloatDBClusterServiceImplBase
        implements RaftNodeReportSupplier {

    private static final Logger LOGGER = LoggerFactory.getLogger(AfloatDBClusterEndpointsPublisher.class);
    private static final long CLUSTER_ENDPOINTS_IDLE_PUBLISH_DURATION_MILLIS = SECONDS.toMillis(30);

    private final Map<String, StreamObserver<AfloatDBClusterEndpointsResponse>> observers = new ConcurrentHashMap<>();
    private final AfloatDBConfig config;
    private final RaftEndpoint localEndpoint;
    private final RaftRpcService raftRpcService;
    private volatile RaftNodeReport lastReport;
    private long raftNodeReportIdlePublishTimestamp;

    @Inject
    public AfloatDBClusterEndpointsPublisher(@Named(CONFIG_KEY) AfloatDBConfig config,
            @Named(LOCAL_ENDPOINT_KEY) RaftEndpoint localEndpoint, RaftRpcService raftRpcService) {
        this.config = config;
        this.localEndpoint = localEndpoint;
        this.raftRpcService = raftRpcService;
        this.raftNodeReportIdlePublishTimestamp = System.currentTimeMillis()
                - CLUSTER_ENDPOINTS_IDLE_PUBLISH_DURATION_MILLIS;
    }

    @PreDestroy
    public void shutdown() {
        observers.values().forEach(observer -> runSilently(observer::onCompleted));
    }

    @Override
    public void listenClusterEndpoints(AfloatDBClusterEndpointsRequest request,
            StreamObserver<AfloatDBClusterEndpointsResponse> responseObserver) {
        StreamObserver<AfloatDBClusterEndpointsResponse> prev = observers.put(request.getClientId(), responseObserver);
        if (prev != null) {
            LOGGER.warn("{} completing already existing stream observer for {}.", localEndpoint.getId(),
                    request.getClientId());
            runSilently(prev::onCompleted);
        }

        LOGGER.debug("{} registering client: {}.", localEndpoint.getId(), request.getClientId());

        if (lastReport != null) {
            try {
                responseObserver.onNext(createResponse(lastReport));
                LOGGER.debug("{} sent {} to {}.", localEndpoint.getId(), lastReport, request.getClientId());
            } catch (Throwable t) {
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.warn(localEndpoint.getId() + " could not send cluster endpoints to " + request.getClientId(),
                            t);
                } else {
                    LOGGER.warn("{} could not send cluster endpoints to {}. Exception: {} Message: {}",
                            localEndpoint.getId(), request.getClientId(), t.getClass().getSimpleName(), t.getMessage());
                }

                observers.remove(request.getClientId(), responseObserver);
            }
        }
    }

    @Override
    public void accept(@Nonnull RaftNodeReport report) {
        long now = System.currentTimeMillis();
        boolean publishForIdleState = now
                - raftNodeReportIdlePublishTimestamp >= CLUSTER_ENDPOINTS_IDLE_PUBLISH_DURATION_MILLIS;
        if (publishForIdleState) {
            raftNodeReportIdlePublishTimestamp = now;
        }

        if (publishForIdleState || report.getReason() != PERIODIC) {
            publish(report);
        }
    }

    private void publish(RaftNodeReport report) {
        AfloatDBClusterEndpointsResponse response = createResponse(report);
        lastReport = report;
        Iterator<Entry<String, StreamObserver<AfloatDBClusterEndpointsResponse>>> it = observers.entrySet().iterator();
        while (it.hasNext()) {
            Entry<String, StreamObserver<AfloatDBClusterEndpointsResponse>> e = it.next();
            String clientId = e.getKey();
            StreamObserver<AfloatDBClusterEndpointsResponse> observer = e.getValue();
            try {
                LOGGER.debug("{} sending {} to client: {}.", localEndpoint.getId(), report, clientId);
                observer.onNext(response);
            } catch (Throwable t) {
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.warn(localEndpoint.getId() + " could not send cluster endpoints to " + clientId, t);
                } else {
                    LOGGER.warn("{} could not send cluster endpoints to {}. Exception: {} Message: {}",
                            localEndpoint.getId(), clientId, t.getClass().getSimpleName(), t.getMessage());
                }
                it.remove();

                runSilently(observer::onCompleted);
            }
        }
    }

    private AfloatDBClusterEndpointsResponse createResponse(RaftNodeReport report) {
        RaftGroupMembers committedMembers = report.getCommittedMembers();

        AfloatDBClusterEndpoints.Builder endpointsBuilder = AfloatDBClusterEndpoints.newBuilder();
        endpointsBuilder.setClusterId(config.getRaftGroupConfig().getId());
        endpointsBuilder.setEndpointsCommitIndex(committedMembers.getLogIndex());
        if (report.getTerm().getLeaderEndpoint() != null) {
            endpointsBuilder.setLeaderId((String) report.getTerm().getLeaderEndpoint().getId());
        }

        endpointsBuilder.setTerm(report.getTerm().getTerm());

        raftRpcService.getAddresses().entrySet().stream()
                .filter(e -> committedMembers.getMembers().contains(e.getKey()))
                .forEach(e -> endpointsBuilder.putEndpoint((String) e.getKey().getId(), e.getValue()));

        return AfloatDBClusterEndpointsResponse.newBuilder().setEndpoints(endpointsBuilder.build()).build();
    }

    @Override
    public RaftNodeReport get() {
        return lastReport;
    }

}
