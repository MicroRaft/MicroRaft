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

package io.afloatdb.internal.rpc.impl;

import io.afloatdb.AfloatDB;
import io.afloatdb.internal.raft.impl.model.AfloatDBEndpoint;
import io.afloatdb.internal.rpc.RaftRpcService;
import io.afloatdb.management.proto.AddRaftEndpointAddressRequest;
import io.afloatdb.management.proto.AddRaftEndpointAddressResponse;
import io.afloatdb.management.proto.AddRaftEndpointRequest;
import io.afloatdb.management.proto.AddRaftEndpointResponse;
import io.afloatdb.management.proto.GetRaftNodeReportRequest;
import io.afloatdb.management.proto.GetRaftNodeReportResponse;
import io.afloatdb.management.proto.ManagementRequestHandlerGrpc.ManagementRequestHandlerImplBase;
import io.afloatdb.management.proto.RemoveRaftEndpointRequest;
import io.afloatdb.management.proto.RemoveRaftEndpointResponse;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import io.microraft.MembershipChangeMode;
import io.microraft.RaftEndpoint;
import io.microraft.RaftNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Singleton;
import java.util.function.Supplier;

import static io.afloatdb.internal.di.AfloatDBModule.RAFT_NODE_SUPPLIER_KEY;
import static io.afloatdb.internal.utils.Exceptions.wrap;
import static io.afloatdb.internal.utils.Serialization.toProto;
import static io.microraft.MembershipChangeMode.REMOVE_MEMBER;

@Singleton
public class ManagementRequestHandler extends ManagementRequestHandlerImplBase {

    private static final Logger LOGGER = LoggerFactory.getLogger(ManagementRequestHandler.class);

    private final RaftNode raftNode;
    private final RaftRpcService raftRpcService;

    @Inject
    public ManagementRequestHandler(@Named(RAFT_NODE_SUPPLIER_KEY) Supplier<RaftNode> raftNodeSupplier,
            RaftRpcService raftRpcService) {
        this.raftNode = raftNodeSupplier.get();
        this.raftRpcService = raftRpcService;
    }

    @Override
    public void removeRaftEndpoint(RemoveRaftEndpointRequest request,
            StreamObserver<RemoveRaftEndpointResponse> responseObserver) {
        AfloatDBEndpoint endpoint = AfloatDBEndpoint.wrap(request.getEndpoint());

        long commitIndex = request.getGroupMembersCommitIndex();
        LOGGER.info("{} received remove endpoint request for {} and group members commit index: {}",
                raftNode.getLocalEndpoint().getId(), endpoint.getId(), commitIndex);

        raftNode.changeMembership(endpoint, REMOVE_MEMBER, commitIndex).whenComplete((result, throwable) -> {
            if (throwable == null) {
                long newCommitIndex = result.getCommitIndex();
                LOGGER.info("{} removed {} from the Raft group. New group members commit index: {}",
                        raftNode.getLocalEndpoint().getId(), endpoint.getId(), newCommitIndex);
                RemoveRaftEndpointResponse response = RemoveRaftEndpointResponse.newBuilder()
                        .setGroupMembersCommitIndex(newCommitIndex).build();
                responseObserver.onNext(response);
            } else {
                LOGGER.error(raftNode.getLocalEndpoint().getId() + " remove endpoint request for " + endpoint.getId()
                        + " and group members commit index: " + commitIndex + " failed!", throwable);
                responseObserver.onError(wrap(throwable));
            }
            responseObserver.onCompleted();
        });
    }

    @Override
    public void getRaftNodeReport(GetRaftNodeReportRequest request,
            StreamObserver<GetRaftNodeReportResponse> responseObserver) {
        raftNode.getReport().whenComplete((response, throwable) -> {
            if (throwable == null) {
                GetRaftNodeReportResponse.Builder builder = GetRaftNodeReportResponse.newBuilder();
                builder.setReport(toProto(response.getResult()));
                raftRpcService.getAddresses()
                        .forEach((key, value) -> builder.putEndpointAddress(key.getId().toString(), value));

                responseObserver.onNext(builder.build());
            } else {
                responseObserver.onError(wrap(throwable));
            }
            responseObserver.onCompleted();
        });
    }

    @Override
    public void addRaftEndpointAddress(AddRaftEndpointAddressRequest request,
            StreamObserver<AddRaftEndpointAddressResponse> responseObserver) {
        try {
            RaftEndpoint endpoint = AfloatDBEndpoint.wrap(request.getEndpoint());
            LOGGER.info("Adding address: {} for {}.", request.getAddress(), endpoint);
            raftRpcService.addAddress(endpoint, request.getAddress());
            responseObserver.onNext(AddRaftEndpointAddressResponse.getDefaultInstance());
        } catch (Throwable t) {
            responseObserver.onError(wrap(t));
        } finally {
            responseObserver.onCompleted();
        }
    }

    @Override
    public void addRaftEndpoint(AddRaftEndpointRequest request,
            StreamObserver<AddRaftEndpointResponse> responseObserver) {
        RaftEndpoint endpoint = AfloatDBEndpoint.wrap(request.getEndpoint());
        String address = raftRpcService.getAddresses().get(endpoint);
        if (address == null) {
            LOGGER.error("{} cannot add {} because its address is not known!", raftNode.getLocalEndpoint().getId(),
                    endpoint.getId());
            responseObserver.onError(new StatusRuntimeException(Status.FAILED_PRECONDITION));
            responseObserver.onCompleted();
            return;
        }

        MembershipChangeMode mode = request.getVotingMember() ? MembershipChangeMode.ADD_OR_PROMOTE_TO_FOLLOWER
                : MembershipChangeMode.ADD_LEARNER;

        LOGGER.info("{} is adding {} with mode: {} and address: {}.", raftNode.getLocalEndpoint().getId(),
                endpoint.getId(), mode, address);

        raftNode.changeMembership(endpoint, mode, request.getGroupMembersCommitIndex())
                .whenComplete((result, throwable) -> {
                    if (throwable == null) {
                        long newCommitIndex = result.getCommitIndex();
                        LOGGER.info("{} added {} with address: {}.", raftNode.getLocalEndpoint().getId(),
                                endpoint.getId(), address);
                        AddRaftEndpointResponse response = AddRaftEndpointResponse.newBuilder()
                                .setGroupMembersCommitIndex(newCommitIndex).build();
                        responseObserver.onNext(response);
                    } else {
                        LOGGER.error(
                                raftNode.getLocalEndpoint().getId() + " could not add " + endpoint + " "
                                        + "with group members commit index: " + request.getGroupMembersCommitIndex(),
                                throwable);
                        responseObserver.onError(wrap(throwable));
                    }
                    responseObserver.onCompleted();
                });
    }

}
