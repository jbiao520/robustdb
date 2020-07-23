package com.robustdb.raft.rpc.server;


import com.robustdb.raft.gRequest.api.*;
import io.grpc.stub.StreamObserver;

public class RaftRPCDataServiceImpl extends RaftRequestDataServiceGrpc.RaftRequestDataServiceImplBase {
    @Override
    public void requestVote(RequestVoteRequest request, StreamObserver<RequestVoteResult> responseObserver) {
        super.requestVote(request, responseObserver);
    }

    @Override
    public void appendEntries(AppendEntriesRequest request, StreamObserver<AppendEntriesResult> responseObserver) {
        super.appendEntries(request, responseObserver);
    }

    @Override
    public void installSnapshot(InstallSnapshotRequest request, StreamObserver<InstallSnapshotResult> responseObserver) {
        super.installSnapshot(request, responseObserver);
    }

    @Override
    public void addServer(AddServerRequest request, StreamObserver<AddServerResult> responseObserver) {
        super.addServer(request, responseObserver);
    }

    @Override
    public void removeServer(RemoveServerRequest request, StreamObserver<RemoveServerResult> responseObserver) {
        super.removeServer(request, responseObserver);
    }
}
