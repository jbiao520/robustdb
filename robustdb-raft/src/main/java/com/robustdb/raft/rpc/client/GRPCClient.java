package com.robustdb.raft.rpc.client;

import com.robustdb.raft.gRequest.api.RaftRequestDataServiceGrpc;
import com.robustdb.raft.gRequest.api.RequestVoteRequest;
import com.robustdb.raft.gRequest.api.RequestVoteResult;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

public class GRPCClient {
    private static final String host = "localhost";
    private static final int serverPort = 9999;

    public static void main(String[] args) throws Exception {
        ManagedChannel managedChannel = ManagedChannelBuilder.forAddress(host, serverPort).usePlaintext().build();
        try {
            RaftRequestDataServiceGrpc.RaftRequestDataServiceBlockingStub raftRpcDataService = RaftRequestDataServiceGrpc.newBlockingStub(managedChannel);
            RequestVoteRequest requestVoteRequest = RequestVoteRequest
                    .newBuilder()
                    .setTerm(0)
                    .build();
            RequestVoteResult requestVoteResult = raftRpcDataService.requestVote(requestVoteRequest);
            System.out.println(requestVoteResult.getVoteGranted());
        } finally {
            managedChannel.shutdown();
        }
    }
}
