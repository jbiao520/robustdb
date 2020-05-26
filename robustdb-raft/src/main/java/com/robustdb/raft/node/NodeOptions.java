package com.robustdb.raft.node;

import lombok.Data;

@Data
public class NodeOptions {
    private int port;
    private int serverId;
    private int electionTimeOutMs;
    private int electionTimeOutInterVal;
    private String addr;
}
