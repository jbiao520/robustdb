package com.robustdb.raft.message;

import com.robustdb.raft.node.NodeId;
import lombok.Data;

@Data
public class RequestVoteRpcMsg {
    private NodeId candidateId;
    private int term;
    private long lastLogIndex;
    private int lastLogTerm;
}
