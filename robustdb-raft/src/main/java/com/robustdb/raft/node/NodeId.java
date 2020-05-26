package com.robustdb.raft.node;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class NodeId {
    private int serverId;
    private EndPoint endPoint;
}
