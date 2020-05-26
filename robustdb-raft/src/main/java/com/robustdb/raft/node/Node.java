package com.robustdb.raft.node;

import com.robustdb.raft.enums.RaftRoleEnum;

import java.io.IOException;

public interface Node {

    RaftRoleEnum getCurrentRole();

    NodeId getCurrentNodeId();

    boolean isLeader();

    boolean isStarted();

    void start() throws IOException;

    NodeId getVotedFor();
}
