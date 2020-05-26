package com.robustdb.raft.message;

import com.robustdb.raft.log.LogEntry;
import com.robustdb.raft.node.NodeId;
import lombok.Data;

import java.util.List;

@Data
public class AppendEntriesRpcMsg {
    private long prevLogIndex;
    private int prevLogTerm;
    private List<LogEntry> entryList;
    private NodeId leaderId;
    private long leaderCommit;
    private int term;
}
