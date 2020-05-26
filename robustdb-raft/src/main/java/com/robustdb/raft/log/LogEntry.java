package com.robustdb.raft.log;

import lombok.Data;

@Data
public class LogEntry {
    private int term;
    private long index;
    private byte[] data;
}
