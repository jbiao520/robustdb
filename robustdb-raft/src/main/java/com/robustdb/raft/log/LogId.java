package com.robustdb.raft.log;

import lombok.Data;

@Data
public class LogId {
    private long index;
    private int term;
}
