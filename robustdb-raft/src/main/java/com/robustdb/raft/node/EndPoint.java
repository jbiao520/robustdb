package com.robustdb.raft.node;


import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class EndPoint {
    private int port;
    private String bindAddr;


}
