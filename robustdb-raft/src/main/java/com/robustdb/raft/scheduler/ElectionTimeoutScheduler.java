package com.robustdb.raft.scheduler;

import com.robustdb.raft.node.NodeOptions;

import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class ElectionTimeoutScheduler {
    private int electionTimeoutMs;
    private NodeOptions nodeOptions;
    private Random random = new Random();
    ScheduledExecutorService pool = Executors.newSingleThreadScheduledExecutor();

    private void init() {
        resetElectionTimeoutMs();
        pool.schedule(() -> {

        }, electionTimeoutMs, TimeUnit.MILLISECONDS);
    }

    public ElectionTimeoutScheduler(NodeOptions nodeOptions) {
        this.nodeOptions = nodeOptions;
        init();
    }

    private void resetElectionTimeoutMs() {
        electionTimeoutMs = nodeOptions.getElectionTimeOutMs() + random.nextInt(nodeOptions.getElectionTimeOutInterVal());
    }

}
