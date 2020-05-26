package com.robustdb.raft.node.impl;

import com.robustdb.raft.enums.RaftRoleEnum;
import com.robustdb.raft.node.EndPoint;
import com.robustdb.raft.node.Node;
import com.robustdb.raft.node.NodeId;
import com.robustdb.raft.node.NodeOptions;
import com.robustdb.raft.scheduler.ElectionTimeoutScheduler;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class NodeImpl implements Node {
    private RaftRoleEnum currentRole = RaftRoleEnum.FOLLOWER;
    private volatile boolean started = false;
    private NodeOptions nodeOptions;
    private NodeId nodeId;
    private NodeId votedFor;
    private ElectionTimeoutScheduler electionTimeoutScheduler;

    public RaftRoleEnum getCurrentRole() {
        return currentRole;
    }

    public NodeId getCurrentNodeId() {
        return nodeId;
    }


    public boolean isLeader() {
        return false;
    }

    public boolean isStarted() {
        return false;
    }

    public void start() throws IOException {
        initConf();
        initComponents();
        initLog();

    }

    public NodeId getVotedFor() {
        return votedFor;
    }

    private void initLog() {
        //TODO
    }

    private void initConf() throws IOException {
        InputStream inStream = NodeImpl.class.getClassLoader().getResourceAsStream("conf/raft.properties");
        Properties prop = new Properties();
        prop.load(inStream);
        nodeOptions = new NodeOptions();
        String addr = prop.getProperty("bind.addr");
        int port = Integer.parseInt(prop.getProperty("bind.port"));
        int serverId = Integer.parseInt(prop.getProperty("raft.serverId"));
        int electionTimeOutMs = Integer.parseInt(prop.getProperty("raft.electionTimeOutMs"));
        int electionTimeOutInterVal = Integer.parseInt(prop.getProperty("raft.electionTimeOutInterVal"));
        nodeOptions.setElectionTimeOutInterVal(electionTimeOutInterVal);
        nodeOptions.setAddr(addr);
        nodeOptions.setElectionTimeOutMs(electionTimeOutMs);
        nodeOptions.setServerId(serverId);
        nodeOptions.setPort(port);
    }

    private void initComponents() {
        EndPoint endPoint = new EndPoint(nodeOptions.getPort(), nodeOptions.getAddr());
        nodeId = new NodeId(nodeOptions.getServerId(), endPoint);
        electionTimeoutScheduler = new ElectionTimeoutScheduler(nodeOptions);
    }
}
