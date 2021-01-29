package com.robustdb.server.netty;

import com.robustdb.server.netty.handlers.AuthServerHandler;
import com.robustdb.server.netty.handlers.MgmtCommandServerHandler;
import com.robustdb.server.netty.handlers.QueryCommandServerHandler;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.socket.SocketChannel;

public class ServerChannelInitializer extends ChannelInitializer<SocketChannel> {
    @Override
    protected void initChannel(SocketChannel ch) throws Exception {

        ch.pipeline().addLast(new AuthServerHandler());

        ch.pipeline().addLast(new QueryCommandServerHandler());
        ch.pipeline().addLast(new MgmtCommandServerHandler());
    }
}
