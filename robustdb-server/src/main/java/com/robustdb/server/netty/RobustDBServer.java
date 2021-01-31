package com.robustdb.server.netty;

import com.robustdb.server.tests.RobustDBTest;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class RobustDBServer {

    public static int DEFAULT_PORT = 3307;
    public static void main(String[] args) throws Exception {
        log.info("Starting netty server");

        EventLoopGroup bossGroup = new NioEventLoopGroup(); // boss
        EventLoopGroup workerGroup = new NioEventLoopGroup(); // worker

        try {
            ServerBootstrap b = new ServerBootstrap();

            b.group(bossGroup, workerGroup)
                    .channel(NioServerSocketChannel.class)
                    .childHandler(new ServerChannelInitializer())
                    .option(ChannelOption.SO_BACKLOG, 128)
                    .childOption(ChannelOption.SO_KEEPALIVE, true);

            ChannelFuture f = b.bind(DEFAULT_PORT).sync();
            log.info("Netty server started with port:{}",DEFAULT_PORT);
//            RobustDBTest.callrobust();
            f.channel().closeFuture().sync();
        } finally {
            workerGroup.shutdownGracefully();
            bossGroup.shutdownGracefully();
        }

    }
}