package com.robustdb.server.netty;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;

public class NettyServer {

    public static int DEFAULT_PORT = 3307;

    public static void main(String[] args) throws Exception {


        EventLoopGroup bossGroup = new NioEventLoopGroup(); // boss
        EventLoopGroup workerGroup = new NioEventLoopGroup(); // worker

        try {
            ServerBootstrap b = new ServerBootstrap();

            b.group(bossGroup, workerGroup)
                    .channel(NioServerSocketChannel.class)
                    .childHandler(new EchoServerHandler())
                    .option(ChannelOption.SO_BACKLOG, 128)
                    .childOption(ChannelOption.SO_KEEPALIVE, true);

            ChannelFuture f = b.bind(DEFAULT_PORT).sync();
            System.out.println("Server已启动，端口：" + DEFAULT_PORT);
            f.channel().closeFuture().sync();
        } finally {

            workerGroup.shutdownGracefully();
            bossGroup.shutdownGracefully();
        }

    }
}