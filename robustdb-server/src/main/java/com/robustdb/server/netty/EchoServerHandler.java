package com.robustdb.server.netty;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;

import java.nio.charset.StandardCharsets;


public class EchoServerHandler extends ChannelInboundHandlerAdapter {

	@Override
	public void channelActive(ChannelHandlerContext ctx) throws Exception {
		super.channelActive(ctx);
		ctx.write("test".getBytes(StandardCharsets.UTF_8));
		ctx.flush();
	}

	@Override
	public void channelRead(ChannelHandlerContext ctx, Object msg) {
		System.out.println(ctx.channel().remoteAddress() + " -> Server :" + msg);
		
		ctx.write(msg);
		ctx.flush();
		
	}
	
	@Override
	public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {

		cause.printStackTrace();
		ctx.close();
	}
}