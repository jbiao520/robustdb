package com.robustdb.server.netty.handlers;

import com.robustdb.server.protocol.mysql.AuthPacket;
import com.robustdb.server.protocol.mysql.HandshakeV10Packet;
import com.robustdb.server.protocol.mysql.OkPacket;
import com.robustdb.server.util.Capabilities;
import com.robustdb.server.util.CharsetUtil;
import com.robustdb.server.util.RandomUtil;
import com.robustdb.server.util.Versions;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;



public class AuthServerHandler extends ChannelInboundHandlerAdapter {

	@Override
	public void channelActive(ChannelHandlerContext ctx) throws Exception {
		super.channelActive(ctx);
		byte[] rand1 = RandomUtil.randomBytes(8);
		byte[] rand2 = RandomUtil.randomBytes(12);

		// 保存认证数据
		byte[] seed = new byte[rand1.length + rand2.length];
		System.arraycopy(rand1, 0, seed, 0, rand1.length);
		System.arraycopy(rand2, 0, seed, rand1.length, rand2.length);

		HandshakeV10Packet hs = new HandshakeV10Packet();
		hs.packetId = 0;
		hs.protocolVersion = Versions.PROTOCOL_VERSION;
		hs.serverVersion = Versions.SERVER_VERSION;
		hs.threadId = 1;
		hs.seed = rand1;
		hs.serverCapabilities = getServerCapabilities();
		hs.serverCharsetIndex = (byte) (CharsetUtil.getIndex("utf8") & 0xff);
		hs.serverStatus = 2;
		hs.restOfScrambleBuff = rand2;
		ctx.write(hs.write());
		ctx.flush();
	}

	@Override
	public void channelRegistered(ChannelHandlerContext ctx) throws Exception {
		super.channelRegistered(ctx);
	}

	@Override
	public void channelReadComplete(ChannelHandlerContext ctx) throws Exception {
		super.channelReadComplete(ctx);
		ctx.flush();
	}

	@Override
	public void channelRead(ChannelHandlerContext ctx, Object obj) {
		System.out.println(ctx.channel().remoteAddress() + " -> Server:" + obj);
		ByteBuf buf = (ByteBuf) obj;
		byte[] bytes = new byte[buf.readableBytes()];

		buf.readBytes(bytes);
		System.out.println(bytes[4]);
		AuthPacket msg = new AuthPacket();
		msg.read(bytes);

		System.out.println(ctx.channel().remoteAddress() + " -> Server:" + msg);
		ByteBuf bufferOut= Unpooled.buffer();
		bufferOut.writeBytes(OkPacket.AUTH_OK);
		ctx.write(bufferOut);

	}

	@Override
	public void channelUnregistered(ChannelHandlerContext ctx) throws Exception {
		super.channelUnregistered(ctx);
	}

	@Override
	public void channelInactive(ChannelHandlerContext ctx) throws Exception {
		super.channelInactive(ctx);
	}

	@Override
	public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {

		cause.printStackTrace();
		ctx.close();
	}

	protected int getServerCapabilities() {
		int flag = 0;
		flag |= Capabilities.CLIENT_LONG_PASSWORD;
		flag |= Capabilities.CLIENT_FOUND_ROWS;
		flag |= Capabilities.CLIENT_LONG_FLAG;
		flag |= Capabilities.CLIENT_CONNECT_WITH_DB;
		// flag |= Capabilities.CLIENT_NO_SCHEMA;
		boolean usingCompress= false;
		if (usingCompress) {
			flag |= Capabilities.CLIENT_COMPRESS;
		}

		flag |= Capabilities.CLIENT_ODBC;
		flag |= Capabilities.CLIENT_LOCAL_FILES;
		flag |= Capabilities.CLIENT_IGNORE_SPACE;
		flag |= Capabilities.CLIENT_PROTOCOL_41;
		flag |= Capabilities.CLIENT_INTERACTIVE;
		// flag |= Capabilities.CLIENT_SSL;
		flag |= Capabilities.CLIENT_IGNORE_SIGPIPE;
		flag |= Capabilities.CLIENT_TRANSACTIONS;
		// flag |= ServerDefs.CLIENT_RESERVED;
		flag |= Capabilities.CLIENT_SECURE_CONNECTION;
		// flag |= Capabilities.CLIENT_MULTI_STATEMENTS;
		flag |= Capabilities.CLIENT_MULTI_RESULTS;
		boolean useHandshakeV10 = true;
		if(useHandshakeV10) {
			flag |= Capabilities.CLIENT_PLUGIN_AUTH;
		}
		return flag;
	}
}