package com.robustdb.server.netty.handlers;

import com.robustdb.server.protocol.mysql.MySQLPacket;
import com.robustdb.server.protocol.mysql.OkPacket;
import com.robustdb.server.util.Capabilities;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;


@ChannelHandler.Sharable
public class CommandServerHandler extends ChannelInboundHandlerAdapter {

	@Override
	public void channelActive(ChannelHandlerContext ctx) throws Exception {
		super.channelActive(ctx);
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
		ByteBuf buf = (ByteBuf) obj;
		byte[] bytes = new byte[buf.readableBytes()];

		buf.readBytes(bytes);
		switch (bytes[4])
		{
			case MySQLPacket.COM_INIT_DB:
				break;
			case MySQLPacket.COM_QUERY:
				break;
			case MySQLPacket.COM_PING:
				break;
			case MySQLPacket.COM_QUIT:
				break;
			case MySQLPacket.COM_PROCESS_KILL:
				break;
			case MySQLPacket.COM_STMT_PREPARE:
				break;
			case MySQLPacket.COM_STMT_SEND_LONG_DATA:
				break;
			case MySQLPacket.COM_STMT_RESET:
				break;
			case MySQLPacket.COM_STMT_EXECUTE:
				break;
			case MySQLPacket.COM_STMT_CLOSE:
				break;
			case MySQLPacket.COM_HEARTBEAT:
				break;
			case MySQLPacket.COM_FIELD_LIST:
				break;
			case MySQLPacket.COM_SET_OPTION:
				break;
			case MySQLPacket.COM_RESET_CONNECTION:
				break;
			default:
				ByteBuf bufferOut= Unpooled.buffer();
				bufferOut.writeBytes(OkPacket.AUTH_OK);
				ctx.write(bufferOut);
		}


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