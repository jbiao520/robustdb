package com.robustdb.server.netty.handlers;

import com.robustdb.server.protocol.mysql.MySQLMessage;
import com.robustdb.server.protocol.mysql.MySQLPacket;
import com.robustdb.server.protocol.mysql.OkPacket;
import com.robustdb.server.protocol.response.SelectVariables;
import com.robustdb.server.util.Capabilities;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import lombok.extern.slf4j.Slf4j;

import java.io.UnsupportedEncodingException;

@Slf4j
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
	public void channelRead(ChannelHandlerContext ctx, Object obj) throws UnsupportedEncodingException {
		log.info("Msg received from channel channel:{}", ctx.channel().id());
		ByteBuf buf = (ByteBuf) obj;
		byte[] bytes = new byte[buf.readableBytes()];

		buf.readBytes(bytes);
		ByteBuf bufferOut;
		switch (bytes[4])
		{
			case MySQLPacket.COM_INIT_DB:
				log.info("Received COM_INIT_DB from channel:{}", ctx.channel().id());
				break;
			case MySQLPacket.COM_QUERY:
				log.info("Received COM_QUERY from channel:{}", ctx.channel().id());
				MySQLMessage mm = new MySQLMessage(bytes);
				mm.position(5);
				String sql = mm.readString("utf-8");

				log.info("Sql content : {}", sql);
				bufferOut= SelectVariables.execute(sql);
				ctx.write(bufferOut);
				break;
			case MySQLPacket.COM_PING:
				log.info("Received COM_PING from channel:{}", ctx.channel().id());
				bufferOut = Unpooled.buffer();
				bufferOut.writeBytes(OkPacket.OK);
				ctx.write(bufferOut);
				break;
			case MySQLPacket.COM_QUIT:
				log.info("Received COM_QUIT from channel:{}", ctx.channel().id());
				break;
			case MySQLPacket.COM_PROCESS_KILL:
				log.info("Received COM_PROCESS_KILL from channel:{}", ctx.channel().id());
				break;
			case MySQLPacket.COM_STMT_PREPARE:
				log.info("Received COM_STMT_PREPARE from channel:{}", ctx.channel().id());
				break;
			case MySQLPacket.COM_STMT_SEND_LONG_DATA:
				log.info("Received COM_STMT_SEND_LONG_DATA from channel:{}", ctx.channel().id());
				break;
			case MySQLPacket.COM_STMT_RESET:
				log.info("Received COM_STMT_RESET from channel:{}", ctx.channel().id());
				break;
			case MySQLPacket.COM_STMT_EXECUTE:
				log.info("Received COM_STMT_EXECUTE from channel:{}", ctx.channel().id());
				break;
			case MySQLPacket.COM_STMT_CLOSE:
				log.info("Received COM_STMT_CLOSE from channel:{}", ctx.channel().id());
				break;
			case MySQLPacket.COM_HEARTBEAT:
				log.info("Received COM_HEARTBEAT from channel:{}", ctx.channel().id());
				break;
			case MySQLPacket.COM_FIELD_LIST:
				log.info("Received COM_FIELD_LIST from channel:{}", ctx.channel().id());
				break;
			case MySQLPacket.COM_SET_OPTION:
				log.info("Received COM_SET_OPTION from channel:{}", ctx.channel().id());
				break;
			case MySQLPacket.COM_RESET_CONNECTION:
				log.info("Received COM_RESET_CONNECTION from channel:{}", ctx.channel().id());
				break;
			default:

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