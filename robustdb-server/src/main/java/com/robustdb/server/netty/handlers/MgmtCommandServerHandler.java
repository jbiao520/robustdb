package com.robustdb.server.netty.handlers;

import com.robustdb.server.protocol.mysql.MySQLMessage;
import com.robustdb.server.protocol.mysql.OkPacket;
import com.robustdb.server.sql.mycat.manager.ManagerParse;
import com.robustdb.server.sql.mycat.manager.ManageSelectHandler;
import com.robustdb.server.sql.mycat.manager.ShowHandler;
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
public class MgmtCommandServerHandler extends ChannelInboundHandlerAdapter {
	private static final int SHIFT = 8;
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
		try{
			ByteBuf buf = (ByteBuf) obj;
			buf.resetReaderIndex();
			byte[] bytes = new byte[buf.readableBytes()];
			buf.readBytes(bytes);
			MySQLMessage mm = new MySQLMessage(bytes);
			mm.position(5);
			String sql = mm.readString("utf-8");
			int rs = ManagerParse.parse(sql);
			ByteBuf bufferOut;
			int x = rs & 0xff;
			switch (x) {
				case ManagerParse.SELECT:
					bufferOut = ManageSelectHandler.handle(sql, rs >>> SHIFT);
					ctx.write(bufferOut);
					break;
				case ManagerParse.SET:
					log.info("Received ManagerParse.SET from channel:{}", ctx.channel().id());
					bufferOut = Unpooled.buffer();
					bufferOut.writeBytes(OkPacket.OK);
					ctx.write(bufferOut);
					break;
				case ManagerParse.SHOW:
					bufferOut = ShowHandler.handle(sql, rs >>> SHIFT);
					ctx.write(bufferOut);
					break;
				case ManagerParse.SWITCH:
					break;
				case ManagerParse.KILL_CONN:
					break;
				case ManagerParse.OFFLINE:
					break;
				case ManagerParse.ONLINE:
					break;
				case ManagerParse.STOP:
					break;
				case ManagerParse.RELOAD:
					break;
				case ManagerParse.ROLLBACK:
					break;
				case ManagerParse.CLEAR:
					break;
				case ManagerParse.CONFIGFILE:
					break;
				case ManagerParse.LOGFILE:
					break;
				case ManagerParse.ZK:
					break;
				default:
			}
		}catch(Exception e){
			log.error("Unexpected error:{}",e);
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