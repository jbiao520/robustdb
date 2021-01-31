package com.robustdb.server.netty.handlers;

import com.robustdb.server.netty.biz.ServerQueryBizHandler;
import com.robustdb.server.protocol.mysql.MySQLMessage;
import com.robustdb.server.protocol.mysql.MySQLPacket;
import com.robustdb.server.protocol.mysql.OkPacket;
import com.robustdb.server.sql.MySqlEngine;
import com.robustdb.server.sql.mycat.manager.SelectVariables;
import com.robustdb.server.sql.mycat.manager.ManagerParse;
import com.robustdb.server.sql.mycat.manager.ManageSelectHandler;
import com.robustdb.server.util.GlobalSqlUtil;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelId;
import io.netty.channel.ChannelInboundHandlerAdapter;
import lombok.extern.slf4j.Slf4j;

import java.io.UnsupportedEncodingException;
import java.util.*;

@Slf4j
@ChannelHandler.Sharable
public class ServerCommandServerHandler extends ChannelInboundHandlerAdapter {
	private Map<ChannelId, List<String>> clientSqls = new HashMap<>();
	private MySqlEngine mySqlEngine = new MySqlEngine();
	private ServerQueryBizHandler serverQueryBizHandler = new ServerQueryBizHandler();
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
			byte[] bytes = new byte[buf.readableBytes()];
			MySQLMessage mm = new MySQLMessage(bytes);
			buf.readBytes(bytes);
			ByteBuf bufferOut;
			String sql = GlobalSqlUtil.getSql(bytes);
			int i = bytes[4];
			switch (i)
			{
				case MySQLPacket.COM_INIT_DB:
					mm.position(5);
					String db = mm.readString();
					log.info("Received COM_INIT_DB from channel:{},db:{}", ctx.channel().id(),db);
					break;
				case MySQLPacket.COM_QUERY:

//				String sql = GlobalSqlUtil.getSql(bytes);
					log.info("Received COM_QUERY from channel:{},sql:{}", ctx.channel().id(),sql);
					if(isMgmtSql(sql)){
						log.info("Mgmt sql detected, Sql content : {}", sql);
						ctx.fireChannelRead(obj);
					}else{
						log.info("Normal sql detected, Sql content : {}", sql);
						bufferOut = serverQueryBizHandler.handle(sql);
						mySqlEngine.executeSql(sql);
						ctx.write(bufferOut);
						ctx.flush();
					}
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
					log.info("Default received from channel:{}", ctx.channel().id());
					ctx.fireChannelRead(obj);
					break;
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
		clientSqls.forEach((k,v)->{
			log.info("Dumping all sqls for:{}",k);
			v.forEach(sql->{
				log.info(sql);
			});
		});
	}

	@Override
	public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {

		cause.printStackTrace();
		ctx.close();
	}

	private boolean isMgmtSql(String sql){
		int rs = ManagerParse.parse(sql);
		switch (rs & 0xff) {
			case ManagerParse.SELECT:
				return ManageSelectHandler.isMgmtSelect(sql);
			case ManagerParse.SET:
				return true;
			case ManagerParse.SHOW:
				return true;
			case ManagerParse.SWITCH:
			case ManagerParse.KILL_CONN:
			case ManagerParse.OFFLINE:
			case ManagerParse.ONLINE:
			case ManagerParse.STOP:
			case ManagerParse.RELOAD:
				return false;
			case ManagerParse.ROLLBACK:
				return false;
			case ManagerParse.CLEAR:
				return false;
			case ManagerParse.CONFIGFILE:
				return false;
			case ManagerParse.LOGFILE:
				return false;
			case ManagerParse.ZK:
				return false;
			default:
				return false;
		}
	}
}