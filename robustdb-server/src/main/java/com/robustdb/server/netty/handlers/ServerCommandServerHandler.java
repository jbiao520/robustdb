package com.robustdb.server.netty.handlers;

import com.robustdb.server.protocol.mysql.MySQLMessage;
import com.robustdb.server.protocol.mysql.MySQLPacket;
import com.robustdb.server.protocol.mysql.OkPacket;
import com.robustdb.server.sql.MySqlEngine;
import com.robustdb.server.sql.mycat.server.ServerParse;
import com.robustdb.server.util.GlobalSqlUtil;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelId;
import io.netty.channel.ChannelInboundHandlerAdapter;
import lombok.extern.slf4j.Slf4j;

import java.io.UnsupportedEncodingException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

@Slf4j
@ChannelHandler.Sharable
public class ServerCommandServerHandler extends ChannelInboundHandlerAdapter {
	private Map<ChannelId, List<String>> clientSqls = new HashMap<>();
	private MySqlEngine mySqlEngine = new MySqlEngine();
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


		ByteBuf bufferOut;
		String sql = GlobalSqlUtil.getSql((ByteBuf) obj);
		int rs = ServerParse.parse(sql);
		int sqlType = rs & 0xff;

		switch (sqlType) {
			//explain sql
			case ServerParse.EXPLAIN:
				log.info("EXPLAIN");
				break;
			//explain2 datanode=? sql=?
			case ServerParse.EXPLAIN2:
				log.info("EXPLAIN2");
				break;
			case ServerParse.COMMAND:
				log.info("COMMAND");
				break;
			case ServerParse.SET:
				log.info("SET");
				break;
			case ServerParse.SHOW:
				log.info("SHOW");
				break;
			case ServerParse.SELECT:
				log.info("SELECT");
				break;
			case ServerParse.START:
				log.info("START");
				break;
			case ServerParse.BEGIN:
				log.info("BEGIN");
				break;
			//不支持oracle的savepoint事务回退点
			case ServerParse.SAVEPOINT:
				log.info("SAVEPOINT");
				break;
			case ServerParse.KILL:
				log.info("KILL");
				break;
			//不支持KILL_Query
			case ServerParse.KILL_QUERY:
				log.info("KILL_QUERY");
				break;
			case ServerParse.USE:
				log.info("USE");
				break;
			case ServerParse.COMMIT:
				log.info("COMMIT");
				break;
			case ServerParse.ROLLBACK:
				log.info("ROLLBACK");
				break;
			case ServerParse.HELP:
				log.info("HELP");
				break;
			case ServerParse.MYSQL_CMD_COMMENT:
				log.info("MYSQL_CMD_COMMENT");
				break;
			case ServerParse.MYSQL_COMMENT:
				log.info("MYSQL_COMMENT");
				break;
			case ServerParse.LOAD_DATA_INFILE_SQL:
				log.info("LOAD_DATA_INFILE_SQL");
				break;
			case ServerParse.MIGRATE: {
				log.info("MIGRATE");
				break;
			}
			case ServerParse.LOCK:
				log.info("LOCK");
				break;
			case ServerParse.UNLOCK:
				log.info("UNLOCK");
				break;
			default:
				log.info("default");
//				c.execute(sql, rs & 0xff);
		}

		switch (sqlType) {
			case ServerParse.SELECT:
			case ServerParse.DELETE:
			case ServerParse.UPDATE:
			case ServerParse.INSERT:
			case ServerParse.COMMAND:
				// curd 在后面会更新
				log.info("crud");
				break;
			default:
				log.info("default again");
//				c.setExecuteSql(null);
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
}