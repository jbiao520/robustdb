package com.robustdb.server.netty.biz;

import com.robustdb.server.protocol.mysql.OkPacket;
import com.robustdb.server.sql.MySqlEngine;
import com.robustdb.server.sql.mycat.manager.ManageSelectHandler;
import com.robustdb.server.sql.mycat.manager.ManagerParse;
import com.robustdb.server.sql.mycat.manager.SelectVariables;
import com.robustdb.server.sql.mycat.server.DDLHandler;
import com.robustdb.server.sql.mycat.server.DMLHandler;
import com.robustdb.server.sql.mycat.server.ServerParse;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelId;
import lombok.extern.slf4j.Slf4j;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Slf4j
public class ServerQueryBizHandler {
	private Map<ChannelId, List<String>> clientSqls = new HashMap<>();
	private MySqlEngine mySqlEngine = new MySqlEngine();

	public ByteBuf handle(String sql)  {


		ByteBuf bufferOut = null;
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

				log.info("Normal sql detected, Sql content : {}", sql);
//				mySqlEngine.executeSql(sql);
				bufferOut= SelectVariables.execute(sql,null);
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
				bufferOut = Unpooled.buffer();
				bufferOut.writeBytes(OkPacket.OK);
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
			case ServerParse.DDL:
				log.info("DDL");
				bufferOut = DDLHandler.handle(sql);
				break;
			case ServerParse.CREATE:
				log.info("CREATE");
				bufferOut = DDLHandler.handle(sql);
				break;
			case ServerParse.DROP:
				log.info("CREATE");
				bufferOut = DDLHandler.handle(sql);
				break;
			case ServerParse.TRUNCATE:
				log.info("CREATE");
				bufferOut = DDLHandler.handle(sql);
				break;
			case ServerParse.ALTER:
				log.info("CREATE");
				bufferOut = DDLHandler.handle(sql);
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
				bufferOut = DMLHandler.handle(sql);
				break;
			default:
				log.info("default again");
//				c.setExecuteSql(null);
		}
		return bufferOut;
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