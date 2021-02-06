package com.robustdb.server.sql.mycat.server;


import com.robustdb.server.protocol.mysql.ErrorPacket;
import com.robustdb.server.protocol.mysql.OkPacket;
import com.robustdb.server.sql.MySqlEngine;
import com.robustdb.server.util.ErrorCode;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import lombok.extern.slf4j.Slf4j;

/**
 * @author mycat
 */
@Slf4j
public final class DMLHandler {
    private static MySqlEngine mySqlEngine = new MySqlEngine();

    public static ByteBuf handle(String sql) {
        ByteBuf bufferOut = null;
        try {
            int rs = ServerParse.parse(sql);
            int result = rs & 0xff;
            switch (result) {
                case ServerParse.SELECT:
                    log.info("CREATE");
                    bufferOut = handleDML(sql);
                    break;
                case ServerParse.DELETE:
                    log.info("CREATE");
                    bufferOut = handleDML(sql);
                    break;
                case ServerParse.UPDATE:
                    log.info("CREATE");
                    bufferOut = handleDML(sql);
                    break;
                case ServerParse.INSERT:
                    log.info("CREATE");
                    bufferOut = handleDML(sql);
                    break;
                default:
                    log.info("default");
//				c.execute(sql, rs & 0xff);
            }
        } catch (Exception e) {
            e.printStackTrace();

        }

        return bufferOut;
    }

    private static ByteBuf handleDML(String sql) {
        ByteBuf byteBuf = mySqlEngine.executeSql(sql).getByteBuf();
        return byteBuf;
    }


}