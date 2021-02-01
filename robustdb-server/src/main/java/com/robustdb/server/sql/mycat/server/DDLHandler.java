package com.robustdb.server.sql.mycat.server;


import com.robustdb.server.protocol.mysql.OkPacket;
import com.robustdb.server.sql.MySqlEngine;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import lombok.extern.slf4j.Slf4j;

/**
 * @author mycat
 */
@Slf4j
public final class DDLHandler {
    private static MySqlEngine mySqlEngine = new MySqlEngine();
    public static ByteBuf handle(String sql) {
        ByteBuf bufferOut = null;
        int rs = ServerParse.parse(sql);
        int result = rs & 0xff;
        switch (result) {
            case ServerParse.CREATE:
                log.info("CREATE");
                bufferOut = handleCreate(sql);
                break;
            case ServerParse.DROP:
                log.info("CREATE");
                bufferOut = handleDrop(sql);
                break;
            case ServerParse.TRUNCATE:
                log.info("CREATE");
                bufferOut = handleTruncate(sql);
                break;
            case ServerParse.ALTER:
                log.info("CREATE");
                bufferOut = handleAlter(sql);
                break;
            default:
                log.info("default");
//				c.execute(sql, rs & 0xff);
        }
        return bufferOut;
    }

    private static ByteBuf handleCreate(String sql){
        ByteBuf byteBuf = null;
        try{
            byteBuf = mySqlEngine.executeSql(sql).getByteBuf();
        }catch(Exception e){
            e.printStackTrace();
        }
        return byteBuf;
    }

    private static ByteBuf handleDrop(String sql){
        ByteBuf byteBuf = Unpooled.buffer();
        return byteBuf;
    }

    private static ByteBuf handleAlter(String sql){
        ByteBuf byteBuf = null;
        try{
            byteBuf = mySqlEngine.executeSql(sql).getByteBuf();
        }catch(Exception e){
            e.printStackTrace();
        }
        return byteBuf;
    }

    private static ByteBuf handleTruncate(String sql){
        ByteBuf byteBuf = Unpooled.buffer();
        return byteBuf;
    }
}