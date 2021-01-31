package com.robustdb.server.util;

import com.robustdb.server.protocol.mysql.ErrorPacket;
import com.robustdb.server.protocol.mysql.MySQLMessage;
import io.netty.buffer.ByteBuf;

import java.io.UnsupportedEncodingException;

public class GlobalSqlUtil {
    public static final String UTF8="utf-8";

    public static String getSql(byte[] data) {
        // 取得语句
        String sql = null;
        try {
            MySQLMessage mm = new MySQLMessage(data);
            mm.position(5);
            sql = mm.readString(GlobalSqlUtil.UTF8);
        } catch (UnsupportedEncodingException e) {

        }
        return sql;
    }

    public static String getSql( ByteBuf buf) {
        byte[] bytes = new byte[buf.readableBytes()];
        buf.readBytes(bytes);
        return getSql(bytes);
    }


    private final static byte[] encodeString(String src, String charset) {
        if (src == null) {
            return null;
        }
        if (charset == null) {
            return src.getBytes();
        }
        try {
            return src.getBytes(charset);
        } catch (UnsupportedEncodingException e) {
            return src.getBytes();
        }
    }
}
