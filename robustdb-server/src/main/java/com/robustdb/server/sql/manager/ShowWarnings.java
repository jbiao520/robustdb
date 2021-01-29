package com.robustdb.server.sql.manager;

import com.robustdb.server.protocol.mysql.EOFPacket;
import com.robustdb.server.protocol.mysql.FieldPacket;
import com.robustdb.server.protocol.mysql.ResultSetHeaderPacket;
import com.robustdb.server.protocol.mysql.RowDataPacket;
import com.robustdb.server.protocol.response.Fields;
import com.robustdb.server.util.PacketUtil;
import com.robustdb.server.util.StringUtil;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import java.util.HashMap;
import java.util.Map;

public class ShowWarnings {

    private static final int FIELD_COUNT = 3;
    private static final ResultSetHeaderPacket header = PacketUtil.getHeader(FIELD_COUNT);
    private static final FieldPacket[] fields = new FieldPacket[FIELD_COUNT];
    private static final EOFPacket eof = new EOFPacket();
    static {
        int i = 0;
        byte packetId = 0;
        header.packetId = ++packetId;

        fields[i] = PacketUtil.getField("Level", Fields.FIELD_TYPE_VAR_STRING);
        fields[i++].packetId = ++packetId;

        fields[i] = PacketUtil.getField("Code", Fields.FIELD_TYPE_VAR_STRING);
        fields[i++].packetId = ++packetId;
        fields[i] = PacketUtil.getField("Message", Fields.FIELD_TYPE_VAR_STRING);
        fields[i++].packetId = ++packetId;
        eof.packetId = ++packetId;
    }

    public static ByteBuf execute() {
        ByteBuf buffer = Unpooled.buffer();

        // write header
        buffer = header.write(buffer);

        // write fields
        for (FieldPacket field : fields) {
            buffer = field.write(buffer);
        }

        // write eof
        buffer = eof.write(buffer);

        // write rows
        byte packetId = eof.packetId;
//        for (Map.Entry<String, String> e : variables.entrySet()) {
//            RowDataPacket row = getRow(e.getKey(), e.getValue(), "utf-8");
//            row.packetId = ++packetId;
//            buffer = row.write(buffer);
//        }

        // write lastEof
        EOFPacket lastEof = new EOFPacket();
        lastEof.packetId = ++packetId;
        buffer = lastEof.write(buffer);

        // write buffer
        return buffer;
    }

    private static RowDataPacket getRow(String name, String value, String charset) {
        RowDataPacket row = new RowDataPacket(FIELD_COUNT);
        row.add(StringUtil.encode(name, charset));
        row.add(StringUtil.encode(value, charset));
        return row;
    }


}
