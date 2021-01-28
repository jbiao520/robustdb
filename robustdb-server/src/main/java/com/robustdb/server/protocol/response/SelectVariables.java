package com.robustdb.server.protocol.response;


import com.google.common.base.Splitter;
import com.robustdb.server.protocol.mysql.EOFPacket;
import com.robustdb.server.protocol.mysql.FieldPacket;
import com.robustdb.server.protocol.mysql.ResultSetHeaderPacket;
import com.robustdb.server.protocol.mysql.RowDataPacket;
import com.robustdb.server.util.PacketUtil;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author mycat
 */
public final class SelectVariables {
    private static final Logger LOGGER = LoggerFactory.getLogger(SelectVariables.class);


    public static ByteBuf execute(String sql) {

        String subSql = sql.substring(sql.indexOf("SELECT") + 6);
        List<String> splitVar = Splitter.on(",").omitEmptyStrings().trimResults().splitToList(subSql);
        splitVar = convert(splitVar);
        int FIELD_COUNT = splitVar.size();
        ResultSetHeaderPacket header = PacketUtil.getHeader(FIELD_COUNT);
        FieldPacket[] fields = new FieldPacket[FIELD_COUNT];

        int i = 0;
        byte packetId = 1;
        header.packetId = ++packetId;
        for (int i1 = 0, splitVarSize = splitVar.size(); i1 < splitVarSize; i1++) {
            String s = splitVar.get(i1);
            fields[i] = PacketUtil.getField(s, Fields.FIELD_TYPE_VAR_STRING);
            fields[i++].packetId = ++packetId;
        }


        ByteBuf buffer = Unpooled.buffer();

        // write header
        buffer = header.write(buffer);

        // write fields
        for (FieldPacket field : fields) {
            buffer = field.write(buffer);
        }


        EOFPacket eof = new EOFPacket();
        eof.packetId = ++packetId;
        // write eof
        buffer = eof.write(buffer);

        // write rows
        //byte packetId = eof.packetId;

        RowDataPacket row = new RowDataPacket(FIELD_COUNT);
        for (int i1 = 0, splitVarSize = splitVar.size(); i1 < splitVarSize; i1++) {
            String s = splitVar.get(i1);
            String value = variables.get(s) == null ? "" : variables.get(s);
            row.add(value.getBytes());

        }

        row.packetId = ++packetId;
        buffer = row.write(buffer);


        // write lastEof
        EOFPacket lastEof = new EOFPacket();
        lastEof.packetId = ++packetId;
        buffer = lastEof.write(buffer);

        // write buffer
        return buffer;
    }

    private static List<String> convert(List<String> in) {
        List<String> out = new ArrayList<>();
        for (String s : in) {
            int asIndex = s.toUpperCase().indexOf(" AS ");
            if (asIndex != -1) {
                out.add(s.substring(asIndex + 4));
            }
        }
        if (out.isEmpty()) {
            return in;
        } else {
            return out;
        }


    }


    private static final Map<String, String> variables = new HashMap<String, String>();

    static {
        variables.put("@@character_set_client", "utf8");
        variables.put("@@character_set_connection", "utf8");
        variables.put("@@character_set_results", "utf8");
        variables.put("@@character_set_server", "utf8");
        variables.put("@@init_connect", "");
        variables.put("@@interactive_timeout", "172800");
        variables.put("@@license", "GPL");
        variables.put("@@lower_case_table_names", "1");
        variables.put("@@max_allowed_packet", "16777216");
        variables.put("@@net_buffer_length", "16384");
        variables.put("@@net_write_timeout", "60");
        variables.put("@@query_cache_size", "0");
        variables.put("@@query_cache_type", "OFF");
        variables.put("@@sql_mode", "STRICT_TRANS_TABLES");
        variables.put("@@system_time_zone", "CST");
        variables.put("@@time_zone", "SYSTEM");
        variables.put("@@tx_isolation", "REPEATABLE-READ");
        variables.put("@@wait_timeout", "172800");
        variables.put("@@session.auto_increment_increment", "1");

        variables.put("character_set_client", "utf8");
        variables.put("character_set_connection", "utf8");
        variables.put("character_set_results", "utf8");
        variables.put("character_set_server", "utf8");
        variables.put("init_connect", "");
        variables.put("interactive_timeout", "172800");
        variables.put("license", "GPL");
        variables.put("lower_case_table_names", "1");
        variables.put("max_allowed_packet", "16777216");
        variables.put("net_buffer_length", "16384");
        variables.put("net_write_timeout", "60");
        variables.put("query_cache_size", "0");
        variables.put("query_cache_type", "OFF");
        variables.put("sql_mode", "STRICT_TRANS_TABLES");
        variables.put("system_time_zone", "CST");
        variables.put("time_zone", "SYSTEM");
        variables.put("tx_isolation", "REPEATABLE-READ");
        variables.put("wait_timeout", "172800");
        variables.put("auto_increment_increment", "1");
    }


    public static void main(String[] args) {
        String sql = "SELECT @@session.auto_increment_increment AS auto_increment_increment, @@character_set_client AS character_set_client, @@character_set_connection AS character_set_connection, @@character_set_results AS character_set_results, @@character_set_server AS character_set_server, @@init_connect AS init_connect, @@interactive_timeout AS interactive_timeout, @@license AS license, @@lower_case_table_names AS lower_case_table_names, @@max_allowed_packet AS max_allowed_packet, @@net_buffer_length AS net_buffer_length, @@net_write_timeout AS net_write_timeout, @@query_cache_size AS query_cache_size, @@query_cache_type AS query_cache_type, @@sql_mode AS sql_mode, @@system_time_zone AS system_time_zone, @@time_zone AS time_zone, @@tx_isolation AS tx_isolation, @@wait_timeout AS wait_timeout ";
        SelectVariables.execute(sql);
    }
}