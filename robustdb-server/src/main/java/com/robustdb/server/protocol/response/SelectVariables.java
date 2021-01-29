package com.robustdb.server.protocol.response;


import com.alibaba.druid.sql.ast.statement.SQLSelectItem;
import com.google.common.base.Splitter;
import com.robustdb.server.protocol.mysql.EOFPacket;
import com.robustdb.server.protocol.mysql.FieldPacket;
import com.robustdb.server.protocol.mysql.ResultSetHeaderPacket;
import com.robustdb.server.protocol.mysql.RowDataPacket;
import com.robustdb.server.util.CharsetUtil;
import com.robustdb.server.util.PacketUtil;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.checkerframework.checker.units.qual.A;
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

    private static List<String> convertToStr(List<SQLSelectItem> sqlSelectItems) {
        List<String> strings = new ArrayList<>();
        for (SQLSelectItem sqlSelectItem : sqlSelectItems) {
            strings.add(sqlSelectItem.getExpr().toString().toLowerCase());
        }
        return strings;
    }

    public static ByteBuf execute(String sql, List<SQLSelectItem> sqlSelectItems) {
        List<String> splitVar;
        if (sqlSelectItems == null) {
            String subSql = "";
            if (sql.contains("SELECT")) {
                subSql = sql.substring(sql.indexOf("SELECT") + 6);
            } else if (sql.contains("select")) {
                subSql = sql.substring(sql.indexOf("select") + 6);
            }

            splitVar = Splitter.on(",").omitEmptyStrings().trimResults().splitToList(subSql);
            splitVar = convert(splitVar);
        }else{
            splitVar = convertToStr(sqlSelectItems);
        }
        int FIELD_COUNT = splitVar.size();
        ResultSetHeaderPacket header = PacketUtil.getHeader(FIELD_COUNT);
        FieldPacket[] fields = new FieldPacket[FIELD_COUNT];

        int i = 0;
        byte packetId = 0;
        header.packetId = ++packetId;
        for (int i1 = 0, splitVarSize = splitVar.size(); i1 < splitVarSize; i1++) {
            String s = splitVar.get(i1);
            int fieldType = types.get(s)==null?Fields.FIELD_TYPE_VAR_STRING:types.get(s);
            fields[i] = PacketUtil.getField(s, fieldType);
            fields[i].charsetIndex = CharsetUtil.getIndex(encodes.get(fieldType));
            fields[i].length = fields[i].calcPacketSize();
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
        variables.put("@@sql_mode", "ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_AUTO_CREATE_USER,NO_ENGINE_SUBSTITUTION");
        variables.put("@@system_time_zone", "CST");
        variables.put("@@time_zone", "SYSTEM");
        variables.put("@@transaction_isolation", "REPEATABLE-READ");
        variables.put("@@wait_timeout", "172800");
        variables.put("@@session.auto_increment_increment", "1");
        variables.put("@@session.collation_server", "latin1_swedish_ci");
        variables.put("@@session.collation_connection", "utf8_general_ci");
        variables.put("@@session.performance_schema", "1");
        variables.put("@@version_comment", "robust wip");

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
        variables.put("sql_mode", "ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_AUTO_CREATE_USER,NO_ENGINE_SUBSTITUTION");
        variables.put("system_time_zone", "CST");
        variables.put("time_zone", "SYSTEM");
        variables.put("transaction_isolation", "REPEATABLE-READ");
        variables.put("wait_timeout", "172800");
        variables.put("auto_increment_increment", "1");
        variables.put("collation_server", "latin1_swedish_ci");
        variables.put("collation_connection", "utf8_general_ci");
        variables.put("performance_schema", "1");
        variables.put("database()", "robustdb");
        variables.put("schema()", "robustdb");
        variables.put("left(user(),instr(concat(user(),'@'),'@')-1)", "robustdb");
        variables.put("version()", "0.0.1");
        variables.put("USER()", "jianguo");

    }

    private static final Map<String, Integer> types = new HashMap<String, Integer>();

    static {
        types.put("@@character_set_client", Fields.FIELD_TYPE_VAR_STRING);
        types.put("@@character_set_connection", Fields.FIELD_TYPE_VAR_STRING);
        types.put("@@character_set_results", Fields.FIELD_TYPE_VAR_STRING);
        types.put("@@character_set_server", Fields.FIELD_TYPE_VAR_STRING);
        types.put("@@init_connect", Fields.FIELD_TYPE_VAR_STRING);
        types.put("@@interactive_timeout", Fields.FIELD_TYPE_LONGLONG);
        types.put("@@license", Fields.FIELD_TYPE_VAR_STRING);
        types.put("@@lower_case_table_names", Fields.FIELD_TYPE_LONGLONG);
        types.put("@@max_allowed_packet", Fields.FIELD_TYPE_LONGLONG);
        types.put("@@net_buffer_length", Fields.FIELD_TYPE_LONGLONG);
        types.put("@@net_write_timeout", Fields.FIELD_TYPE_LONGLONG);
        types.put("@@query_cache_size", Fields.FIELD_TYPE_LONGLONG);
        types.put("@@query_cache_type", Fields.FIELD_TYPE_VAR_STRING);
        types.put("@@sql_mode", Fields.FIELD_TYPE_VAR_STRING);
        types.put("@@system_time_zone", Fields.FIELD_TYPE_VAR_STRING);
        types.put("@@time_zone", Fields.FIELD_TYPE_VAR_STRING);
        types.put("@@transaction_isolation", Fields.FIELD_TYPE_VAR_STRING);
        types.put("@@wait_timeout", Fields.FIELD_TYPE_LONGLONG);
        types.put("@@session.auto_increment_increment", Fields.FIELD_TYPE_LONGLONG);
        types.put("@@session.collation_server", Fields.FIELD_TYPE_VAR_STRING);
        types.put("@@session.collation_connection", Fields.FIELD_TYPE_VAR_STRING);
        types.put("@@session.performance_schema", Fields.FIELD_TYPE_LONGLONG);
        types.put("@@version_comment", Fields.FIELD_TYPE_VAR_STRING);

        types.put("character_set_client", Fields.FIELD_TYPE_VAR_STRING);
        types.put("character_set_connection", Fields.FIELD_TYPE_VAR_STRING);
        types.put("character_set_results", Fields.FIELD_TYPE_VAR_STRING);
        types.put("character_set_server", Fields.FIELD_TYPE_VAR_STRING);
        types.put("init_connect", Fields.FIELD_TYPE_VAR_STRING);
        types.put("interactive_timeout", Fields.FIELD_TYPE_LONGLONG);
        types.put("license", Fields.FIELD_TYPE_VAR_STRING);
        types.put("lower_case_table_names", Fields.FIELD_TYPE_LONGLONG);
        types.put("max_allowed_packet", Fields.FIELD_TYPE_LONGLONG);
        types.put("net_buffer_length", Fields.FIELD_TYPE_LONGLONG);
        types.put("net_write_timeout", Fields.FIELD_TYPE_LONGLONG);
        types.put("query_cache_size", Fields.FIELD_TYPE_LONGLONG);
        types.put("query_cache_type", Fields.FIELD_TYPE_VAR_STRING);
        types.put("sql_mode", Fields.FIELD_TYPE_VAR_STRING);
        types.put("system_time_zone", Fields.FIELD_TYPE_VAR_STRING);
        types.put("time_zone", Fields.FIELD_TYPE_VAR_STRING);
        types.put("transaction_isolation", Fields.FIELD_TYPE_VAR_STRING);
        types.put("wait_timeout", Fields.FIELD_TYPE_LONGLONG);
        types.put("auto_increment_increment", Fields.FIELD_TYPE_LONGLONG);
        types.put("collation_server", Fields.FIELD_TYPE_VAR_STRING);
        types.put("collation_connection", Fields.FIELD_TYPE_VAR_STRING);
        types.put("performance_schema", Fields.FIELD_TYPE_LONGLONG);
        types.put("database()", Fields.FIELD_TYPE_VAR_STRING);
        types.put("schema()", Fields.FIELD_TYPE_VAR_STRING);
        types.put("version()", Fields.FIELD_TYPE_VAR_STRING);
        types.put("left(user(),instr(concat(user(),'@'),'@')-1)", Fields.FIELD_TYPE_VAR_STRING);
        types.put("USER()", Fields.FIELD_TYPE_VAR_STRING);
    }

    private static final Map<Integer, String> encodes = new HashMap();

    static {
        encodes.put(Fields.FIELD_TYPE_VAR_STRING, "utf-8");
        encodes.put(Fields.FIELD_TYPE_LONGLONG, "iso-8859-1");
    }

}