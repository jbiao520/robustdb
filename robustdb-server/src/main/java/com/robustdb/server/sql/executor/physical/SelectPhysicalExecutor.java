package com.robustdb.server.sql.executor.physical;

import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.expr.*;
import com.alibaba.druid.sql.ast.statement.SQLSelectItem;
import com.google.gson.JsonObject;

import com.robustdb.server.model.metadata.TableDef;
import com.robustdb.server.model.parser.ParseResult;
import com.robustdb.server.model.parser.SelectParseResult;
import com.robustdb.server.protocol.mysql.EOFPacket;
import com.robustdb.server.protocol.mysql.FieldPacket;
import com.robustdb.server.protocol.mysql.ResultSetHeaderPacket;
import com.robustdb.server.protocol.mysql.RowDataPacket;
import com.robustdb.server.protocol.response.Fields;
import com.robustdb.server.sql.def.DefinitionCache;
import com.robustdb.server.sql.executor.ExecutorResult;
import com.robustdb.server.util.CharsetUtil;
import com.robustdb.server.util.PacketUtil;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import java.util.*;

public class SelectPhysicalExecutor extends AbstractPhysicalExecutor {

    @Override
    protected ExecutorResult execute(ParseResult parseResult) {

        SelectParseResult result = (SelectParseResult) parseResult;
        String tableName = result.getTableName();
        SQLExpr where = result.getWhere();
        TableDef tableDef = DefinitionCache.getTableDef(tableName);
        List<SQLSelectItem> selectList = result.getSelectList();
//        if (where instanceof SQLBinaryOpExpr) {
//            SQLBinaryOpExpr opWhere = (SQLBinaryOpExpr) where;
//            List<SQLBinaryOpExpr> parallelOps = new ArrayList<>();
//            Map<String, String> queryCondition = new HashMap<>();
//            iterateOPS(opWhere, parallelOps);
//            for (SQLBinaryOpExpr parallelOp : parallelOps) {
//                SQLIdentifierExpr left = (SQLIdentifierExpr) parallelOp.getLeft();
//                SQLExpr right = parallelOp.getRight();
//                if (right instanceof SQLIntegerExpr) {
//                    SQLIntegerExpr sqlIntegerExpr = (SQLIntegerExpr) right;
//                    queryCondition.put(left.getName(), sqlIntegerExpr.getNumber().toString());
//                } else if (right instanceof SQLCharExpr) {
//                    SQLCharExpr sqlCharExpr = (SQLCharExpr) right;
//                    queryCondition.put(left.getName(), sqlCharExpr.getText());
//                }
//            }
//
//            TableDef indexTableDef = analyzeQueryPlan(parallelOps, tableName, tableDef);
//            List<JsonObject> jsonObjectList = null;
//            if (indexTableDef != null) {
//                if (indexTableDef.isIndexTable()) {
//                    jsonObjectList = queryOnIndex(queryCondition, indexTableDef);
//                } else {
//                    jsonObjectList = queryOnPK(queryCondition, indexTableDef);
//                }
//            } else {
//                jsonObjectList = fulltableScan(tableDef, queryCondition);
//            }
//
//        }
        Map<String, JsonObject> map = fetchRequiredRows(tableName, where, tableDef);
        return formSelectResult(selectList, map);
    }

    private ExecutorResult formSelectResult(List<SQLSelectItem> selectList,  Map<String, JsonObject> map) {
        int size = selectList.size();
        ResultSetHeaderPacket header = PacketUtil.getHeader(size);
        FieldPacket[] fields = new FieldPacket[size];

        int i = 0;
        byte packetId = 0;
        header.packetId = ++packetId;
        for (int i1 = 0, splitVarSize = size; i1 < splitVarSize; i1++) {
            int fieldType = Fields.FIELD_TYPE_VAR_STRING;
            String col = selectList.get(i1).getExpr().toString();
            fields[i] = PacketUtil.getField(col, fieldType);
            fields[i].charsetIndex = CharsetUtil.getIndex("utf-8");
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
        for (JsonObject jsonObject : map.values()) {
            RowDataPacket row = new RowDataPacket(size);
            for (int i1 = 0, splitVarSize = size; i1 < splitVarSize; i1++) {
                String col = selectList.get(i1).getExpr().toString();
                String val = jsonObject.get(col).getAsString();
                row.add(val.getBytes());
            }
            row.packetId = ++packetId;
            buffer = row.write(buffer);
        }
        // write lastEof
        EOFPacket lastEof = new EOFPacket();
        lastEof.packetId = ++packetId;
        buffer = lastEof.write(buffer);

        return ExecutorResult.builder().byteBuf(buffer).build();
    }


    @Override
    protected boolean compatible(ParseResult parseResult) {
        return parseResult instanceof SelectParseResult;
    }


}
