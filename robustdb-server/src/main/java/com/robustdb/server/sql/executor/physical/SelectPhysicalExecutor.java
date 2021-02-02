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
        if (where instanceof SQLBinaryOpExpr) {
            SQLBinaryOpExpr opWhere = (SQLBinaryOpExpr) where;
            List<SQLBinaryOpExpr> parallelOps = new ArrayList<>();
            Map<String, String> queryCondition = new HashMap<>();
            iterateOPS(opWhere, parallelOps);
            for (SQLBinaryOpExpr parallelOp : parallelOps) {
                SQLIdentifierExpr left = (SQLIdentifierExpr) parallelOp.getLeft();
                SQLExpr right = parallelOp.getRight();
                if (right instanceof SQLIntegerExpr) {
                    SQLIntegerExpr sqlIntegerExpr = (SQLIntegerExpr) right;
                    queryCondition.put(left.getName(), sqlIntegerExpr.getNumber().toString());
                } else if (right instanceof SQLCharExpr) {
                    SQLCharExpr sqlCharExpr = (SQLCharExpr) right;
                    queryCondition.put(left.getName(), sqlCharExpr.getText());
                }

            }

            TableDef indexTableDef = analyzeQueryPlan(parallelOps, tableName);
            List<JsonObject> jsonObjectList = new ArrayList<>();
            if (indexTableDef != null) {
                queryOnIndex(tableDef, queryCondition, indexTableDef, jsonObjectList);

                return formSelectResult(selectList, jsonObjectList);
            }
        }

        return null;
    }

    private ExecutorResult formSelectResult(List<SQLSelectItem> selectList, List<JsonObject> jsonObjectList) {
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
        //byte packetId = eof.packetId;

        for (JsonObject jsonObject : jsonObjectList) {
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

    private void queryOnIndex(TableDef tableDef, Map<String, String> queryCondition, TableDef indexTableDef, List<JsonObject> jsonObjectList) {
        String tableName = tableDef.getTableName();
        String indexName = indexTableDef.getTableName();
        JsonObject indexJson = new JsonObject();
        for (String s : indexTableDef.getColumnDefMap().keySet()) {
            indexJson.addProperty(s, queryCondition.get(s));
        }
        byte[] key = kvClient.getDataNodeData(tableName+"_"+indexName+"_"+indexJson.toString());
        if (key != null) {
            byte[] value = kvClient.getDataNodeData(new String(key));
            if (value != null) {
                String valueJson = new String(value);
                JsonObject jsonObject = gson.fromJson(valueJson, JsonObject.class);
                jsonObjectList.add(jsonObject);
            }
        }

    }

    @Override
    protected boolean compatible(ParseResult parseResult) {
        return parseResult instanceof SelectParseResult;
    }

    private void iterateOPS(SQLBinaryOpExpr sqlBinaryOpExpr, List<SQLBinaryOpExpr> parallelOps) {
        if (sqlBinaryOpExpr.getOperator().isRelational()) {
            parallelOps.add(sqlBinaryOpExpr);
            return;
        } else if (sqlBinaryOpExpr.getOperator().isLogical()) {
            iterateOPS((SQLBinaryOpExpr) sqlBinaryOpExpr.getLeft(), parallelOps);
            iterateOPS((SQLBinaryOpExpr) sqlBinaryOpExpr.getRight(), parallelOps);
        }
    }

    private TableDef analyzeQueryPlan(List<SQLBinaryOpExpr> parallelOps, String tableName) {
        List<TableDef> tableDefs = DefinitionCache.getIndexTableDefs(tableName);
        Set<String> whereCols = new HashSet<>();
        for (SQLBinaryOpExpr parallelOp : parallelOps) {
            SQLIdentifierExpr left = (SQLIdentifierExpr) parallelOp.getLeft();
//            SQLBinaryOperator sqlBinaryOperator = parallelOp.getOperator();
//            SQLIdentifierExpr right = (SQLIdentifierExpr) parallelOp.getRight();
            String columnName = left.getName();
            whereCols.add(columnName);
        }
        for (TableDef tableDef : tableDefs) {
            for (String idxColName : tableDef.getColumnDefMap().keySet()) {
                if (whereCols.contains(idxColName)) {
                    return tableDef;
                }
            }
        }
        return null;
    }
}
