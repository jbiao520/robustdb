package com.robustdb.server.sql.executor.physical;

import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.expr.SQLBinaryOpExpr;
import com.alibaba.druid.sql.ast.expr.SQLCharExpr;
import com.alibaba.druid.sql.ast.expr.SQLIdentifierExpr;
import com.alibaba.druid.sql.ast.expr.SQLIntegerExpr;
import com.alibaba.druid.sql.ast.statement.SQLUpdateSetItem;
import com.google.gson.JsonObject;
import com.robustdb.server.enums.MysqlDataType;
import com.robustdb.server.model.metadata.TableDef;
import com.robustdb.server.model.parser.ParseResult;
import com.robustdb.server.model.parser.UpdateParseResult;
import com.robustdb.server.protocol.mysql.OkPacket;
import com.robustdb.server.sql.def.DefinitionCache;
import com.robustdb.server.sql.executor.ExecutorResult;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class UpdatePhysicalExecutor extends AbstractPhysicalExecutor{

    @Override
    protected ExecutorResult execute(ParseResult parseResult) {
        UpdateParseResult result = (UpdateParseResult) parseResult;
        String tableName = result.getTableName();
        SQLExpr where = result.getWhere();
        TableDef tableDef = DefinitionCache.getTableDef(tableName);
        List<SQLUpdateSetItem> updateSetItems =  result.getUpdateSetItems();
        Map<String, JsonObject> map = fetchRequiredRows(tableName, where, tableDef);
        Map<String, String> kvs = new HashMap<>();
        for (Map.Entry kv : map.entrySet()) {
            String key = (String) kv.getKey();
            JsonObject jsonObject = (JsonObject) kv.getValue();
            for (SQLUpdateSetItem updateSetItem : updateSetItems) {
                String col = updateSetItem.getColumn().toString();
                SQLExpr rowValue = updateSetItem.getValue();
                if (rowValue instanceof SQLIntegerExpr) {
                    jsonObject.addProperty(col,((SQLIntegerExpr) rowValue).getNumber());
                } else if (rowValue instanceof SQLCharExpr){
                    jsonObject.addProperty(col,((SQLCharExpr) rowValue).getText());
                }
            }
            kvs.put(key,jsonObject.toString());
        }
        kvClient.insertData(kvs);
        ByteBuf bufferOut = Unpooled.buffer();
        bufferOut.writeBytes(OkPacket.OK);
        return ExecutorResult.builder().byteBuf(bufferOut).build();
    }

    @Override
    protected boolean compatible(ParseResult parseResult) {
        return parseResult instanceof UpdateParseResult;
    }
}
