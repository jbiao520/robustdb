package com.robustdb.server.sql.executor.physical;

import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.expr.SQLBinaryOpExpr;
import com.alibaba.druid.sql.ast.expr.SQLCharExpr;
import com.alibaba.druid.sql.ast.expr.SQLIdentifierExpr;
import com.alibaba.druid.sql.ast.expr.SQLIntegerExpr;
import com.alibaba.druid.sql.ast.statement.SQLSelectItem;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.robustdb.server.client.KVClient;
import com.robustdb.server.client.local.LocalKVClient;
import com.robustdb.server.enums.ConstraintType;
import com.robustdb.server.model.metadata.TableDef;
import com.robustdb.server.model.parser.ParseResult;
import com.robustdb.server.protocol.mysql.*;
import com.robustdb.server.protocol.response.Fields;
import com.robustdb.server.sql.def.DefinitionCache;
import com.robustdb.server.sql.executor.ExecutorResult;
import com.robustdb.server.util.CharsetUtil;
import com.robustdb.server.util.PacketUtil;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import java.util.*;

public abstract class AbstractPhysicalExecutor {

    protected AbstractPhysicalExecutor nextExecutor;
    protected Gson gson = new Gson();
    protected KVClient kvClient = new LocalKVClient();

    public void setNextExecutor(AbstractPhysicalExecutor sqlExectuor) {
        this.nextExecutor = sqlExectuor;
    }

    public ExecutorResult executePlan(ParseResult parseResult) {
        if (compatible(parseResult)) {
            return execute(parseResult);
        }
        if (nextExecutor != null) {
            return nextExecutor.executePlan(parseResult);
        }
        ByteBuf byteBuf = Unpooled.buffer();
        byteBuf.writeBytes(OkPacket.OK);
        return ExecutorResult.builder().byteBuf(byteBuf).build();
    }

    abstract protected ExecutorResult execute(ParseResult parseResult);

    abstract protected boolean compatible(ParseResult parseResult);

    protected List<ConstraintType> getConstriants(TableDef tableDef, String columnName) {
        return tableDef.getColumnDefMap().get(columnName).getConstraintTypes();
    }

    protected void iterateOPS(SQLBinaryOpExpr sqlBinaryOpExpr, List<SQLBinaryOpExpr> parallelOps) {
        if (sqlBinaryOpExpr.getOperator().isRelational()) {
            parallelOps.add(sqlBinaryOpExpr);
            return;
        } else if (sqlBinaryOpExpr.getOperator().isLogical()) {
            iterateOPS((SQLBinaryOpExpr) sqlBinaryOpExpr.getLeft(), parallelOps);
            iterateOPS((SQLBinaryOpExpr) sqlBinaryOpExpr.getRight(), parallelOps);
        }
    }

    protected Map<String, JsonObject> fulltableScan(TableDef tableDef, Map<String, String> queryCondition) {
        int tableId = tableDef.getTableId();
        return kvClient.fullTableScan(queryCondition, tableId + "_");

    }


    protected Map<String, JsonObject> queryOnPK(Map<String, String> queryCondition, TableDef tableDef) {
        Map<String, JsonObject> map = new HashMap<>();
        int tableId = tableDef.getTableId();
        String key = tableId + "_" + queryCondition.get(tableDef.getPrimaryKey());
        byte[] value = kvClient.getDataNodeData(key);
        if (value != null) {
            String valueJson = new String(value);
            JsonObject jsonObject = gson.fromJson(valueJson, JsonObject.class);
            map.put(key, jsonObject);
        }
        return map;
    }

    protected Map<String, JsonObject> queryOnIndex(Map<String, String> queryCondition, TableDef indexTableDef) {
        Map<String, JsonObject> map = new HashMap<>();
        List<JsonObject> jsonObjectList = new ArrayList<>();
        int indexId = indexTableDef.getTableId();
        JsonObject indexJson = new JsonObject();
        for (String s : indexTableDef.getColumnDefMap().keySet()) {
            indexJson.addProperty(s, queryCondition.get(s));
        }
        String keyHint = indexId + "_" + indexJson.toString();
        if (indexTableDef.isUnique()) {
            byte[] key = kvClient.getDataNodeData(keyHint);
            if (key != null) {
                String keyStr = new String(key);
                byte[] value = kvClient.getDataNodeData(keyStr);
                if (value != null) {
                    String valueJson = new String(value);
                    JsonObject jsonObject = gson.fromJson(valueJson, JsonObject.class);
                    jsonObjectList.add(jsonObject);
                    map.put(keyStr, jsonObject);
                }
            }
        } else {
            List<String> keys = kvClient.getSecondaryIndexesOnDataNode(keyHint);
            for (String key : keys) {
                String pk = key.replace(keyHint + "_", "");
                if (pk != null) {
                    byte[] value = kvClient.getDataNodeData(pk);
                    if (value != null) {
                        String valueJson = new String(value);
                        JsonObject jsonObject = gson.fromJson(valueJson, JsonObject.class);
                        jsonObjectList.add(jsonObject);
                        map.put(key, jsonObject);
                    }
                }
            }
        }
        return map;
    }

    protected TableDef analyzeQueryPlan(List<SQLBinaryOpExpr> parallelOps, String tableName, TableDef originalTableDef) {
        List<TableDef> tableDefs = DefinitionCache.getIndexTableDefs(tableName);
        Set<String> whereCols = new HashSet<>();
        for (SQLBinaryOpExpr parallelOp : parallelOps) {
            SQLIdentifierExpr left = (SQLIdentifierExpr) parallelOp.getLeft();
//            SQLBinaryOperator sqlBinaryOperator = parallelOp.getOperator();
//            SQLIdentifierExpr right = (SQLIdentifierExpr) parallelOp.getRight();
            String columnName = left.getName();
            whereCols.add(columnName);
        }
        if (whereCols.contains(originalTableDef.getPrimaryKey())) {
            //pk found
            return originalTableDef;
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

    protected Map<String, JsonObject> fetchRequiredRows(String tableName, SQLExpr where, TableDef tableDef) {
        Map<String, JsonObject> map = new HashMap<>();
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
            TableDef indexTableDef = analyzeQueryPlan(parallelOps, tableName, tableDef);
            if (indexTableDef != null) {
                if (indexTableDef.isIndexTable()) {
                    map.putAll(queryOnIndex(queryCondition, indexTableDef));
                } else {
                    map.putAll(queryOnPK(queryCondition, indexTableDef));
                }
            } else {
                map.putAll(fulltableScan(tableDef, queryCondition));
            }
        }
        return map;
    }
}
