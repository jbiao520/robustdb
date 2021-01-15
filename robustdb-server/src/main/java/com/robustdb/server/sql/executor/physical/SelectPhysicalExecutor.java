package com.robustdb.server.sql.executor.physical;

import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.expr.*;
import com.alibaba.druid.sql.ast.statement.SQLSelectItem;
import com.google.gson.JsonObject;
import com.robustdb.server.model.metadata.TableDef;
import com.robustdb.server.model.parser.ParseResult;
import com.robustdb.server.model.parser.SelectParseResult;
import com.robustdb.server.sql.def.DefinitionCache;

import java.util.*;

public class SelectPhysicalExecutor extends AbstractPhysicalExecutor {

    @Override
    protected void execute(ParseResult parseResult) {

        SelectParseResult result = (SelectParseResult) parseResult;
        String tableName = result.getTableName();
        SQLExpr where = result.getWhere();
        TableDef tableDef = DefinitionCache.getTableDef(tableName);
        List<SQLSelectItem> selectList = result.getSelectList();
        if (where instanceof SQLBinaryOpExpr) {
            SQLBinaryOpExpr opWhere = (SQLBinaryOpExpr) where;
            List<SQLBinaryOpExpr> parallelOps = new ArrayList<>();
            Map<String,String> queryCondition = new HashMap<>();
            iterateOPS(opWhere, parallelOps);
            for (SQLBinaryOpExpr parallelOp : parallelOps) {
                SQLIdentifierExpr left = (SQLIdentifierExpr)parallelOp.getLeft();
                SQLExpr right = parallelOp.getRight();
                if(right instanceof SQLIntegerExpr){
                    SQLIntegerExpr sqlIntegerExpr = (SQLIntegerExpr)right;
                    queryCondition.put(left.getName(),sqlIntegerExpr.getNumber().toString());
                }else if(right instanceof SQLCharExpr){
                    SQLCharExpr sqlCharExpr = (SQLCharExpr)right;
                    queryCondition.put(left.getName(),sqlCharExpr.getText());
                }

            }

            TableDef indexTableDef = analyzeQueryPlan(parallelOps,tableName);
            if(indexTableDef!=null){
                JsonObject indexJson = new JsonObject();
                for (String s : indexTableDef.getColumnDefMap().keySet()) {
                    indexJson.addProperty(s,queryCondition.get(s));
                }
                byte[] key = kvClient.getDataData(indexJson.toString(),indexTableDef.getTableName());
                byte[] value = kvClient.getDataData(new String(key),tableDef.getTableName());
                String valueJson = new String(value);
                JsonObject jsonObject = gson.fromJson(valueJson,JsonObject.class);
                System.out.println(jsonObject.toString());
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
                if(whereCols.contains(idxColName)){
                    return tableDef;
                }
            }
        }
        return null;
    }
}
