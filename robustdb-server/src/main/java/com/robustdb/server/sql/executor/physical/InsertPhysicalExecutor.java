package com.robustdb.server.sql.executor.physical;

import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.expr.SQLCharExpr;
import com.alibaba.druid.sql.ast.expr.SQLIntegerExpr;
import com.google.gson.JsonObject;
import com.robustdb.server.enums.ConstraintType;
import com.robustdb.server.exception.RobustDBValidationException;
import com.robustdb.server.model.metadata.TableDef;
import com.robustdb.server.model.parser.InsertParseResult;
import com.robustdb.server.model.parser.ParseResult;
import com.robustdb.server.sql.def.DefinitionCache;
import com.robustdb.server.util.Requires;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class InsertPhysicalExecutor extends AbstractPhysicalExecutor {

    @Override
    protected void execute(ParseResult parseResult) {
        InsertParseResult insertParseResult = (InsertParseResult) parseResult;
        String tableName = insertParseResult.getTableName();
        List<SQLExpr> columns = insertParseResult.getColumns();
        List<Map<String, SQLExpr>> values = insertParseResult.getValues();
        Requires.requireTrue(columns.size() == values.get(0).size(), "Column size and value size doesn't match");
        TableDef tableDef = DefinitionCache.getTableDef(tableName);
        List<TableDef> indexTableDefs = DefinitionCache.getIndexTableDefs(tableName);

        Map<String, String> map = new HashMap<>();

        Map<String, Map<String, String>> indexMap = new HashMap<>();
        //process each row
        for (Map<String, SQLExpr> rowValueMap : values) {
            JsonObject valueJson = new JsonObject();
            JsonObject keyJson = new JsonObject();
            populateRowValue(tableDef, rowValueMap, valueJson, keyJson, false);
            map.put(keyJson.toString(), valueJson.toString());

            for (TableDef idxDef : indexTableDefs) {
                JsonObject indexJson = new JsonObject();
                populateRowValue(idxDef, rowValueMap, indexJson, null, true);
                if (indexMap.get(idxDef.getTableName()) != null) {
                    indexMap.get(idxDef.getTableName()).put(indexJson.toString(), keyJson.toString());
                } else {
                    Map<String, String> indexVal = new HashMap<>();
                    indexVal.put(indexJson.toString(), keyJson.toString());
                    indexMap.put(idxDef.getTableName(), indexVal);
                }
            }
        }

        kvClient.insertData(map, tableName);
        for (Map.Entry<String, Map<String, String>> idxEntry : indexMap.entrySet()) {
            kvClient.insertData(idxEntry.getValue(), idxEntry.getKey());
        }
    }

    private void populateRowValue(TableDef tableDef, Map<String, SQLExpr> rowValueMap, JsonObject valueJson, JsonObject keyJson, boolean isIndex) {
        for (Map.Entry<String, SQLExpr> rowEntry : rowValueMap.entrySet()) {
            SQLExpr rowValue = rowEntry.getValue();
            String columnName = rowEntry.getKey();
            if (!isIndex) {
                List<ConstraintType> constraints = getConstriants(tableDef, columnName);
                constraintCheck(constraints, rowValue, keyJson);
            }
            if (rowValue instanceof SQLIntegerExpr) {
                if(tableDef.getColumnDefMap().containsKey(rowEntry.getKey())){
                    valueJson.addProperty(columnName, ((SQLIntegerExpr) rowValue).getNumber());
                }
            } else if (rowValue instanceof SQLCharExpr) {
                if(tableDef.getColumnDefMap().containsKey(rowEntry.getKey())){
                    valueJson.addProperty(columnName, ((SQLCharExpr) rowValue).getText());
                }
            } else {
                throw new RobustDBValidationException("Invalid parameter:" + rowValue);
            }
        }
    }

    @Override
    protected boolean compatible(ParseResult parseResult) {
        return parseResult instanceof InsertParseResult;
    }

    private void constraintCheck(List<ConstraintType> constraints, SQLExpr value, JsonObject keyJson) {
        for (ConstraintType constraintType : constraints) {
            if (constraintType == ConstraintType.PK) {
                if (value instanceof SQLIntegerExpr) {
                    keyJson.addProperty("pk", ((SQLIntegerExpr) value).getNumber());
                } else if (value instanceof SQLCharExpr) {
                    keyJson.addProperty("pk", ((SQLCharExpr) value).getText());
                } else {
                    throw new RobustDBValidationException("Invalid parameter:" + value);
                }
            } else if (constraintType == ConstraintType.UNIQUE) {
                //TODO
            } else if (constraintType == ConstraintType.NOTNULL) {
                Requires.requireNonNull(value);
            }
        }
    }
}
