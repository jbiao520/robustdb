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
import com.robustdb.server.protocol.mysql.ErrorPacket;
import com.robustdb.server.protocol.mysql.OkPacket;
import com.robustdb.server.sql.def.DefinitionCache;
import com.robustdb.server.sql.executor.ExecutorResult;
import com.robustdb.server.util.ErrorCode;
import com.robustdb.server.util.Requires;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class InsertPhysicalExecutor extends AbstractPhysicalExecutor {

    @Override
    protected ExecutorResult execute(ParseResult parseResult) {
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
//            JsonObject keyJson = new JsonObject();
            StringBuilder keyBuffer = new StringBuilder(tableName);
            populateRowValue(tableDef, rowValueMap, valueJson, keyBuffer, false);
            String pk = keyBuffer.toString();
            if(validateUK(pk)){
                return getUKError();
            }
            map.put(pk, valueJson.toString());

            for (TableDef idxDef : indexTableDefs) {
                String indexName = idxDef.getTableName();
                JsonObject indexJson = new JsonObject();
                populateRowValue(idxDef, rowValueMap, indexJson, null, true);
                if (indexMap.get(idxDef.getTableName()) != null) {
                    indexMap.get(idxDef.getTableName()).put(indexJson.toString(), keyBuffer.toString());
                } else {
                    Map<String, String> indexVal = new HashMap<>();
                    String keyPrefix = tableName+"_"+indexName+"_"+indexJson.toString();
                    if(idxDef.isUnique()){
                        if(validateUK(keyPrefix)){
                            return getUKError();
                        }
                        indexVal.put(keyPrefix,keyBuffer.toString());
                    }else{
                        String key = keyPrefix+"_"+keyBuffer.toString();
                        indexVal.put(key,null);
                    }

                    indexMap.put(indexName, indexVal);
                }
            }
        }

        kvClient.insertData(map);
        for (Map.Entry<String, Map<String, String>> idxEntry : indexMap.entrySet()) {
            kvClient.insertData(idxEntry.getValue());
        }
        ByteBuf byteBuf = Unpooled.buffer();
        byteBuf.writeBytes(OkPacket.OK);
        return ExecutorResult.builder().byteBuf(byteBuf).build();
    }

    private ExecutorResult getUKError() {
        ErrorPacket error = new ErrorPacket();
        error.packetId = 1;
        error.errno = ErrorCode.ER_INSERT_INFO;
        error.message = "Duplicated record found".getBytes();
        ByteBuf byteBuf =error.write();
        return ExecutorResult.builder().byteBuf(byteBuf).build();
    }

    private boolean validateUK(String key){
        return kvClient.containsKeyInDataNode(key);
    }

    private void populateRowValue(TableDef tableDef, Map<String, SQLExpr> rowValueMap, JsonObject valueJson, StringBuilder keyBuffer, boolean isIndex) {
        for (Map.Entry<String, SQLExpr> rowEntry : rowValueMap.entrySet()) {
            SQLExpr rowValue = rowEntry.getValue();
            String columnName = rowEntry.getKey();
            if (!isIndex) {
                List<ConstraintType> constraints = getConstriants(tableDef, columnName);
                constraintCheck(constraints, rowValue, keyBuffer);
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

    private void constraintCheck(List<ConstraintType> constraints, SQLExpr value, StringBuilder keyBuffer) {
        for (ConstraintType constraintType : constraints) {
            if (constraintType == ConstraintType.PK) {
                if (value instanceof SQLIntegerExpr) {
                    keyBuffer.append("_");
                    keyBuffer.append(((SQLIntegerExpr) value).getNumber());
//                    keyJson.addProperty("pk", ((SQLIntegerExpr) value).getNumber());
                } else if (value instanceof SQLCharExpr) {
                    keyBuffer.append("_");
                    keyBuffer.append(((SQLCharExpr) value).getText());
//                    keyJson.addProperty("pk", ((SQLCharExpr) value).getText());
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
