package com.robustdb.server.sql.executor.physical;

import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.expr.SQLCharExpr;
import com.alibaba.druid.sql.ast.expr.SQLIdentifierExpr;
import com.alibaba.druid.sql.ast.expr.SQLIntegerExpr;
import com.alibaba.druid.sql.ast.statement.SQLInsertStatement;
import com.google.gson.JsonObject;
import com.robustdb.server.exception.RobustDBValidationException;
import com.robustdb.server.model.parser.CreateParseResult;
import com.robustdb.server.model.parser.InsertParseResult;
import com.robustdb.server.model.parser.ParseResult;
import com.robustdb.server.util.Requires;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class InsertPhysicalExecutor extends AbstractPhysicalExecutor{

    @Override
    protected void execute(ParseResult parseResult) {
        InsertParseResult insertParseResult = (InsertParseResult)parseResult;
        List<SQLExpr> columns = insertParseResult.getColumns();
        List<SQLInsertStatement.ValuesClause> values = insertParseResult.getValues();
        Requires.requireTrue(columns.size()==values.get(0).getValues().size(),"Column size and value size doesn't match");

        List<JsonObject> jsonObjectList = new ArrayList<>();
        Map<String,String> map = new HashMap<>();
        for (SQLInsertStatement.ValuesClause value : insertParseResult.getValues()) {
            JsonObject valueJson = new JsonObject();
            for(int i=0;i<columns.size();i++){
                SQLIdentifierExpr column = (SQLIdentifierExpr)columns.get(i);
                SQLExpr valueValue = value.getValues().get(i);
                if(valueValue instanceof SQLIntegerExpr){
                    valueJson.addProperty(column.getName(),((SQLIntegerExpr) valueValue).getNumber());
                }else if(valueValue instanceof SQLCharExpr){
                    valueJson.addProperty(column.getName(),((SQLCharExpr) valueValue).getText());
                }else{
                    throw new RobustDBValidationException("Invalid parameter:"+valueValue);
                }
            }
            map.put(valueJson.toString(),valueJson.toString());
        }
        kvClient.insertData(map,insertParseResult.getTableName());
    }

    @Override
    protected boolean compatible(ParseResult parseResult) {
        return parseResult instanceof InsertParseResult;
    }
}
