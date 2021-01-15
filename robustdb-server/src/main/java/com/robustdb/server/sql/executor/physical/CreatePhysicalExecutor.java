package com.robustdb.server.sql.executor.physical;

import com.alibaba.druid.sql.ast.expr.SQLIntegerExpr;
import com.alibaba.druid.sql.ast.statement.*;
import com.robustdb.server.enums.ConstraintType;
import com.robustdb.server.model.metadata.ColumnDef;
import com.robustdb.server.model.metadata.TableDef;
import com.robustdb.server.model.parser.CreateParseResult;
import com.robustdb.server.model.parser.ParseResult;
import com.robustdb.server.sql.def.DefinitionCache;

import java.util.*;

public class CreatePhysicalExecutor extends AbstractPhysicalExecutor{
    @Override
    protected void execute(ParseResult parseResult) {
        CreateParseResult createParseResult = (CreateParseResult)parseResult;
        String tableName = createParseResult.getTableName();
        String rawReq = createParseResult.getRawTableDef();
//        List<ColumnDef> columnDefs = new ArrayList<>();
        Map<String, ColumnDef> columnDefMap = new LinkedHashMap<>();
        String pk = "";
        for (SQLColumnDefinition column : createParseResult.getColumns()) {
            String dataType = column.getDataType().getName();
            SQLIntegerExpr sqlIntegerExpr = (SQLIntegerExpr)column.getDataType().getArguments().get(0);
            String length = sqlIntegerExpr.getNumber().toString();
            List<SQLColumnConstraint> columnConstraints= column.getConstraints();
            List<ConstraintType> constraintTypes = new ArrayList<>();
            for (SQLColumnConstraint columnConstraint : columnConstraints) {
                if(columnConstraint instanceof SQLColumnPrimaryKey){
                    constraintTypes.add(ConstraintType.PK);
                    pk = column.getColumnName();
                }else if(columnConstraint instanceof SQLColumnUniqueKey){
                    constraintTypes.add(ConstraintType.UNIQUE);
                }else if(columnConstraint instanceof SQLNotNullConstraint){
                    constraintTypes.add(ConstraintType.NOTNULL);
                }else if(columnConstraint instanceof SQLNullConstraint){
                    constraintTypes.add(ConstraintType.NULL);
                }
            }
            ColumnDef columnDef = ColumnDef.builder()
                    .dataType(dataType)
                    .constraintTypes(constraintTypes)
                    .length(length)
                    .name(column.getColumnName())
                    .fullName(column.getColumnName())
                    .table(tableName)
                    .build();
            columnDefMap.put(column.getColumnName(),columnDef);
        }
        TableDef tableDef = TableDef.builder()
                .tableName(tableName)
                .rawTableDef(rawReq)
                .columnDefMap(columnDefMap)
                .primaryKey(pk)
                .isIndexTable(false)
                .build();
        kvClient.createTableMetaData(tableDef);
        kvClient.createDataTable(tableName);
        DefinitionCache.addTableDef(tableName,tableDef);
    }

    @Override
    protected boolean compatible(ParseResult parseResult) {
        return parseResult instanceof CreateParseResult;
    }
}
