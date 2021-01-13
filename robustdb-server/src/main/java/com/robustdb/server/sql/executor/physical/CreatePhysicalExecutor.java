package com.robustdb.server.sql.executor.physical;

import com.alibaba.druid.sql.ast.SQLDataType;
import com.alibaba.druid.sql.ast.expr.SQLIntegerExpr;
import com.alibaba.druid.sql.ast.statement.SQLColumnDefinition;
import com.robustdb.server.client.KVClient;
import com.robustdb.server.client.local.LocalKVClient;
import com.robustdb.server.model.metadata.ColumnDef;
import com.robustdb.server.model.metadata.TableDef;
import com.robustdb.server.model.parser.CreateParseResult;
import com.robustdb.server.model.parser.ParseResult;

import java.util.ArrayList;
import java.util.List;

public class CreatePhysicalExecutor extends AbstractPhysicalExecutor{
    KVClient kvClient = new LocalKVClient();
    @Override
    protected void execute(ParseResult parseResult) {
        CreateParseResult createParseResult = (CreateParseResult)parseResult;
        String tableName = createParseResult.getTableName();
        String rawReq = createParseResult.getRawTableDef();
        List<ColumnDef> columnDefs = new ArrayList<>();
        for (SQLColumnDefinition column : createParseResult.getColumns()) {
            String dataType = column.getDataType().getName();
            SQLIntegerExpr sqlIntegerExpr = (SQLIntegerExpr)column.getDataType().getArguments().get(0);
            String length = sqlIntegerExpr.getNumber().toString();
            ColumnDef columnDef = ColumnDef.builder()
                    .dataType(dataType)
                    .length(length)
                    .name(column.getColumnName())
                    .fullName(column.getColumnName())
                    .table(tableName)
                    .build();
            columnDefs.add(columnDef);
        }
        TableDef tableDef = TableDef.builder()
                .tableName(tableName)
                .rawTableDef(rawReq)
                .columnDefList(columnDefs)
                .build();
        kvClient.createTableMetaData(tableDef);
        kvClient.createDataTable(tableName);
    }

    @Override
    protected boolean compatible(ParseResult parseResult) {
        return parseResult instanceof CreateParseResult;
    }
}
