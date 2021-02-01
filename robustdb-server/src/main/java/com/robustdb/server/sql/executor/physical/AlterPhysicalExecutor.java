package com.robustdb.server.sql.executor.physical;

import com.alibaba.druid.sql.ast.expr.SQLIdentifierExpr;
import com.robustdb.server.enums.AlterType;
import com.robustdb.server.model.metadata.ColumnDef;
import com.robustdb.server.model.metadata.TableDef;
import com.robustdb.server.model.parser.AlterParseResult;
import com.robustdb.server.model.parser.InsertParseResult;
import com.robustdb.server.model.parser.ParseResult;
import com.robustdb.server.protocol.mysql.OkPacket;
import com.robustdb.server.sql.def.DefinitionCache;
import com.robustdb.server.sql.executor.ExecutorResult;
import com.robustdb.server.util.Constants;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class AlterPhysicalExecutor extends AbstractPhysicalExecutor {

    @Override
    protected ExecutorResult execute(ParseResult parseResult) {
        AlterParseResult alterParseResult = (AlterParseResult) parseResult;
        String tableName = alterParseResult.getTableName();
        List<SQLIdentifierExpr> columns = alterParseResult.getColumns();
        AlterType type = alterParseResult.getAlterType();
        if(type==AlterType.ADDINDEX){
            String indexName = alterParseResult.getIndexName();
            String indexTableName = tableName+Constants.INDEX_SEPERATER+indexName;
            Map<String, ColumnDef> columnDefMap = new LinkedHashMap<>();
            for (SQLIdentifierExpr column : columns) {
                ColumnDef columnDef = ColumnDef.builder()
                        .name(column.getName())
                        .table(indexTableName)
                        .build();
                columnDefMap.put(column.getName(),columnDef);
            }
            TableDef tableDef = TableDef.builder().rawTableDef(alterParseResult.getRawSQL())
                    .tableName(indexTableName)
                    .primaryKey(Constants.IDX_TBL_PK)
                    .columnDefMap(columnDefMap)
                    .isIndexTable(true)
                    .build();
            kvClient.createTableMetaData(tableDef);
            kvClient.createDataTable(indexTableName);
            DefinitionCache.addIndexTableDef(tableName,tableDef);
        }
        ByteBuf byteBuf = Unpooled.buffer();
        byteBuf.writeBytes(OkPacket.OK);
        return ExecutorResult.builder().byteBuf(byteBuf).build();
    }

    @Override
    protected boolean compatible(ParseResult parseResult) {
        return parseResult instanceof AlterParseResult;
    }
}
