package com.robustdb.server.sql.parser;

import com.alibaba.druid.sql.ast.SQLIndexDefinition;
import com.alibaba.druid.sql.ast.SQLName;
import com.alibaba.druid.sql.ast.expr.SQLIdentifierExpr;
import com.alibaba.druid.sql.ast.statement.*;
import com.alibaba.druid.sql.dialect.mysql.parser.MySqlStatementParser;
import com.robustdb.server.enums.AlterType;
import com.robustdb.server.model.parser.AlterParseResult;
import com.robustdb.server.model.parser.ParseResult;
import com.robustdb.server.util.Constants;

import java.util.ArrayList;
import java.util.List;

public class AlterQuerySQLParser extends AbstractSQLParser {
    public AlterQuerySQLParser() {
        this.type = "ALTER";
    }

    @Override
    protected ParseResult parse(String sql) {
        MySqlStatementParser parser = new MySqlStatementParser(sql);
        SQLAlterTableStatement alter = (SQLAlterTableStatement) parser.parseStatement();
        String tableName = alter.getTableName();
        List<SQLIdentifierExpr> sqlIdentifierExprs = new ArrayList<>();
        for (SQLAlterTableItem item : alter.getItems()) {
            if (item instanceof SQLAlterTableDropIndex) {
                return handleDropIndex(tableName, (SQLAlterTableDropIndex) item, sqlIdentifierExprs, sql);
            } else if (item instanceof SQLAlterTableDropColumnItem) {
                SQLAlterTableDropColumnItem dropColumn = (SQLAlterTableDropColumnItem) item;
                for (SQLName column : dropColumn.getColumns()) {
                    sqlIdentifierExprs.add((SQLIdentifierExpr) column);
                }
            } else if (item instanceof SQLAlterTableAddIndex) {
                return handleAddIndex(tableName, (SQLAlterTableAddIndex) item, sqlIdentifierExprs, sql);
            } else if (item instanceof SQLAlterTableAddColumn) {
                return handleAddColumn(tableName, (SQLAlterTableAddColumn) item, sqlIdentifierExprs, sql);
            }
        }
        return null;
    }

    private AlterParseResult handleAddIndex(String tableName, SQLAlterTableAddIndex addIndex, List<SQLIdentifierExpr> sqlIdentifierExprs, String sql) {
        String indexName = addIndex.getName().getSimpleName();
        SQLIndexDefinition sqlIndexDefinition = addIndex.getIndexDefinition();
        for (SQLSelectOrderByItem column : sqlIndexDefinition.getColumns()) {
            sqlIdentifierExprs.add((SQLIdentifierExpr) column.getExpr());
        }
        return AlterParseResult.builder()
                .alterType(AlterType.ADDINDEX)
                .tableName(tableName)
                .indexName(indexName)
                .columns(sqlIdentifierExprs)
                .rawSQL(sql)
                .build();
    }

    private AlterParseResult handleDropIndex(String tableName, SQLAlterTableDropIndex dropIndex, List<SQLIdentifierExpr> sqlIdentifierExprs, String sql) {
        String indexName = dropIndex.getIndexName().getSimpleName();
        return AlterParseResult.builder()
                .alterType(AlterType.DROPINDEX)
                .tableName(tableName)
                .indexName(indexName)
                .rawSQL(sql)
                .build();
    }

    private AlterParseResult handleAddColumn(String tableName, SQLAlterTableAddColumn addColumn, List<SQLIdentifierExpr> sqlIdentifierExprs, String sql) {
        //TODO
        return AlterParseResult.builder()
                .alterType(AlterType.ADDCOLUMN)
                .tableName(tableName)
                .rawSQL(sql)
                .build();
    }
}
