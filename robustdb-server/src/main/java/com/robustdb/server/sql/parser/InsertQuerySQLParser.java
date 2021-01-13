package com.robustdb.server.sql.parser;

import com.alibaba.druid.sql.SQLUtils;
import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.statement.SQLInsertStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlInsertStatement;
import com.robustdb.server.model.parser.InsertParseResult;
import com.robustdb.server.model.parser.ParseResult;

import java.util.List;

public class InsertQuerySQLParser extends AbstractSQLParser {
    public InsertQuerySQLParser() {
        this.type = "INSERT";
    }

    @Override
    protected ParseResult parse(String sql) {
        MySqlInsertStatement sqlStatement = (MySqlInsertStatement) SQLUtils.parseSingleMysqlStatement(sql);
        String tableName = sqlStatement.getTableName().getSimpleName();
        List<SQLExpr> columns = sqlStatement.getColumns();
        List<SQLInsertStatement.ValuesClause> values = sqlStatement.getValuesList();
        return InsertParseResult.builder().columns(columns).values(values).tableName(tableName).build();
    }
}
