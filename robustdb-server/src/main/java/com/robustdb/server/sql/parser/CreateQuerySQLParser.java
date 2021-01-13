package com.robustdb.server.sql.parser;

import com.alibaba.druid.sql.SQLUtils;
import com.alibaba.druid.sql.ast.statement.SQLColumnDefinition;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlCreateTableStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlTableIndex;
import com.robustdb.server.model.parser.CreateParseResult;
import com.robustdb.server.model.parser.ParseResult;

import java.util.List;

public class CreateQuerySQLParser extends AbstractSQLParser {
    public CreateQuerySQLParser() {
        this.type = "CREATE";
    }

    @Override
    protected ParseResult parse(String sql) {
        MySqlCreateTableStatement sqlStatement = (MySqlCreateTableStatement) SQLUtils.parseSingleMysqlStatement(sql);
        String tableName = sqlStatement.getTableName();
        List<SQLColumnDefinition> columns = sqlStatement.getColumnDefinitions();
        List<MySqlTableIndex> indexes = sqlStatement.getMysqlIndexes();
        return CreateParseResult.builder().columns(columns).indexList(indexes).tableName(tableName).rawTableDef(sql).build();
    }
}
