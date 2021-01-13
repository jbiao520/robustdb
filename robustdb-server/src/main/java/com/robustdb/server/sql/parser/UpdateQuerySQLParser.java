package com.robustdb.server.sql.parser;

import com.alibaba.druid.sql.SQLUtils;
import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.statement.SQLUpdateSetItem;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlUpdateStatement;
import com.robustdb.server.model.parser.ParseResult;
import com.robustdb.server.model.parser.UpdateParseResult;

import java.util.List;

public class UpdateQuerySQLParser extends AbstractSQLParser {
    public UpdateQuerySQLParser() {
        this.type = "UPDATE";
    }

    @Override
    protected ParseResult parse(String sql) {
        MySqlUpdateStatement statement = (MySqlUpdateStatement) SQLUtils.parseSingleMysqlStatement(sql);
        String tableName = statement.getTableName().getSimpleName();
        List<SQLUpdateSetItem> updateSetItems =  statement.getItems();
        SQLExpr where = statement.getWhere();
        return UpdateParseResult.builder()
                .tableName(tableName)
                .updateSetItems(updateSetItems)
                .where(where)
                .build();

    }
}
