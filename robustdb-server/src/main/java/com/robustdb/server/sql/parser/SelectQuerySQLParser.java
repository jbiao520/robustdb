package com.robustdb.server.sql.parser;

import com.alibaba.druid.sql.SQLUtils;
import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.statement.*;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlInsertStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlStatement;
import com.robustdb.server.model.parser.InsertParseResult;
import com.robustdb.server.model.parser.ParseResult;
import com.robustdb.server.model.parser.SelectParseResult;

import java.util.List;

public class SelectQuerySQLParser extends AbstractSQLParser {
    public SelectQuerySQLParser() {
        this.type = "SELECT";
    }

    @Override
    protected ParseResult parse(String sql) {
        SQLSelectStatement statement = (SQLSelectStatement) SQLUtils.parseSingleMysqlStatement(sql);
        SQLSelect select = statement.getSelect();
        SQLSelectQueryBlock query = (SQLSelectQueryBlock) select.getQuery();
        SQLExprTableSource tableSource = (SQLExprTableSource) query.getFrom();
        String tableName = tableSource.getExpr().toString();
        SQLExpr where = query.getWhere();
        List<SQLSelectItem> selectList = query.getSelectList();

        return SelectParseResult.builder()
                .tableName(tableName)
                .where(where)
                .selectList(selectList).build();
    }
}