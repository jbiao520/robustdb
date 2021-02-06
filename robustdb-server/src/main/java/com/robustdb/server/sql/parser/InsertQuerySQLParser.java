package com.robustdb.server.sql.parser;

import com.alibaba.druid.sql.SQLUtils;
import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.expr.SQLIdentifierExpr;
import com.alibaba.druid.sql.ast.statement.SQLInsertStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlInsertStatement;
import com.alibaba.druid.util.JdbcConstants;
import com.robustdb.server.model.parser.InsertParseResult;
import com.robustdb.server.model.parser.MultiInsertParseResult;
import com.robustdb.server.model.parser.ParseResult;
import org.checkerframework.checker.units.qual.A;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class InsertQuerySQLParser extends AbstractSQLParser {
    public InsertQuerySQLParser() {
        this.type = "INSERT";
    }

    @Override
    protected ParseResult parse(String sql) {
        List<SQLStatement> sqlInsertStatements = SQLUtils.parseStatements(sql, JdbcConstants.MYSQL);
        List<InsertParseResult> insertParseResults = new ArrayList<>();
        for (SQLStatement sqlInsertStatement : sqlInsertStatements) {
            MySqlInsertStatement sqlStatement = (MySqlInsertStatement)sqlInsertStatement;
            InsertParseResult parseResult = parseSingle(sqlStatement);
            insertParseResults.add(parseResult);
        }

        return MultiInsertParseResult.builder().insertParseResult(insertParseResults).build();
    }


    protected InsertParseResult parseSingle(MySqlInsertStatement sqlStatement) {
        String tableName = sqlStatement.getTableName().getSimpleName();
        List<SQLExpr> columns = sqlStatement.getColumns();
        List<SQLInsertStatement.ValuesClause> values = sqlStatement.getValuesList();
        List<Map<String,SQLExpr>> list = new ArrayList<>();
        for (SQLInsertStatement.ValuesClause value : values) {
            Map<String,SQLExpr> map = new HashMap<>();
            for (int i = 0; i < value.getValues().size(); i++) {
                SQLExpr valueValue = value.getValues().get(i);
                SQLIdentifierExpr column = (SQLIdentifierExpr) columns.get(i);
                map.put(column.getName(),valueValue);
            }
            list.add(map);
        }
        return InsertParseResult.builder().columns(columns).values(list).tableName(tableName).build();
    }
}
