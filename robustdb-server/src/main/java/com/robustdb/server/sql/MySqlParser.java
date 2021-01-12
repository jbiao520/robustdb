package com.robustdb.server.sql;

import com.alibaba.druid.sql.SQLUtils;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.dialect.mysql.visitor.MySqlSchemaStatVisitor;
import com.alibaba.druid.stat.TableStat;
import com.alibaba.druid.util.JdbcConstants;

import java.util.Collection;
import java.util.List;
import java.util.Map;

public class MySqlParser implements SQLParser {
    public void validateSQL() {

    }

    public void parseSql(String sql) {
        String dbType = JdbcConstants.MYSQL.name();

        String result = SQLUtils.format(sql, dbType);
        SQLStatement statement = SQLUtils.parseSingleStatement(sql, dbType);

        MySqlSchemaStatVisitor visitor = new MySqlSchemaStatVisitor();
        statement.accept(visitor);

        Map<TableStat.Name, TableStat> manipulation = visitor.getTables();
        Collection<TableStat.Column> columns = visitor.getColumns();
        List<TableStat.Condition> conditions = visitor.getConditions();
        for (TableStat value : manipulation.values()) {
        }
    }
}
