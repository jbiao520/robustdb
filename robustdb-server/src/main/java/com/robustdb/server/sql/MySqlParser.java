package com.robustdb.server.sql;

import com.alibaba.druid.sql.SQLUtils;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.dialect.mysql.visitor.MySqlSchemaStatVisitor;
import com.alibaba.druid.stat.TableStat;
import com.alibaba.druid.util.JdbcConstants;
import com.robustdb.server.enums.SQLType;
import com.robustdb.server.handlers.SQLHandler;
import com.robustdb.server.model.ColumnDef;
import com.robustdb.server.model.TableDef;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

@Slf4j
public class MySqlParser implements SQLParser {
    SQLHandler sqlHandler = new SQLHandler();
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
        for (TableStat.Name name : manipulation.keySet()) {
            if (manipulation.get(name).getCreateCount() > 0) {
                //start create table
                log.info("Detected create table sql, will start create");
                List<ColumnDef> columnDefList = new ArrayList();
                for (TableStat.Column column : columns) {
                    ColumnDef columnDef = ColumnDef.builder()
                            .table(column.getTable())
                            .name(column.getName())
                            .fullName(column.getFullName())
                            .dataType(column.getDataType())
                            .build();
                    columnDefList.add(columnDef);
                }
                TableDef tableDef = TableDef.builder()
                        .rawTableDef(sql)
                        .tableName(name.getName())
                        .columnDefList(columnDefList).build();

                log.info("TableDef:{}",tableDef);
                sqlHandler.handle(SQLType.CREATE,tableDef);

            }else  if (manipulation.get(name).getInsertCount() > 0) {
                //start create table
                log.info("Detected insert sql, will start to insert");
                List<ColumnDef> columnDefList = new ArrayList();
                for (TableStat.Column column : columns) {
                    ColumnDef columnDef = ColumnDef.builder()
                            .table(column.getTable())
                            .name(column.getName())
                            .fullName(column.getFullName())
                            .dataType(column.getDataType())
                            .build();
                    columnDefList.add(columnDef);
                }
                TableDef tableDef = TableDef.builder()
                        .rawTableDef(sql)
                        .tableName(name.getName())
                        .columnDefList(columnDefList).build();

                log.info("TableDef:{}",tableDef);
                sqlHandler.handle(SQLType.CREATE,tableDef);

            }
        }
    }
}
