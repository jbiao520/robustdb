package com.robustdb.server.sql;

import com.alibaba.druid.sql.SQLUtils;
import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.statement.*;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlCreateTableStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlTableIndex;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlUpdateStatement;
import com.alibaba.druid.util.JdbcConstants;

import java.util.List;

public class Test {
    public static void main(String[] args) {
//        sql = "SELECT LastName, FirstName, Address, City FROM Persons a, td b WHERE PersonID = 1 and a.id=b.id";
//        sql = "insert into Persons ( PersonID , LastName , FirstName, Address,City ) values (1,'guo','jianbiao','sunqiao road','shanghai');";
//        create();
        select();
    }

    private static void create() {
        String sql = "CREATE TABLE Persons (\n" +
                "    PersonID int,\n" +
                "    LastName varchar(255),\n" +
                "    FirstName varchar(255),\n" +
                "    Address varchar(255),\n" +
                "    City varchar(255)\n" +
                ");";

        String dbType = JdbcConstants.MYSQL.name();

        //?????
        String result = SQLUtils.format(sql, dbType);
        System.out.println(result); // ??????
        MySqlCreateTableStatement sqlStatement = (MySqlCreateTableStatement) SQLUtils.parseSingleMysqlStatement(sql);
        String tableName = sqlStatement.getTableName();
        System.out.println(tableName);
        List<SQLColumnDefinition> columns = sqlStatement.getColumnDefinitions();
        for (SQLColumnDefinition column : columns) {
            System.out.println(column.getColumnName()+" "+column.getDataType());
        }
        List<MySqlTableIndex> sqlTableIndices = sqlStatement.getMysqlIndexes();
        System.out.println(sqlTableIndices);
    }

    private static void select() {
        String sql = "SELECT LastName, FirstName, Address, City FROM Persons a where Address=1 and City=2 and FirstName=5";
        SQLSelectStatement statement = (SQLSelectStatement) SQLUtils.parseSingleMysqlStatement(sql);
        SQLSelect select = statement.getSelect();
        System.out.println(select);
        SQLSelectQueryBlock query = (SQLSelectQueryBlock) select.getQuery();
        System.out.println(query.getSelectList());
        System.out.println(query.getFrom());
        SQLExpr where = query.getWhere();
        System.out.println(query.getWhere());
        SQLExprTableSource tableSource = (SQLExprTableSource) query.getFrom();
        String tableName = tableSource.getExpr().toString();
        System.out.println("tableName:" + tableName);
    }

    private static void update(){
        String sql = "update  Persons set Address='test',FirstName='pretty' where city='2'";
        MySqlUpdateStatement statement = (MySqlUpdateStatement) SQLUtils.parseSingleMysqlStatement(sql);
        System.out.println(statement.getTableName().getSimpleName());
        System.out.println(statement.getItems());
        System.out.println(statement.getWhere());
    }

}
