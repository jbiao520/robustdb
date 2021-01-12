package com.robustdb.server.sql;

import com.alibaba.druid.sql.SQLUtils;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.dialect.mysql.visitor.MySqlParameterizedVisitor;
import com.alibaba.druid.sql.dialect.mysql.visitor.MySqlSchemaStatVisitor;
import com.alibaba.druid.stat.TableStat;
import com.alibaba.druid.util.JdbcConstants;

import java.util.List;

public class Test {
    public static void main(String[] args) {
        String sql = "CREATE TABLE Persons (\n" +
                "    PersonID int,\n" +
                "    LastName varchar(255),\n" +
                "    FirstName varchar(255),\n" +
                "    Address varchar(255),\n" +
                "    City varchar(255)\n" +
                ");";
//        sql = "SELECT LastName, FirstName, Address, City FROM Persons a, td b WHERE PersonID = 1 and a.id=b.id";
        sql = "insert into Persons ( PersonID , LastName , FirstName, Address,City ) values (1,'guo','jianbiao','sunqiao road','shanghai');";
        System.out.println(sql);
        String dbType = JdbcConstants.MYSQL.name();

        //?????
        String result = SQLUtils.format(sql, dbType);
        System.out.println(result); // ??????
        List<SQLStatement> stmtList = SQLUtils.parseStatements(sql, dbType);

        //???????????
        System.out.println("size is:" + stmtList.size());
        for (int i = 0; i < stmtList.size(); i++) {

            SQLStatement stmt = stmtList.get(i);
            MySqlSchemaStatVisitor visitor = new MySqlSchemaStatVisitor();
            MySqlParameterizedVisitor parameterizedVisitor = new MySqlParameterizedVisitor();
            stmt.accept(parameterizedVisitor);
            stmt.accept(visitor);
            System.out.println("columns : " + visitor.getColumns());
            System.out.println("params : " + parameterizedVisitor.getParameters());


            System.out.println("Manipulation : " + visitor.getTables());
            System.out.println("fields : " + visitor.getTables());
            for (TableStat value : visitor.getTables().values()) {
                System.out.println(value.getSelectCount());
            }
            System.out.println("condition : " + visitor.getConditions());
        }

    }
}
