package com.robustdb.server.tests;

import lombok.extern.slf4j.Slf4j;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

public class RobustDBTest {
    public static void main(String[] argv) {
        callrobust();
    }

    public static void callrobust() {
        System.out.println("-------- MySQL JDBC Connection Demo ------------");
        try
        {
            Class.forName("com.mysql.jdbc.Driver");
        }
        catch (ClassNotFoundException e) {
            System.out.println("MySQL JDBC Driver not found !!");
            return;
        }
        System.out.println("MySQL JDBC Driver Registered!");
        Connection connection = null;
        try {
            connection = DriverManager
                    .getConnection("jdbc:mysql://localhost:3307", "", "");
            System.out.println("SQL Connection to database established!");
            Statement statement
             = connection.createStatement();
            statement.execute("CREATE TABLE Persons ( PersonID int(8) PRIMARY KEY, LastName varchar(255) not null, FirstName varchar(255), Address varchar(255), City varchar(255) );");
        } catch (SQLException e) {
            System.out.println("Connection Failed! Check output console");
            return;
        } finally {
            try
            {
                if(connection != null)
                    connection.close();
                System.out.println("Connection closed !!");
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }
}
