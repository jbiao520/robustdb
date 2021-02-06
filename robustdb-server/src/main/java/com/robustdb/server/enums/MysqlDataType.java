package com.robustdb.server.enums;

public enum MysqlDataType {
    INT("int"),
    VARCHAR("varchar"),
    ;

    private String type;

    MysqlDataType(String type) {
        this.type = type;
    }
    public static MysqlDataType fromValue(String value){
        if(value.equals("int")){
            return INT;
        }else{
            return VARCHAR;
        }

    }
}
