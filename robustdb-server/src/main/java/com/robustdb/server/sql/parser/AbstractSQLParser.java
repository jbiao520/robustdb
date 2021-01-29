package com.robustdb.server.sql.parser;

import com.robustdb.server.model.parser.ParseResult;

public abstract class AbstractSQLParser {
    protected AbstractSQLParser nextParser;
    protected String type;
    public void setNextParser(AbstractSQLParser sqlParser){
        this.nextParser = sqlParser;
    }

    public ParseResult parseSql(String sql){
        if(sql.toUpperCase().contains(type)){
            return parse(sql);
        }
        if(nextParser!=null){
            return nextParser.parseSql(sql);
        }
        return null;
    }

    abstract protected ParseResult parse(String sql);
}
