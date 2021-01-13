package com.robustdb.server.sql;

import com.robustdb.server.model.parser.ParseResult;
import com.robustdb.server.sql.executor.physical.*;
import com.robustdb.server.sql.parser.*;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class MySqlEngine implements SQLEngine {


    public void executeSql(String sql) {
        ParseResult parseResult = getChainOfParsers().parseSql(sql);
        getChainOfExecutors().executePlan(parseResult);
    }

    private static AbstractSQLParser getChainOfParsers() {

        AbstractSQLParser createQuerySQLParser = new CreateQuerySQLParser();
        AbstractSQLParser updateQuerySQLParser = new UpdateQuerySQLParser();
        AbstractSQLParser selectQuerySQLParser = new SelectQuerySQLParser();
        AbstractSQLParser insertQuerySQLParser = new InsertQuerySQLParser();

        selectQuerySQLParser.setNextParser(updateQuerySQLParser);
        updateQuerySQLParser.setNextParser(insertQuerySQLParser);
        insertQuerySQLParser.setNextParser(createQuerySQLParser);

        return selectQuerySQLParser;
    }

    private static AbstractPhysicalExecutor getChainOfExecutors() {

        AbstractPhysicalExecutor createPhysicalExecutor = new CreatePhysicalExecutor();
        AbstractPhysicalExecutor updateQuerySQLExecutor = new UpdatePhysicalExecutor();
        AbstractPhysicalExecutor selectQuerySQLExecutor = new SelectPhysicalExecutor();
        AbstractPhysicalExecutor insertQuerySQLExecutor = new InsertPhysicalExecutor();

        selectQuerySQLExecutor.setNextExecutor(updateQuerySQLExecutor);
        updateQuerySQLExecutor.setNextExecutor(insertQuerySQLExecutor);
        insertQuerySQLExecutor.setNextExecutor(createPhysicalExecutor);

        return selectQuerySQLExecutor;
    }
}
