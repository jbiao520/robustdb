package com.robustdb.server.sql.executor.physical;

import com.robustdb.server.model.parser.ParseResult;

public abstract class AbstractPhysicalExecutor {
    protected AbstractPhysicalExecutor nextExecutor;
    public void setNextExecutor(AbstractPhysicalExecutor sqlExectuor){
        this.nextExecutor = sqlExectuor;
    }
    public void executePlan(ParseResult parseResult) {
        if (compatible(parseResult)) {
            execute(parseResult);
        }
        if (nextExecutor != null) {
            nextExecutor.executePlan(parseResult);
        }
    }

    abstract protected void execute(ParseResult parseResult);

    abstract protected boolean compatible(ParseResult parseResult);
}
