package com.robustdb.server.sql.executor.physical;

import com.robustdb.server.model.parser.ParseResult;

public abstract class AbstractPhysicalExecutor {
    protected AbstractPhysicalExecutor nextExecutor;
    public void setNextExecutor(AbstractPhysicalExecutor sqlExecturot){
        this.nextExecutor = sqlExecturot;
    }
    public void executePlan(ParseResult parseResult) {
        if (compatible(parseResult)) {
            execute(parseResult);
        }
        if (nextExecutor != null) {
            nextExecutor.execute(parseResult);
        }
    }

    abstract protected void execute(ParseResult parseResult);

    abstract protected boolean compatible(ParseResult parseResult);
}
