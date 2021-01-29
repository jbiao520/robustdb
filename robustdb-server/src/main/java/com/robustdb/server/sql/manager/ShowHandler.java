package com.robustdb.server.sql.manager;


import io.netty.buffer.ByteBuf;

/**
 * @author mycat
 */
public final class ShowHandler {

    public static ByteBuf handle(String stmt, int offset) {
        int rs = ManagerParseShow.parse(stmt, offset);
        ByteBuf buf= null;
        int result = rs & 0xff;
        switch (result) {
            case ManagerParseShow.SYSPARAM://add rainbow
//                ShowSysParam.execute(c);
                break;
            case ManagerParseShow.SYSLOG: //add by zhuam
                String lines = stmt.substring(rs >>> 8).trim();
//                ShowSysLog.execute(c, Integer.parseInt( lines ) );
                break;
            case ManagerParseShow.COMMAND:
//                ShowCommand.execute(c);
                break;
            case ManagerParseShow.COLLATION:
//                ShowCollation.execute(c);
                break;
            case ManagerParseShow.CONNECTION:
//                ShowConnection.execute(c);
                break;
            case ManagerParseShow.BACKEND:
//                ShowBackend.execute(c);
                break;
            case ManagerParseShow.BACKEND_OLD:
//                ShowBackendOld.execute(c);
                break;
            case ManagerParseShow.CONNECTION_SQL:
//                ShowConnectionSQL.execute(c);
                break;
            case ManagerParseShow.DATABASE:
//                ShowDatabase.execute(c);
                break;
            case ManagerParseShow.DATANODE:
//                ShowDataNode.execute(c, null);
                break;
            case ManagerParseShow.DATANODE_WHERE: {
//                String name = stmt.substring(rs >>> 8).trim();
//                if (StringUtil.isEmpty(name)) {
//                    c.writeErrMessage(ErrorCode.ER_YES, "Unsupported statement");
//                } else {
//                    ShowDataNode.execute(c, name);
//                }
                break;
            }
            case ManagerParseShow.DATASOURCE:
//                ShowDataSource.execute(c, null);
                break;
            case ManagerParseShow.DATASOURCE_WHERE: {
//                String name = stmt.substring(rs >>> 8).trim();
//                if (StringUtil.isEmpty(name)) {
//                    c.writeErrMessage(ErrorCode.ER_YES, "Unsupported statement");
//                } else {
//                    ShowDataSource.execute(c, name);
//                }
                break;
            }
            case ManagerParseShow.HELP:
//                ShowHelp.execute(c);
                break;
            case ManagerParseShow.HEARTBEAT:
//                ShowHeartbeat.response(c);
                break;
            case ManagerParseShow.PARSER:
//                ShowParser.execute(c);
                break;
            case ManagerParseShow.PROCESSOR:
//                ShowProcessor.execute(c);
                break;
            case ManagerParseShow.ROUTER:
//                ShowRouter.execute(c);
                break;
            case ManagerParseShow.SERVER:
//                ShowServer.execute(c);
                break;
            case ManagerParseShow.WHITE_HOST:
//                ShowWhiteHost.execute(c);
                break;
            case ManagerParseShow.WHITE_HOST_SET:
//                ShowWhiteHost.setHost(c,ParseUtil.parseString(stmt));
                break;
            case ManagerParseShow.SQL:
                boolean isClearSql = Boolean.valueOf( stmt.substring(rs >>> 8).trim() );
//                ShowSQL.execute(c, isClearSql);
                break;
            case ManagerParseShow.SQL_DETAIL:
//                ShowSQLDetail.execute(c, ParseUtil.getSQLId(stmt));
                break;
            case ManagerParseShow.SQL_EXECUTE:
//                ShowSQLExecute.execute(c);
                break;
            case ManagerParseShow.SQL_SLOW:
                boolean isClearSlow = Boolean.valueOf( stmt.substring(rs >>> 8).trim() );
//                ShowSQLSlow.execute(c, isClearSlow, null);
                break;
            case ManagerParseShow.SQL_HIGH:
                boolean isClearHigh = Boolean.valueOf( stmt.substring(rs >>> 8).trim() );
//                ShowSQLHigh.execute(c, isClearHigh);
                break;
            case ManagerParseShow.SQL_LARGE:
                boolean isClearLarge = Boolean.valueOf( stmt.substring(rs >>> 8).trim() );
//                ShowSQLLarge.execute(c, isClearLarge);
                break;
            case ManagerParseShow.SQL_CONDITION:
//                ShowSQLCondition.execute(c);
                break;
            case ManagerParseShow.SQL_RESULTSET:
//                ShowSqlResultSet.execute(c);
                break;
            case ManagerParseShow.SQL_SUM_USER:
                boolean isClearSum = Boolean.valueOf( stmt.substring(rs >>> 8).trim() );
//                ShowSQLSumUser.execute(c,isClearSum);
                break;
            case ManagerParseShow.SQL_SUM_TABLE:
                boolean isClearTable = Boolean.valueOf( stmt.substring(rs >>> 8).trim() );
//                ShowSQLSumTable.execute(c, isClearTable);
                break;
            case ManagerParseShow.SLOW_DATANODE: {
//                String name = stmt.substring(rs >>> 8).trim();
//                if (StringUtil.isEmpty(name)) {
//                    c.writeErrMessage(ErrorCode.ER_YES, "Unsupported statement");
//                } else {
//                    // ShowSlow.dataNode(c, name);
//                    c.writeErrMessage(ErrorCode.ER_YES, "Unsupported statement");
//                }
                break;
            }
            case ManagerParseShow.SLOW_SCHEMA: {
//                String name = stmt.substring(rs >>> 8).trim();
//                if (StringUtil.isEmpty(name)) {
//                    c.writeErrMessage(ErrorCode.ER_YES, "Unsupported statement,schema value is null");
//                } else {
//                    // c.writeErrMessage(ErrorCode.ER_YES, "Unsupported statement");
//                    ShowSQLSlow.execute(c, false, StringUtil.removeBackquote(name));
//                }
                break;
            }
            case ManagerParseShow.THREADPOOL:
//                ShowThreadPool.execute(c);
                break;
            case ManagerParseShow.CACHE:
//                ShowCache.execute(c);
                break;
            case ManagerParseShow.SESSION:
//                ShowSession.execute(c);
                break;
            case ManagerParseShow.TIME_CURRENT:
//                ShowTime.execute(c, ManagerParseShow.TIME_CURRENT);
                break;
            case ManagerParseShow.TIME_STARTUP:
//                ShowTime.execute(c, ManagerParseShow.TIME_STARTUP);
                break;
            case ManagerParseShow.VARIABLES:
                buf = ShowVariables.execute();
                break;
            case ManagerParseShow.VERSION:
//                ShowVersion.execute(c);
                break;
            case ManagerParseShow.HEARTBEAT_DETAIL://by songwie
//                ShowHeartbeatDetail.response(c,stmt);
                break;
            case ManagerParseShow.DATASOURCE_SYNC://by songwie
//                ShowDatasourceSyn.response(c,stmt);
                break;
            case ManagerParseShow.DATASOURCE_SYNC_DETAIL://by songwie
//                ShowDatasourceSynDetail.response(c,stmt);
                break;
            case ManagerParseShow.DATASOURCE_CLUSTER://by songwie
//                ShowDatasourceCluster.response(c,stmt);
                break;
            case ManagerParseShow.DIRECTMEMORY_DETAILl:
//                ShowDirectMemory.execute(c,2);
                break;
            case ManagerParseShow.DIRECTMEMORY_TOTAL:
//                ShowDirectMemory.execute(c,1);
                break;
            case ManagerParseShow.CHECK_GLOBAL:
//                CheckGlobalTable.execute(c, stmt);
                break;
            case ManagerParseShow.WARNINGS:
                buf = ShowWarnings.execute();
                break;
            default:
                break;
        }
        return buf;
    }
}