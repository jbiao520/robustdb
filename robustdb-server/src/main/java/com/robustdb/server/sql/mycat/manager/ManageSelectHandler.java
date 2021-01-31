package com.robustdb.server.sql.mycat.manager;

import com.alibaba.druid.sql.ast.statement.SQLSelectItem;
import com.alibaba.druid.util.StringUtils;
import com.robustdb.server.model.parser.SelectParseResult;
import com.robustdb.server.sql.parser.SelectQuerySQLParser;
import io.netty.buffer.ByteBuf;

import java.util.List;

import static com.robustdb.server.sql.mycat.manager.ManagerParseSelect.*;

/**
 * @author mycat
 */
public final class ManageSelectHandler {

    public static ByteBuf handle(String stmt, int offset) {
        ByteBuf buf;
        switch (ManagerParseSelect.parse(stmt, offset)) {
            case VERSION_COMMENT:
                buf = SelectVariables.execute(stmt,null);
                break;
            case SESSION_AUTO_INCREMENT:
                buf = SelectVariables.execute(stmt,null);
                break;
            case SESSION_TX_READ_ONLY:
                buf = SelectVariables.execute(stmt,null);
                break;
            default:

                try{
                    SelectQuerySQLParser selectQuerySQLParser = new SelectQuerySQLParser();
                    SelectParseResult selectParseResult = (SelectParseResult) selectQuerySQLParser.parseSql(stmt);
                    List<SQLSelectItem> sqlSelectItems = selectParseResult.getSelectList();
                    buf = SelectVariables.execute(stmt, sqlSelectItems);
                }catch(Exception e){
                    buf = null;

                }

                break;
        }
        return buf;
    }

    public static boolean isMgmtSelect(String sql){
        SelectQuerySQLParser selectQuerySQLParser = new SelectQuerySQLParser();
        SelectParseResult selectParseResult = (SelectParseResult) selectQuerySQLParser.parseSql(sql);
        return StringUtils.isEmpty(selectParseResult.getTableName());
    }

}
