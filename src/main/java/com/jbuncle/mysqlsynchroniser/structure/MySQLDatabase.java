/*
 *  Copyright (c) 2013 James Buncle
 * 
 *  Permission is hereby granted, free of charge, to any person obtaining a copy
 *  of this software and associated documentation files (the "Software"), to deal
 *  in the Software without restriction, including without limitation the rights
 *  to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 *  copies of the Software, and to permit persons to whom the Software is
 *  furnished to do so, subject to the following conditions:
 * 
 *  The above copyright notice and this permission notice shall be included in
 *  all copies or substantial portions of the Software.
 * 
 *  THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 *  IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 *  FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 *  AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 *  LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 *  OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 *  THE SOFTWARE.
 */
package com.jbuncle.mysqlsynchroniser.structure;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 *
 * @author James Buncle
 */
class MySQLDatabase extends AbstractMySQLSynchroniser<MySQLDatabase> {

    private final Map<String, MySQLTable> tables;

    public MySQLDatabase(Connection conn) throws SQLException {
        this.tables = new LinkedHashMap<String, MySQLTable>();
        this.tables.putAll(MySQLBaseTable.getMySQLBaseTables(conn));
        //Add views after, as they may have dependencies on the tables above
        this.tables.putAll(MySQLView.getViews(conn));
    }

    public List<String> getTableUpdates(MySQLDatabase target) {

        final List<String> updates = new LinkedList<String>();
        final Map<String, MySQLTable> targetTables = new HashMap<String, MySQLTable>();
        targetTables.putAll(target.tables);
        //Iterate source tables
        for (final String sourceTableName : this.tables.keySet()) {
            if (targetTables.containsKey(sourceTableName)) {
                //Has table, compare and update if necessary
                updates.addAll(this.tables.get(sourceTableName).getSynchroniseUpdates(targetTables.get(sourceTableName)));
            } else {
                //Target is missing table, get updates
                updates.add(this.tables.get(sourceTableName).getCreateStatement());
            }
            targetTables.remove(sourceTableName);
        }
        //Iterate remaining target tables for deletes
        for (final MySQLTable targetTable : targetTables.values()) {
            updates.add(targetTable.getDropStatement());
        }
        return updates;
    }

    private static boolean createStatementsSame(
            final Connection sourceConn, final String sourceTable,
            final Connection targetConn, final String targetTable)
            throws SQLException {
        final String sourceCreate = MySQLBaseTable.getCreateStatement(sourceConn, sourceTable).trim();
        String targetCreate = MySQLBaseTable.getCreateStatement(targetConn, targetTable);
        //Replace table name to make statements the same
        targetCreate = targetCreate.replace("`" + targetTable + "`", "`" + sourceTable + "`");
        targetCreate = targetCreate.replaceAll("AUTO_INCREMENT=([0-9]){1,2} ", "");
        targetCreate = targetCreate.trim();
        if (!targetCreate.equals(sourceCreate)) {

            System.out.println("SOURCE:\n");
            System.out.println(sourceCreate);
            System.out.println("TARGET:\n");
            System.out.println(targetCreate);
            return false;
        } else {
            return true;
        }
    }

    public List<String> getSynchroniseUpdates(MySQLDatabase target) {
        return getTableUpdates(target);
    }
}
