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
package com.mycompany.mysqldiffgenerator.structure;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 *
 * @author James Buncle
 */
public class MySQLBaseTable extends AbstractMySQLSynchroniser<MySQLBaseTable> implements MySQLTable<MySQLBaseTable> {

    private final String tableName;
    private final List<MySQLColumn> columns;
    private final List<MySQLIndexDescriptor> indexes;
    private final String createStatement;

    public MySQLBaseTable(final Connection conn, final String tableName) throws SQLException {
        this.tableName = tableName;
        this.createStatement = getCreateStatement(conn, this.tableName);

        this.columns = new LinkedList<MySQLColumn>(MySQLColumn.loadFromShowFullColumns(conn, tableName));
        this.indexes = new LinkedList<MySQLIndexDescriptor>(MySQLIndexDescriptor.loadIndexes(conn, tableName));
    }

    public static Map<String, MySQLBaseTable> getMySQLBaseTables(final Connection conn) throws SQLException {
        final Map<String, MySQLBaseTable> tables = new HashMap<String, MySQLBaseTable>();
        for (final String tableName : getTables(conn)) {
            tables.put(tableName, new MySQLBaseTable(conn, tableName));
        }
        return tables;
    }

    private static List<String> getTables(final Connection conn) throws SQLException {
        final List<String> tables = new LinkedList<String>();
        ResultSet rs = null;
        try {
            rs = conn.createStatement().executeQuery("SHOW FULL TABLES WHERE Table_type = 'BASE TABLE';");
            while (rs.next()) {
                final String tableName = rs.getString(1);
                tables.add(tableName);
            }
        } finally {
            try {
                rs.close();
            } catch (Exception e) {
            }
        }
        return tables;
    }

    public String getCreateStatement() {
        return createStatement;
    }

    protected static String getCreateStatement(final Connection conn, final String table) throws SQLException {
        final String query = "SHOW CREATE TABLE " + table + ";";
        ResultSet rs = null;
        try {
            rs = conn.createStatement().executeQuery(query);
            final String createStatement;
            if (rs.next()) {
                createStatement = rs.getString("Create Table");
            } else {
                createStatement = null;
            }
            return createStatement;
        } finally {
            try {
                rs.close();
            } catch (Exception e) {
            }
        }
    }

    private List<String> getDeletedColumns(final MySQLBaseTable target) {
        final List<String> updates = new LinkedList<String>();
        //Work out deletes
        for (final MySQLColumn column : target.columns) {
            final MySQLColumn thisColumn = this.getColumn(column.getName());
            if (thisColumn == null) {
                //Extra column in target
                updates.add(column.getDeleteStatement(target.tableName));
            }
        }
        return updates;
    }

    private List<String> getUpdatedColumns(final MySQLBaseTable target) {
        final List<String> updates = new LinkedList<String>();
        MySQLColumn lastColumnDescriptor = null;
        for (final MySQLColumn column : columns) {
            final MySQLColumn targetColumn = target.getColumn(column.getName());
            if (targetColumn == null) {
                //Missing column, add it
                if (lastColumnDescriptor == null) {
                    //First column
                    updates.add(column.getInsertColumnFirstStatement(target.tableName) + "\n");
                } else {
                    //Add after last column
                    updates.add(column.getInsertColumnAfterStatement(target.tableName, lastColumnDescriptor.getName()) + "\n");
                }
            } else {
                if (!column.isSame(targetColumn)) {
                    updates.add(column.getUpdateStatement(target.tableName) + "\n");
                }
            }
            lastColumnDescriptor = column;
        }
        return updates;
    }

    private List<String> getUpdatedIndexes(MySQLBaseTable target) {
        final List<String> updates = new LinkedList<String>();
        for (final MySQLIndexDescriptor sourceIndex : indexes) {
            final String sourceKeyName = sourceIndex.getKeyName();
            final MySQLIndexDescriptor targetIndex = target.getIndex(sourceKeyName);
            if (targetIndex == null) {
                //Get index add
                updates.add(sourceIndex.getCreateStatement(target.tableName));
            } else if (!sourceIndex.isSame(targetIndex)) {
                //Different, drop then add
                updates.add(sourceIndex.getDeleteStatament(target.tableName));
                updates.add(sourceIndex.getCreateStatement(target.tableName));
            }
        }
        return updates;
    }

    private List<String> getDeletedIndexes(MySQLBaseTable target) {
        final List<String> updates = new LinkedList<String>();

        for (final MySQLIndexDescriptor index : target.indexes) {
            final String indexKeyName = index.getKeyName();
            final MySQLIndexDescriptor sourceIndex = this.getIndex(indexKeyName);
            if (sourceIndex == null) {
                //Get delete index
                updates.add(index.getDeleteStatament(target.tableName));
            }
        }
        return updates;
    }

    public List<String> getSynchroniseUpdates(final MySQLBaseTable target) {
        final List<String> updates = new LinkedList<String>();

        //Get deleted columns
        updates.addAll(getDeletedColumns(target));

        //Get updated columns
        updates.addAll(getUpdatedColumns(target));

        //Get deleted indexes
        updates.addAll(getDeletedIndexes(target));

        //Get updated indexes
        updates.addAll(getUpdatedIndexes(target));

        return updates;
    }

    private MySQLIndexDescriptor getIndex(final String keyName) {
        for (final MySQLIndexDescriptor index : this.indexes) {
            if (index.getKeyName().equals(keyName)) {
                return index;
            }
        }
        return null;
    }

    private MySQLColumn getColumn(final String columnName) {
        for (final MySQLColumn column : this.columns) {
            if (column.getName().equals(columnName)) {
                return column;
            }
        }
        return null;
    }

    public String getDropStatement() {
        return "DROP TABLE " + this.tableName + ";";
    }
}