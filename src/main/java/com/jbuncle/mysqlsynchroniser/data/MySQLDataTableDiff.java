/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.jbuncle.mysqlsynchroniser.data;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.LinkedList;
import java.util.List;

/**
 *
 * @author James Buncle
 */
class MySQLDataTableDiff {

    private String primaryKey;
    private final Connection conn;
    private final List<String> columns;
    private final String table;

    public MySQLDataTableDiff(final Connection sourceConn, final String table) throws SQLException {
        this.conn = sourceConn;
        this.columns = new LinkedList<String>();
        ResultSet rs = null;
        try {
            rs = conn.createStatement().executeQuery("SHOW FULL COLUMNS FROM `" + table + "`;");
            while (rs.next()) {
                final String key = rs.getString("Key");
                final String column = rs.getString("Field");
                if (key.equals("PRI")) {
                    this.primaryKey = column;
                }
                columns.add(column);
            }
        } finally {
            if (rs != null) {
                rs.close();
            }
        }
        if (this.primaryKey == null) {
            throw new RuntimeException("Unable to determine Primary Key for table " + table);
        }
        this.table = table;
    }

    private String getSelectQuery(final String table) {
        return "SELECT * FROM " + table + " ORDER BY " + this.primaryKey + " ASC;";
    }

    private String getStatementColumns() {
        StringBuilder sb = new StringBuilder();
        for (String column : columns) {
            sb.append(column);
            sb.append(",");
        }
        sb.deleteCharAt(sb.length() - 1);
        return sb.toString();
    }

    public List<String> getUpdatesByPrimaryKey(Connection targetConn, final String targetTable) throws SQLException {
        final List<String> updates = new LinkedList<String>();

        final ResultSet sourceEntries = getEntries(this.conn, this.table);
        final ResultSet targetEntries = getEntries(targetConn, targetTable);

        sourceEntries.next();
        targetEntries.next();
        //Iterate source rows
        while (!sourceEntries.isLast() && !targetEntries.isLast()) {

            final long sourcePK = getPrimaryKeyValue(sourceEntries);
            final long targetPK = getPrimaryKeyValue(targetEntries);

            if (sourcePK == targetPK) {
                //Same row, compare and add update if necessary
                final String update = getUpdate(sourceEntries, targetEntries, targetTable);
                if (update != null) {
                    //Row different
                    updates.add(update);
                }
                sourceEntries.next();
                targetEntries.next();
            } else {


                if (sourcePK < targetPK) {
                    updates.add(getInsert(sourceEntries, targetTable));
                    sourceEntries.next();
                }
                if (sourcePK > targetPK) {
                    //Get delete statement for current row
                    updates.add(getDelete(targetEntries, targetTable));
                    //increment target till it matches
                    targetEntries.next();
                }
            }
        }
        //Go back one, as the while loop incremented too far
        while (sourceEntries.next()) {
            updates.add(getInsert(sourceEntries, targetTable));
        }
        while (targetEntries.next()) {
            updates.add(getDelete(targetEntries, targetTable));
        }
        return updates;
    }

    private long getPrimaryKeyValue(ResultSet rs) throws SQLException {
        return rs.getLong(primaryKey);
    }

    private static boolean haveNext(ResultSet sourceRs, ResultSet targetRS) throws SQLException {
        return !sourceRs.isLast() && !targetRS.isLast();
    }

    private ResultSet getEntries(Connection conn, final String table) throws SQLException {
        return conn.createStatement().executeQuery(getSelectQuery(table));
    }

    private static String getPrimaryKeyColumn(final Connection conn, final String tableName) throws SQLException {
        ResultSet rs = null;
        try {
            rs = conn.createStatement().executeQuery("SHOW FULL COLUMNS FROM `" + tableName + "` WHERE `Key` LIKE 'PRI';");
            if (rs.next()) {
                return rs.getString("Field");
            } else {
                throw new RuntimeException("Unable to find Primary Key for table: " + tableName);
            }
        } finally {
            if (rs != null) {
                rs.close();
            }
        }
    }

    private int comparePKs(ResultSet sourceRow, ResultSet targetRow) {
        return 0;
    }

    private String getUpdate(ResultSet sourceEntries, ResultSet targetEntries, final String targetTable) throws SQLException {

        StringBuilder columnUpdates = new StringBuilder();
        columnUpdates.append("UPDATE `").append(targetTable);
        columnUpdates.append("` SET ");
        boolean hasUpdates = false;
        for (String column : columns) {
            if (!columnSame(sourceEntries, targetEntries, column)) {
                columnUpdates.append(column);
                columnUpdates.append("=");
                columnUpdates.append(getValueAsString(sourceEntries, column));
                columnUpdates.append(",");
                hasUpdates = true;
            }
        }
        columnUpdates.deleteCharAt(columnUpdates.length() - 1);
        columnUpdates.append(" WHERE ").append(this.primaryKey).append("=").append(getPrimaryKeyValue(targetEntries));
        if (hasUpdates) {
            return columnUpdates.toString();
        } else {
            return null;
        }
    }

    private boolean columnSame(ResultSet sourceEntries, ResultSet targetEntries, String column) throws SQLException {
        return isSame(sourceEntries.getBytes(column), targetEntries.getBytes(column));
    }

    private boolean isSame(byte[] arr1, byte[] arr2) {
        if (arr1 == null && arr2 == null) {
            return true;
        } else if (arr1 == null ^ arr2 == null) {
            return false;
        } else if (arr1.length != arr2.length) {
            return false;
        } else {
            for (int arrIndex = 0; arrIndex < arr1.length; arrIndex++) {
                if (arr1[arrIndex] != arr2[arrIndex]) {
                    return false;
                }
            }
        }
        return true;
    }

    private String getInsert(ResultSet sourceEntries, final String targetTable) throws SQLException {

        final StringBuilder insert = new StringBuilder();
        insert.append("INSERT INTO ").append(targetTable);
        insert.append(" (");
        insert.append(getStatementColumns());
        insert.append(") VALUES (");
        for (String columnName : columns) {
            insert.append(getValueAsString(sourceEntries, columnName));
            insert.append(",");
        }
        insert.deleteCharAt(insert.length() - 1);
        insert.append(")");
        return insert.toString();
    }

    private String getValueAsString(ResultSet rs, String column) throws SQLException {
        final Object object = rs.getObject(column);
        if (object instanceof CharSequence) {
            return "'" + String.valueOf(object).replaceAll("\\'", "\\\\'") + "'";
        } else if (object instanceof java.sql.Timestamp) {
            return "'" + String.valueOf(object) + "'";
        } else {
            return String.valueOf(object);
        }
    }

    private String getDelete(ResultSet targetEntries, final String targetTable) throws SQLException {
        long pk = getPrimaryKeyValue(targetEntries);
        return "DELETE FROM " + targetTable + " WHERE " + this.primaryKey + "=" + pk + ";";
    }
}
