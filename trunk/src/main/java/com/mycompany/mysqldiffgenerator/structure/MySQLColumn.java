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
 * 
 */
package com.mycompany.mysqldiffgenerator.structure;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.LinkedList;
import java.util.List;

/**
 *
 * @author James Buncle
 */
public class MySQLColumn implements MySQLComparable<MySQLColumn> {

    private final String columnName;
    private final String type;
    private final boolean nullable;

    public String getName() {
        return columnName;
    }
    private final String key;
    private final String defaultValue;
    private final String extra;
    private final String collation;
    private final String comment;

    MySQLColumn(
            final String field,
            final String type,
            final boolean nullable,
            final String key,
            final String defaultValue,
            final String extra,
            final String collation,
            final String comment) {
        
        this.columnName = field;
        this.type = type;
        this.nullable = nullable;
        this.key = key;
        this.defaultValue = defaultValue;
        this.extra = extra;
        this.collation = collation;
        this.comment = comment;
    }

    private static MySQLColumn loadFromShowFullColumns(final ResultSet rs) throws SQLException {
        final String field = rs.getString("Field");
        final String type = rs.getString("Type");
        final boolean nullable = rs.getString("Null").equals("YES");
        final String key = rs.getString("Key");
        final String defaultValue = rs.getString("Default");
        final String extra = rs.getString("Extra");
        final String collation = rs.getString("Collation");
        final String comment = rs.getString("Comment");

        return new MySQLColumn(field, type, nullable, key, defaultValue, extra, collation, comment);
    }

    protected static List<MySQLColumn> loadFromShowFullColumns(Connection conn, final String tableName) throws SQLException {
        List<MySQLColumn> columns = new LinkedList<MySQLColumn>();
        final ResultSet rs = conn.createStatement().executeQuery("SHOW FULL COLUMNS IN " + tableName + ";");

        while (rs.next()) {
            columns.add(MySQLColumn.loadFromShowFullColumns(rs));
        }
        return columns;
    }

    @Override
    public String toString() {
        return "MySQLTableDescription{" + "field=" + columnName + ", type=" + type + ", nullable=" + nullable + ", key=" + key + ", defaultValue=" + defaultValue + ", extra=" + extra + '}';
    }

    public String getDeleteStatement(final String tableName) {
        return "ALTER TABLE `" + tableName + "` DROP `" + this.columnName + "`;";
    }

    public String getInsertColumnAfterStatement(final String tableName, final String after) {
        return getAddColumnStatement(tableName) + " AFTER `" + after + "`;";
    }

    public String getInsertColumnLastStatement(final String tableName) {
        return getAddColumnStatement(tableName) + ";";
    }

    public String getInsertColumnFirstStatement(final String tableName) {
        return getAddColumnStatement(tableName) + " FIRST;";
    }

    private String getNullStatement() {
        if (this.nullable) {
            return "NULL";
        } else {
            return "NOT NULL";
        }
    }

    private String getColumnAlterStatement(final String tableName, final String action) {
        StringBuilder sb = new StringBuilder();

        sb.append("ALTER TABLE `").append(tableName).append("` ").append(action);
        sb.append(" `").append(this.columnName).append("`");
        sb.append(" `").append(this.columnName).append("` ");

        sb.append(getColumnDefinition());
        return sb.toString();
    }

    private String getAddColumnStatement(final String tableName) {
        StringBuilder sb = new StringBuilder();

        sb.append("ALTER TABLE `").append(tableName).append("` ").append("ADD");
        sb.append(" `").append(this.columnName).append("` ");

        sb.append(getColumnDefinition());
        return sb.toString();
    }

    private StringBuilder getColumnDefinition() {
        StringBuilder sb = new StringBuilder();
        //Type
        sb.append(this.type).append(" ");
        //Character set
        if (this.collation != null) {
            //Collation
            sb.append("COLLATE ").append(collation).append(" ");
        }
        //Nullable
        sb.append(getNullStatement()).append(" ");
        //Default
        sb.append(extra).append(" ");
        //Comment
        sb.append("COMMENT '").append(this.comment).append("'");
        return sb;
    }

    public String getUpdateStatement(final String tableName) {
        return getColumnAlterStatement(tableName, "CHANGE") + ";";
    }

    public boolean isSame(MySQLColumn target) {

        if (this.nullable != target.nullable) {
            return false;
        }
        if (!isSame(this.type, target.type)) {
            return false;
        }
        if (!isSame(this.key, target.key)) {
            return false;
        }
        if (!isSame(this.defaultValue, target.defaultValue)) {
            return false;
        }
        if (!isSame(this.extra, target.extra)) {
            return false;
        }
        if (!isSame(this.collation, target.collation)) {
            return false;
        }
        if (!isSame(this.comment, target.comment)) {
            return false;
        }
        return true;
    }

    private static boolean isSame(String aStr, String bStr) {
        if (aStr == null ^ bStr == null) {
            return false;
        } else if (aStr == null || bStr == null) {
            return true;
        } else {
            return aStr.equals(bStr);
        }
    }
}