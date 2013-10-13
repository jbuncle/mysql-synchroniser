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
import java.util.LinkedList;
import java.util.List;

/**
 *
 * @author James Buncle
 */
public class MySQLIndexDescriptor implements MySQLComparable<MySQLIndexDescriptor> {

    private final boolean nonUnique;
    private final String keyName;
    private final String columnName;

    private MySQLIndexDescriptor(
            final boolean nonUnique,
            final String keyName,
            final String columnName) {
        this.nonUnique = nonUnique;
        this.keyName = keyName;
        this.columnName = columnName;
    }

    protected static List<MySQLIndexDescriptor> loadIndexes(
            final Connection conn, final String tableName)
            throws SQLException {
        final List<MySQLIndexDescriptor> indexes = new LinkedList<MySQLIndexDescriptor>();
        ResultSet rs = null;
        try {
            rs = conn.createStatement().executeQuery("SHOW INDEXES FROM " + tableName + ";");

            while (rs.next()) {
                indexes.add(createMySQLIndex(rs));
            }
        } finally {
            try {
                rs.close();
            } catch (Exception e) {
            }
        }
        return indexes;
    }

    private static MySQLIndexDescriptor createMySQLIndex(final ResultSet rs) throws SQLException {
        final boolean nonUnique = rs.getBoolean("Non_unique");
        final String keyName = rs.getString("Key_name");
        final String columnName = rs.getString("Column_name");

        return new MySQLIndexDescriptor(nonUnique, keyName, columnName);
    }

    private boolean isPrimaryKey() {
        return this.keyName.equals("PRIMARY");
    }

    private boolean isUniqueKey() {
        return !this.nonUnique;
    }

    private String getDropStatement(final String tableName) {
        if (isPrimaryKey()) {
            return "ALTER TABLE `" + tableName + "` DROP PRIMARY KEY;";
        } else {
            return "DROP INDEX " + keyName + " ON " + tableName + ";";
        }
    }

    private String getSetPrimaryKeyStatement(final String tableName) {
        return "ALTER TABLE `" + tableName + "` ADD PRIMARY KEY(`" + this.columnName + "`);";
    }

    private String getAddUniqueStatement(final String tableName) {
        return "ALTER TABLE `" + tableName + "` ADD UNIQUE (`" + this.columnName + "`);";
    }

    private String getAddIndex(final String tableName) {
        return "ALTER TABLE " + tableName + " ADD INDEX (" + this.columnName + ");";
    }

    protected String getColumnName() {
        return this.columnName;
    }

    protected String getKeyName() {
        return this.keyName;
    }

    public boolean isSame(MySQLIndexDescriptor target) {
        if (isPrimaryKey() ^ target.isPrimaryKey()) {
            return false;
        } else if (isUniqueKey() ^ target.isUniqueKey()) {
            return false;
        } else if (!this.columnName.equals(target.columnName)) {
            return false;
        }
        return true;
    }

    public String getCreateStatement(final String tableName) {
        if (isPrimaryKey()) {
            //Primary key
            return getSetPrimaryKeyStatement(tableName);
        } else if (isUniqueKey()) {
            //Unique key
            return getAddUniqueStatement(tableName);
        } else {
            //Just a key/index
            return getAddIndex(tableName);
        }
    }

    public String getDeleteStatament(final String tableName) {
        return getDropStatement(tableName);
    }
}
