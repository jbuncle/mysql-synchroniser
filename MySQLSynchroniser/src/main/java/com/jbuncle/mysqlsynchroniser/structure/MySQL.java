/*
 * The MIT License
 *
 * Copyright 2016 James Buncle <jbuncle@hotmail.com>.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */
package com.jbuncle.mysqlsynchroniser.structure;

import com.jbuncle.mysqlsynchroniser.structure.diff.IndexesBuilder;
import com.jbuncle.mysqlsynchroniser.structure.objects.View;
import com.jbuncle.mysqlsynchroniser.structure.objects.Column;
import com.jbuncle.mysqlsynchroniser.structure.objects.Index;
import com.jbuncle.mysqlsynchroniser.structure.objects.Table;
import com.jbuncle.mysqlsynchroniser.structure.objects.Database;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 *
 * @author James Buncle <jbuncle@hotmail.com>
 */
public class MySQL {

    private final Connection conn;

    public MySQL(final Connection conn) {
        this.conn = conn;
    }

    public Database loadDatabase() throws SQLException {
        return new Database(loadTables(conn), loadViews(conn));
    }

    public static Map<String, Table> loadTables(final Connection conn) throws SQLException {
        final Map<String, Table> tables = new HashMap<String, Table>();
        for (final String tableName : getTables(conn)) {
            Table table = loadTable(conn, tableName);
            tables.put(tableName, table);
        }
        return tables;
    }

    public static Table loadTable(final Connection conn, final String tableName) throws SQLException {
        final List<Column> columns = loadFromShowFullColumns(conn, tableName);
        final List<Index> indexes = loadIndexes(conn, tableName);
        final Table table = new Table(tableName, columns, indexes);
        return table;
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

    private static Column loadFromShowFullColumns(final ResultSet rs) throws SQLException {
        final String field = rs.getString("Field");
        final String type = rs.getString("Type");
        final boolean nullable = rs.getString("Null").equals("YES");
        final String key = rs.getString("Key");
        final String defaultValue = rs.getString("Default");
        final String extra = rs.getString("Extra");
        final String collation = rs.getString("Collation");
        final String comment = rs.getString("Comment");

        return new Column(field, type, nullable, key, defaultValue, extra, collation, comment);
    }

    public static List<Column> loadFromShowFullColumns(Connection conn, final String tableName) throws SQLException {
        List<Column> columns = new LinkedList<Column>();
        final ResultSet rs = conn.createStatement().executeQuery("SHOW FULL COLUMNS IN " + tableName + ";");

        while (rs.next()) {
            columns.add(loadFromShowFullColumns(rs));
        }
        return columns;
    }

    public static Map<String, View> loadViews(final Connection conn)
            throws SQLException {
        final Map<String, View> views = new HashMap<String, View>();
        for (final String viewName : getViewNames(conn)) {
            views.put(viewName, new View(viewName, loadCreateStatement(conn, viewName)));
        }
        return views;
    }

    private static List<String> getViewNames(final Connection conn) throws SQLException {
        final LinkedList<String> views = new LinkedList<String>();
        ResultSet rs = null;
        try {
            rs = conn.createStatement().executeQuery("SHOW FULL TABLES WHERE TABLE_TYPE LIKE 'VIEW';");
            while (rs.next()) {
                views.add(rs.getString(1));
            }
        } finally {
            try {
                rs.close();
            } catch (Exception e) {
            }
        }
        return views;
    }

    private static String loadCreateStatement(final Connection conn, final String viewName)
            throws SQLException {
        ResultSet rs = null;
        try {
            rs = conn.createStatement().executeQuery("SHOW CREATE VIEW " + viewName + ";");
            if (rs.next()) {
                return rs.getString("Create View");
            }
        } finally {
            try {
                rs.close();
            } catch (Exception e) {
            }
        }
        return null;
    }

    public static List<Index> loadIndexes(
            final Connection conn, final String tableName)
            throws SQLException {
        ResultSet rs = null;
        try {
            rs = conn.createStatement().executeQuery("SHOW INDEXES FROM `" + tableName + "`;");

            final IndexesBuilder indexesBuilder = new IndexesBuilder(tableName);
            while (rs.next()) {
                final boolean nonUnique = rs.getBoolean("Non_unique");
                final String keyName = rs.getString("Key_name");
                final String columnName = rs.getString("Column_name");
                indexesBuilder.addIndex(keyName, nonUnique, columnName);
            }
            return indexesBuilder.getIndexes();
        } finally {
            try {
                rs.close();
            } catch (Exception e) {
            }
        }
    }

}
