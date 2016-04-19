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

import com.jbuncle.mysqlsynchroniser.structure.objects.IndexesBuilder;
import com.jbuncle.mysqlsynchroniser.structure.objects.View;
import com.jbuncle.mysqlsynchroniser.structure.objects.Column;
import com.jbuncle.mysqlsynchroniser.structure.objects.Index;
import com.jbuncle.mysqlsynchroniser.structure.objects.Table;
import com.jbuncle.mysqlsynchroniser.structure.objects.Database;
import com.jbuncle.mysqlsynchroniser.connection.ConnectionStrategy;
import com.jbuncle.mysqlsynchroniser.connection.RowMapper;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.sql.DataSource;

/**
 *
 * @author James Buncle <jbuncle@hotmail.com>
 */
public class MySQL {

    private final ConnectionStrategy connectionStrategy;

    public MySQL(final DataSource dataSource) {
        this.connectionStrategy = new ConnectionStrategy(dataSource);
    }

    public Database loadDatabase() throws SQLException {
        return new Database(loadTables(), loadViews());
    }
    public Map<String, Table> loadTables() throws SQLException {
        final Map<String, Table> tables = new HashMap<>();
        for (final String tableName : getTables()) {
            Table table = loadTable(tableName);
            tables.put(tableName, table);
        }
        return tables;
    }

    public Table loadTable(final String tableName) throws SQLException {
        final List<Column> columns = loadFromShowFullColumns(tableName);
        final List<Index> indexes = loadIndexes(tableName);
        final Table table = new Table(tableName, columns, indexes);
        return table;
    }

    private List<String> getTables() throws SQLException {
        return this.connectionStrategy.query("SHOW FULL TABLES WHERE Table_type = 'BASE TABLE';", new RowMapper<String>() {
            @Override
            public String rowToObject(ResultSet rs) throws SQLException {
                return rs.getString(1);
            }
        });
    }

    private Column loadFromShowFullColumns(final ResultSet rs) throws SQLException {
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

    public List<Column> loadFromShowFullColumns(final String tableName) throws SQLException {

        return this.connectionStrategy.query("SHOW FULL COLUMNS IN " + tableName + ";", new RowMapper<Column>() {
            @Override
            public Column rowToObject(ResultSet rs) throws SQLException {
                return loadFromShowFullColumns(rs);
            }
        });
    }

    private Map<String, View> loadViews()
            throws SQLException {
        final Map<String, View> views = new HashMap<String, View>();
        for (final String viewName : getViewNames()) {
            views.put(viewName, new View(viewName, loadCreateStatement(viewName)));
        }
        return views;
    }

    private List<String> getViewNames() throws SQLException {
        return this.connectionStrategy.query("SHOW FULL TABLES WHERE TABLE_TYPE LIKE 'VIEW';", new RowMapper<String>() {
            @Override
            public String rowToObject(ResultSet rs) throws SQLException {
                return rs.getString(1);
            }
        });
    }

    private String loadCreateStatement(final String viewName)
            throws SQLException {
        return this.connectionStrategy.query(viewName, new RowMapper<String>() {
            @Override
            public String rowToObject(ResultSet rs) throws SQLException {
                return rs.getString("Create View");
            }
        }).get(0);
    }

    public List<Index> loadIndexes(final String tableName)
            throws SQLException {

        final IndexesBuilder indexesBuilder = new IndexesBuilder(tableName);
        this.connectionStrategy.query("SHOW INDEXES FROM `" + tableName + "`;",
                new RowMapper<String>() {
            @Override
            public String rowToObject(ResultSet rs) throws SQLException {
                final boolean nonUnique = rs.getBoolean("Non_unique");
                final String keyName = rs.getString("Key_name");
                final String columnName = rs.getString("Column_name");
                indexesBuilder.addIndex(keyName, nonUnique, columnName);
                return null;
            }
        });
        return indexesBuilder.getIndexes();
    }

}
