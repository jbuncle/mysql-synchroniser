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
package com.jbuncle.mysqlsynchroniser.structure.diff;

import com.jbuncle.mysqlsynchroniser.structure.objects.Table;
import com.jbuncle.mysqlsynchroniser.connection.ConnectionStrategy;
import com.jbuncle.mysqlsynchroniser.connection.RowMapper;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import javax.sql.DataSource;

/**
 *
 * @author James Buncle <jbuncle@hotmail.com>
 */
class TableStatementBuilder {

    private final Table baseTable;

    public TableStatementBuilder(final Table baseTable) {
        this.baseTable = baseTable;
    }

    public String getDropStatement() {
        return "DROP TABLE " + baseTable.getTableName() + ";";
    }

    public String getCreateStatement(DataSource conn) throws SQLException {
        return getCreateStatement(conn, this.baseTable.getTableName());
    }

    private static String getCreateStatement(final DataSource conn, final String table) throws SQLException {
        final List<String> results = new ConnectionStrategy(conn).query("SHOW CREATE TABLE " + table + ";", new RowMapper<String>() {
            @Override
            public String rowToObject(ResultSet rs) throws SQLException {
                return rs.getString("Create Table");
            }
        });
        if (results.isEmpty()) {
            return null;
        } else {
            return results.get(0);
        }
    }

}
