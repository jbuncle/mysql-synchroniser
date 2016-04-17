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
package com.jbuncle.mysqlsynchroniser.util;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.LinkedList;
import java.util.List;
import javax.sql.DataSource;

/**
 *
 * @author James Buncle <jbuncle@hotmail.com>
 */
public class ConnectionStrategy {

    private final DataSource dataSource;

    public ConnectionStrategy(final DataSource dataSource) {
        this.dataSource = dataSource;
    }

    private Connection getConnection() throws SQLException {
        return dataSource.getConnection();
    }

    public interface RowMapper<T> {

        public T rowToObject(ResultSet rs) throws SQLException;
    }

    public <T> List<T> query(final String query, final RowMapper<T> rowMapper) throws SQLException {
        final List<T> objects = new LinkedList<>();
        try (final Connection conn = getConnection();
                final Statement stmt = conn.createStatement();
                final ResultSet result = stmt.executeQuery(query)) {

            while (result.next()) {
                final T object = rowMapper.rowToObject(result);
                objects.add(object);
            }
        }
        return objects;
    }
}
