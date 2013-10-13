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
public class MySQLView extends AbstractMySQLSynchroniser<MySQLView> implements MySQLComparable<MySQLView>, MySQLTable<MySQLView> {

    private final String viewCreateStatement;

    private MySQLView(final String viewCreateStatement) {
        this.viewCreateStatement = viewCreateStatement;
    }

    public boolean isSame(final MySQLView target) {
        return this.viewCreateStatement.equals(target.viewCreateStatement);
    }

    public static Map<String, MySQLView> getViews(final Connection conn)
            throws SQLException {
        final Map<String, MySQLView> views = new HashMap<String, MySQLView>();
        for (final String viewName : getViewNames(conn)) {
            views.put(viewName, new MySQLView(loadCreateStatement(conn, viewName)));
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

    private static String getViewDropStatement(final String view) {
        return "DROP VIEW IF EXISTS " + view + ";";
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

    public String getCreateStatement() {
        return this.viewCreateStatement;
    }

    public String getDropStatement() {
        return getViewDropStatement(viewCreateStatement);
    }

    public List<String> getSynchroniseUpdates(MySQLView target) {
        List<String> updates = new LinkedList<String>();
        if (this.isSame(target)) {
            updates.add(this.getDropStatement());
            updates.add(this.getCreateStatement());
        }
        return updates;
    }
}
