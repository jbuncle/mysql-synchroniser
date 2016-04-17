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
package com.jbuncle.mysqlsynchroniser;

import com.jbuncle.mysqlsynchroniser.util.MySQLUtils;
import com.mysql.jdbc.jdbc2.optional.MysqlDataSource;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import junit.framework.TestCase;

/**
 *
 * @author James Buncle <jbuncle@hotmail.com>
 */
public class ScriptGeneratorTest extends TestCase {

    private Connection source;
    private Connection target;

    public ScriptGeneratorTest(String testName) {
        super(testName);
    }

    @Override
    protected void setUp() throws Exception {
        super.setUp();

        MysqlDataSource dataSource = new MysqlDataSource();
        dataSource.setUser("root");
        dataSource.setPassword("");
        dataSource.setServerName("192.168.99.101");
        Connection conn = dataSource.getConnection();

        conn.createStatement().execute("DROP DATABASE IF EXISTS source;");
        conn.createStatement().execute("DROP DATABASE IF EXISTS target;");
        conn.createStatement().execute("CREATE DATABASE source;");
        conn.createStatement().execute("CREATE DATABASE target;");

        //192.168.99.101:3306
        dataSource.setDatabaseName("source");
        this.source = dataSource.getConnection();
        dataSource.setDatabaseName("target");
        this.target = dataSource.getConnection();

    }

    @Override
    protected void tearDown() throws Exception {
        super.tearDown();
        source.createStatement().execute("DROP DATABASE source;");
        target.createStatement().execute("DROP DATABASE target;");

    }

    /**
     * Test of compareTable method, of class ScriptGenerator.
     */
    public void testCompareTable() throws Exception {
        System.out.println("compareTable");
        source.createStatement().execute("CREATE TABLE pet ("
                + "name VARCHAR(20), "
                + "owner VARCHAR(20), "
                + "species VARCHAR(20), "
                + "sex CHAR(1), "
                + "birth DATE, "
                + "death DATE,"
                + "CONSTRAINT pk_PersonID PRIMARY KEY (name)"
                + ");");
        target.createStatement().execute("CREATE TABLE pet ("
                + "name VARCHAR(20), "
                + "owner VARCHAR(20), "
                + "species VARCHAR(20), "
                + "type VARCHAR(20), "
                + "death DATE);");

        final String table = "pet";
        final String expResult
                = "ALTER TABLE `pet` DROP `type`;"
                + "ALTER TABLE `pet` CHANGE `name` `name` varchar(20) COLLATE latin1_swedish_ci NOT NULL  COMMENT '';"
                + "ALTER TABLE `pet` ADD `sex` char(1) COLLATE latin1_swedish_ci NULL  COMMENT '' AFTER `species`;"
                + "ALTER TABLE `pet` ADD `birth` date NULL  COMMENT '' AFTER `sex`;"
                + "ALTER TABLE `pet` ADD PRIMARY KEY(`name`);";
        final List<String> result = ScriptGenerator.compareTable(source, target, table);

        assertEquals(expResult, implode("", result));

        MySQLUtils.runUpdates(target, result);

        compareQueries(source, target, "DESCRIBE " + table);
        compareQueries(source, target, "SHOW INDEXES FROM " + table);
    }

    private static void compareQueries(final Connection source, final Connection target, final String query)
            throws SQLException {
        final ResultSet sourceRs = source.createStatement().executeQuery(query);
        final ResultSet targetRs = target.createStatement().executeQuery(query);
        while (sourceRs.next() && targetRs.next()) {
            final int sourceColCount = sourceRs.getMetaData().getColumnCount();
            final int targetColCount = targetRs.getMetaData().getColumnCount();

            if (sourceColCount != targetColCount) {
                fail("Column count differs in length");
            }
            for (int i = 1; i <= sourceColCount; i++) {
                assertEquals(sourceRs.getObject(i), targetRs.getObject(i));
            }
        }
        if (sourceRs.next() ^ targetRs.next()) {
            fail("Result sets differ in length");
        }
    }

    public static String implode(final String separator, final Iterable<String> data) {
        final StringBuilder sb = new StringBuilder();
        for (String iterable : data) {
            sb.append(iterable).append(separator);
        }
        return sb.substring(0, sb.length() - separator.length());
    }
}
